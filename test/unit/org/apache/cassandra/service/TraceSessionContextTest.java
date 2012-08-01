/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.service;

import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertFalse;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertSame;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.service.TraceSessionContext.EVENTS_TABLE;
import static org.apache.cassandra.service.TraceSessionContext.EVENT_TYPE;
import static org.apache.cassandra.service.TraceSessionContext.SESSIONS_TABLE;
import static org.apache.cassandra.service.TraceSessionContext.SESSION_TYPE;
import static org.apache.cassandra.service.TraceSessionContext.TRACE_KEYSPACE;
import static org.apache.cassandra.service.TraceSessionContext.traceCtx;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.concurrent.DebuggableThreadPoolExecutor;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class TraceSessionContextTest extends SchemaLoader
{

    private static class LocalTraceSessionContext extends TraceSessionContext
    {

        /**
         * Override the paren't mutation that applies mutation to the cluster to instead apply mutations locally for
         * testing.
         */
        @Override
        protected void store(final byte[] key, final ColumnFamily family)
        {
            try
            {
                RowMutation mutation = new RowMutation(TRACE_KEYSPACE, ByteBuffer.wrap(key));
                mutation.add(family);
                mutation.apply();
            }
            catch (IOException e)
            {
                Throwables.propagate(e);
            }
        }
    }

    private static UUID sessionId;

    @BeforeClass
    public static void loadSchema() throws IOException
    {
        SchemaLoader.loadSchema();
        TraceSessionContext.setCtx(new LocalTraceSessionContext());
    }

    @Test
    public void testNewSession() throws CharacterCodingException
    {
        sessionId = traceCtx().prepareSession();
        traceCtx().startSession(sessionId, "test_session", 123L);
        assertTrue(traceCtx().isTracing());
        assertTrue(traceCtx().isLocalTraceSession());
        assertNotNull(traceCtx().threadLocalState());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(SESSIONS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        SESSIONS_TABLE)));

        // should have two columns
        assertSame(2, family.getColumnCount());

        // request col
        IColumn requestColumn = Iterables.get(family, 0);
        List<CompositeComponent> components = SESSION_TYPE.deconstruct(requestColumn.name());
        InetAddress address = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        String colName = (String) components.get(1).comparator.compose(components.get(1).value);
        assertEquals("request", colName);
        System.out.println(requestColumn);
        assertEquals(FBUtilities.getLocalAddress(), address);
        String request = ByteBufferUtil.string(requestColumn.value());
        assertEquals("test_session", request);

        // startedAt col
        IColumn startColumn = Iterables.get(family, 1);
        components = SESSION_TYPE.deconstruct(startColumn.name());
        address = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        colName = (String) components.get(1).comparator.compose(components.get(1).value);
        assertEquals("startedAt", colName);
        assertEquals(FBUtilities.getLocalAddress(), address);
        // try to parse the long but as the time comes from the system clock when can't actually test it
        assertSame(123L, ByteBufferUtil.toLong(startColumn.value()));
    }

    @Test
    public void testNewLocalTraceEvent() throws CharacterCodingException, UnknownHostException
    {
        UUID eventId = traceCtx().trace("simple trace event", 4321L, 1234L);

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        assertSame(4, family.getColumnCount());

        IColumn durationColumn = Iterables.get(family, 0);
        List<CompositeComponent> components = EVENT_TYPE.deconstruct(durationColumn.name());
        InetAddress coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        UUID decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        String colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("duration", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(4321L, ByteBufferUtil.toLong(durationColumn.value()));

        IColumn eventColumn = Iterables.get(family, 1);
        components = EVENT_TYPE.deconstruct(eventColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("event", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals("simple trace event", ByteBufferUtil.string(eventColumn.value()));

        IColumn happenedAtColumn = Iterables.get(family, 2);
        components = EVENT_TYPE.deconstruct(happenedAtColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("happened_at", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(1234L, ByteBufferUtil.toLong(happenedAtColumn.value()));

        IColumn sourceColumn = Iterables.get(family, 3);
        components = EVENT_TYPE.deconstruct(sourceColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("source", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(FBUtilities.getLocalAddress(),
                InetAddress.getByAddress(ByteBufferUtil.getArray(sourceColumn.value())));
    }

    @Test
    public void testContextTLStateAcompaniesToAnotherThread() throws InterruptedException, ExecutionException,
            CharacterCodingException, UnknownHostException
    {
        // the state should be carried to another thread as long as the debuggable TPE is used
        DebuggableThreadPoolExecutor poolExecutor = DebuggableThreadPoolExecutor
                .createWithFixedPoolSize("TestStage", 1);

        final AtomicReference<UUID> reference = new AtomicReference<UUID>();
        poolExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                assertTrue(traceCtx().isTracing());
                assertEquals(sessionId, traceCtx().getSessionId());
                reference.set(traceCtx().trace("multi threaded trace event", 8765L, 5678L));
            }
        }).get();
        assertNotNull(reference.get());

        // The DebuggableTPE that will executed the task will insert a trace event AFTER
        // it returns so we need to wait a bit.
        Thread.sleep(500);

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        // should now have 16 columns
        // 4 - from the previous method
        // 4 - from the automated trace event inserted at stage start
        // 4 - from the custom trace event inserted by the runnable
        // 4 - from the automated trace event inserted at stage end
        assertSame(16, family.getColumnCount());

        // make sure that the automated stage tracing events are there (start)
        IColumn traceStart = Iterables.get(family, 5);
        List<CompositeComponent> components = EVENT_TYPE.deconstruct(traceStart.name());
        InetAddress coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        UUID decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        String colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("event", colName);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals("STAGE_BEGIN[TestStage]", ByteBufferUtil.string(traceStart.value()));

        // now make sure all the details of our trace event match
        UUID eventId = reference.get();

        IColumn durationColumn = Iterables.get(family, 8);
        components = EVENT_TYPE.deconstruct(durationColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("duration", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(8765L, ByteBufferUtil.toLong(durationColumn.value()));

        IColumn eventColumn = Iterables.get(family, 9);
        components = EVENT_TYPE.deconstruct(eventColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("event", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals("multi threaded trace event", ByteBufferUtil.string(eventColumn.value()));

        IColumn happenedAtColumn = Iterables.get(family, 10);
        components = EVENT_TYPE.deconstruct(happenedAtColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("happened_at", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(5678L, ByteBufferUtil.toLong(happenedAtColumn.value()));

        IColumn sourceColumn = Iterables.get(family, 11);
        components = EVENT_TYPE.deconstruct(sourceColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("source", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(FBUtilities.getLocalAddress(),
                InetAddress.getByAddress(ByteBufferUtil.getArray(sourceColumn.value())));

        // make sure that the automated stage tracing events are there (finish)
        IColumn traceFinish = Iterables.get(family, 13);
        components = EVENT_TYPE.deconstruct(traceFinish.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("event", colName);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals("STAGE_FINISH[TestStage]", ByteBufferUtil.string(traceFinish.value()));
    }

    @Test
    public void testTracingSessionsContinuesRemotelyIfMessageHasSessionContextHeader() throws UnknownHostException,
            InterruptedException, ExecutionException, CharacterCodingException
    {

        // use a different TPE so that context is not automatically carried over
        ThreadPoolExecutor poolExecutor = new ThreadPoolExecutor(1, 1, 1, TimeUnit.SECONDS,
                new LinkedBlockingQueue<Runnable>());

        MessageIn<Void> messageIn = MessageIn.create(FBUtilities.getLocalAddress(), null,
                ImmutableMap.<String, byte[]>
                        of(TraceSessionContext.TRACE_SESSION_CONTEXT_HEADER, traceCtx().getSessionContextHeader()),
                Verb.UNUSED_1, 1);

        final AtomicReference<UUID> reference = new AtomicReference<UUID>();

        // change the local address to emulate another node
        traceCtx().setLocalAddress(InetAddress.getByName("0.0.0.0"));

        MessagingService.instance().registerVerbHandlers(Verb.UNUSED_1, new IVerbHandler<Void>()
        {

            @Override
            public void doVerb(MessageIn<Void> message, String id)
            {
                assertTrue(traceCtx().isTracing());
                assertFalse(traceCtx().isLocalTraceSession());
                assertEquals(sessionId, traceCtx().getSessionId());
                reference.set(traceCtx().trace("remote trace event", 9123L, 3219L));
            }
        });
        poolExecutor.submit(new MessageDeliveryTask(messageIn, "id")).get();

        assertNotNull(reference.get());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        // Because the MessageDeliveryTask does not run on a DebuggableTPE no automated tracing
        // events were inserted, we just have 4 more than after the previous method
        assertSame(20, family.getColumnCount());

        UUID eventId = reference.get();

        IColumn durationColumn = Iterables.get(family, 16);
        List<CompositeComponent> components = EVENT_TYPE.deconstruct(durationColumn.name());
        InetAddress coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        UUID decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        String colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("duration", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(9123L, ByteBufferUtil.toLong(durationColumn.value()));

        IColumn eventColumn = Iterables.get(family, 17);
        components = EVENT_TYPE.deconstruct(eventColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("event", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals("remote trace event", ByteBufferUtil.string(eventColumn.value()));

        IColumn happenedAtColumn = Iterables.get(family, 18);
        components = EVENT_TYPE.deconstruct(happenedAtColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("happened_at", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(3219L, ByteBufferUtil.toLong(happenedAtColumn.value()));

        IColumn sourceColumn = Iterables.get(family, 19);
        components = EVENT_TYPE.deconstruct(sourceColumn.name());
        coordinator = (InetAddress) components.get(0).comparator.compose(components.get(0).value);
        decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
        colName = (String) components.get(2).comparator.compose(components.get(2).value);
        assertEquals("source", colName);
        assertEquals(eventId, decodedEventId);
        assertEquals(FBUtilities.getLocalAddress(), coordinator);
        assertEquals(InetAddress.getByName("0.0.0.0"),
                InetAddress.getByAddress(ByteBufferUtil.getArray(sourceColumn.value())));

    }
}
