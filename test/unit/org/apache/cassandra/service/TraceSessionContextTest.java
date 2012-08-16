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
import static org.apache.cassandra.tracing.TraceSessionContext.EVENTS_TABLE;
import static org.apache.cassandra.tracing.TraceSessionContext.SESSIONS_TABLE;
import static org.apache.cassandra.tracing.TraceSessionContext.SESSION_TYPE;
import static org.apache.cassandra.tracing.TraceSessionContext.TRACE_KEYSPACE;
import static org.apache.cassandra.tracing.TraceSessionContext.isTracing;
import static org.apache.cassandra.tracing.TraceSessionContext.traceCtx;

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
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.net.IVerbHandler;
import org.apache.cassandra.net.MessageDeliveryTask;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.net.MessagingService;
import org.apache.cassandra.net.MessagingService.Verb;
import org.apache.cassandra.tracing.TraceEvent;
import org.apache.cassandra.tracing.TraceEvent.Type;
import org.apache.cassandra.tracing.TraceEventBuilder;
import org.apache.cassandra.tracing.TracePrettyPrinter;
import org.apache.cassandra.tracing.TraceSessionContext;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

public class TraceSessionContextTest extends SchemaLoader
{

    private static class LocalTraceSessionContext extends TraceSessionContext
    {

        /**
         * Override the parent mutation that applies mutation to the cluster to instead apply mutations locally and
         * immediately for testing.
         */
        @Override
        protected void store(final UUID key, final ColumnFamily family)
        {
            RowMutation mutation = new RowMutation(TRACE_KEYSPACE, TimeUUIDType.instance.decompose(key));
            mutation.add(family);
            mutation.apply();
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
        assertTrue(isTracing());
        assertTrue(traceCtx().isLocalTraceSession());
        assertNotNull(traceCtx().threadLocalState());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(SESSIONS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(TimeUUIDType.instance.decompose(sessionId)),
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
        UUID eventId = traceCtx().trace(
                new TraceEventBuilder().name("simple trace event").duration(4321L).timestamp(1234L)
                        .addPayload("simplePayload", LongType.instance, 9876L).build());

        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);
        // we should have two events because "get" actually produces one
        assertSame(2, traceEvents.size());

        TraceEvent event = Iterables.get(traceEvents, 1);
        assertEquals("simple trace event", event.name());
        assertEquals(4321L, event.duration());
        assertEquals(1234L, event.timestamp());
        assertEquals(9876L, event.getFromPayload("simplePayload"));
        assertEquals(eventId, event.id());

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
                assertTrue(isTracing());
                assertEquals(sessionId, traceCtx().getSessionId());
                reference.set(traceCtx().trace(
                        new TraceEventBuilder().name("multi threaded trace event").duration(8765L).timestamp(5678L)
                                .build()));
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

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);

        // we should have 6 events
        // "getColumnFamily" from testNewSession
        // custom trace from testNewLocalTraceEvent
        // "getColumnFamily" from testNewLocalTraceEvent
        // stage start trace event from testContextTLStateAcompaniesToAnotherThread
        // custom event from testContextTLStateAcompaniesToAnotherThread
        // stage finish event from testContextTLStateAcompaniesToAnotherThread

        assertSame(6, traceEvents.size());

        TraceEvent stageStartEvent = Iterables.get(traceEvents, 3);
        TraceEvent customEvent = Iterables.get(traceEvents, 4);
        TraceEvent stageFinishEvent = Iterables.get(traceEvents, 5);

        assertEquals("TestStage", stageStartEvent.name());
        assertSame(Type.STAGE_START, stageStartEvent.type());
        assertEquals(8765L, customEvent.duration());
        assertEquals("multi threaded trace event", customEvent.name());
        assertEquals(5678L, customEvent.timestamp());
        assertEquals(FBUtilities.getLocalAddress(), customEvent.source());
        assertEquals("TestStage", stageFinishEvent.name());
        assertSame(Type.STAGE_FINISH, stageFinishEvent.type());
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
                assertTrue(isTracing());
                assertFalse(traceCtx().isLocalTraceSession());
                assertEquals(sessionId, traceCtx().getSessionId());
                reference.set(traceCtx().trace(
                        new TraceEventBuilder().name("remote trace event").duration(9123L).timestamp(3219L)
                                .build()));
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

        // Because the MessageDeliveryTask does not run on a DebuggableTPE no automated stage tracing
        // events were inserted, we just have 4 more than after the previous method
        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);
        assertSame(8, traceEvents.size());

        TraceEvent remoteEvent = Iterables.get(traceEvents, 7);
        assertEquals(9123L, remoteEvent.duration());
        assertEquals("remote trace event", remoteEvent.name());
        assertEquals(3219L, remoteEvent.timestamp());
        assertEquals(InetAddress.getByName("0.0.0.0"), remoteEvent.source());
    }

    @Test
    public void testPrettyPrinter()
    {
        ColumnFamily family = Table
                .open(TRACE_KEYSPACE)
                .getColumnFamilyStore(EVENTS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(UUIDGen.decompose(sessionId))),
                                new QueryPath(
                                        EVENTS_TABLE)));

        List<TraceEvent> traceEvents = TraceEventBuilder.fromColumnFamily(sessionId, family);

        TracePrettyPrinter.printSingleSessionTrace(sessionId, traceEvents, System.out);

    }
}
