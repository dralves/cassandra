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
import java.util.concurrent.atomic.AtomicBoolean;

import com.google.common.base.Throwables;
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
        protected void mutate(RowMutation mutation)
        {
            try
            {
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
        sessionId = traceCtx().startSession("test_session", 123L);
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
    public void testContextTLStateAcompaniesToAnotherThread() throws InterruptedException, ExecutionException
    {
        // the state should be carried to another thread as long as the debuggable TPE is used
        DebuggableThreadPoolExecutor poolExecutor = DebuggableThreadPoolExecutor
                .createWithFixedPoolSize("test_pool", 1);

        final AtomicBoolean executed = new AtomicBoolean(false);
        poolExecutor.submit(new Runnable()
        {
            @Override
            public void run()
            {
                assertTrue(traceCtx().isTracing());
                assertEquals(sessionId, traceCtx().getSessionId());
                executed.set(true);
            }
        }).get();
        assertTrue(executed.get());
    }

    @Test
    public void testTracingSessionsContinueRemotely()
    {

    }
}
