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

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Predicate;
import com.google.common.base.Stopwatch;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at table.
 */
public class TraceSessionContext
{
    /* column names */
    public static final String SESSION_ID = "sessionId";
    public static final String COORDINATOR = "coordinator";
    public static final String SESSION_REQUEST = "request";
    public static final String SESSION_START = "startedAt";
    public static final String EVENT_ID = "eventId";
    public static final String SOURCE = "source";
    public static final String EVENT = "event";
    public static final String HAPPENED = "happened_at";
    public static final String DURATION = "duration";

    /* keyspace and column families */
    public static final String TRACE_KEYSPACE = "trace";
    public static final String SESSIONS_TABLE = "trace_sessions";
    public static final String EVENTS_TABLE = "trace_events";

    private CFMetaData sessionsCfm;

    private CFMetaData eventsCfm;

    /**
     * Trace session meta events.
     */
    public enum TraceEvent
    {
        /**
         * Signals a remotely initiated trace session's beginning.
         */
        REMOTE_TRACE_SESSION_BEGIN,
        /**
         * Signals a remotely initiated trace session's end.
         */
        REMOTE_TRACE_SESSION_END,
        /**
         * Signals a locally initiated trace session's end.
         */
        TRACE_SESSION_END;
    }

    public static final String SESSION_CONTEXT_HEADER = "SessionContext";
    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);
    private static final CompositeType SESSION_CF_KEY_TYPE = CompositeType.getInstance(ImmutableList
            .<AbstractType<?>> of(Int32Type.instance, InetAddressType.instance
            ));

    private static final CompositeType EVENTS_CF_KEY_TYPE = CompositeType.getInstance(ImmutableList
            .<AbstractType<?>> of(Int32Type.instance, InetAddressType.instance,
                    Int32Type.instance));

    private static TraceSessionContext ctx;
    private static boolean initializing = false;

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private final InetAddress localAddress;
    private ThreadLocal<TraceSessionContextThreadLocalState> sessionContextThreadLocalState = new ThreadLocal<TraceSessionContextThreadLocalState>();

    private TraceSessionContext()
    {

        if (!Iterables.tryFind(Table.all(), new Predicate<Table>()
        {
            public boolean apply(Table table)
            {
                return table.name.equals(TRACE_KEYSPACE);
            }

        }).isPresent())
        {
            try
            {
                KSMetaData traceKs = KSMetaData.newKeyspace(TRACE_KEYSPACE, SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "1"));
                MigrationManager.announceNewKeyspace(traceKs);
            }
            catch (ConfigurationException e)
            {
                Throwables.propagate(e);
            }
        }

        sessionsCfm = compile("CREATE TABLE " + TRACE_KEYSPACE + "." + SESSIONS_TABLE + " (" +
                "  " + SESSION_ID + "      int," +
                "  " + COORDINATOR + "     inet," +
                "  " + SESSION_START + "   bigint," +
                "  " + SESSION_REQUEST + " text," +
                "  PRIMARY KEY (" + SESSION_ID + ", " + COORDINATOR + "));");

        System.out.println("B4: " + sessionsCfm);

        eventsCfm = compile("CREATE TABLE " + TRACE_KEYSPACE + "." + EVENTS_TABLE + " (" +
                "  " + SESSION_ID + "      int," +
                "  " + COORDINATOR + "     inet," +
                "  " + EVENT_ID + "        int," +
                "  " + SOURCE + "          inet," +
                "  " + EVENT + "           text," +
                "  " + DURATION + "        bigint," +
                "  " + HAPPENED + "        bigint," +
                "  PRIMARY KEY (" + SESSION_ID + ", " + COORDINATOR + ", " + EVENT_ID + "));");

        try
        {
            MigrationManager.announceNewColumnFamily(sessionsCfm);
            MigrationManager.announceNewColumnFamily(eventsCfm);

            System.out.println("AF: " + Table.open(TRACE_KEYSPACE).getColumnFamilyStore(SESSIONS_TABLE).metadata);
        }
        catch (ConfigurationException e)
        {
            Throwables.propagate(e);
        }

        this.localAddress = FBUtilities.getLocalAddress();
    }

    public int startSession(String request)
    {
        assert sessionContextThreadLocalState.get() == null;

        int sessionId = idGenerator.incrementAndGet();

        TraceSessionContextThreadLocalState tsctls = new TraceSessionContextThreadLocalState(localAddress,
                localAddress, sessionId);

        sessionContextThreadLocalState.set(tsctls);

        newSessionEvent(sessionId, tsctls.origin, request);
        return sessionId;
    }

    public void stopSession()
    {
        trace(TraceEvent.TRACE_SESSION_END);
        reset();
    }

    /**
     * Indicates if the current thread's execution is being traced.
     * 
     * @return
     */
    public boolean isTracing()
    {
        return sessionContextThreadLocalState.get() == null ? false : true;
    }

    /**
     * Indicates if the query originated on this node.
     * 
     * @return
     */
    public boolean isLocalTraceSession()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return ((tls != null) && tls.origin.equals(localAddress)) ? true : false;
    }

    public Integer getSessionId()
    {
        return isTracing() ? sessionContextThreadLocalState.get().sessionId : null;
    }

    public InetAddress getOrigin()
    {
        return isTracing() ? sessionContextThreadLocalState.get().origin : null;
    }

    public String logMessagePrefix()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        if (tls == null)
            return null;

        if (tls.messageId == null)
        {
            return String.format("query %d@%s - ", tls.sessionId, tls.origin);
        }
        return String.format("query %d@%s message %s - ", tls.sessionId, tls.origin, tls.messageId);
    }

    public TraceSessionContextThreadLocalState threadLocalState()
    {
        return sessionContextThreadLocalState.get();
    }

    /**
     * Copies the thread local state, if any. Used when the QueryContext needs to be copied into another thread. Use the
     * update() function to update the thread local state.
     */
    public TraceSessionContextThreadLocalState copy()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return tls == null ? null : new TraceSessionContextThreadLocalState(tls);
    }

    /**
     * Updates the Query Context for this thread. Call copy() to obtain a copy of a threads query context.
     */
    public void update(final TraceSessionContextThreadLocalState tls)
    {
        sessionContextThreadLocalState.set(tls);
    }

    public void reset()
    {
        sessionContextThreadLocalState.set(null);
    }

    /**
     * Updates the threads query context from a message
     * 
     * @param message
     *            The internode message
     * @param bytes
     *            Bytes used in the header, returned from call getMessageBytes()
     */
    public void update(final MessageIn<?> message, final byte[] bytes, String id)
    {
        checkState((bytes != null) && bytes.length > 0);
        final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(bytes));
        final Integer sessionId;
        try
        {
            sessionId = dis.readInt();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        sessionContextThreadLocalState.set(new TraceSessionContextThreadLocalState(message.from, localAddress,
                sessionId, id));
    }

    /**
     * Creates a byte[] to use a message header to serialise this context to another node, if any. The context is only
     * included in the message if it started locally.
     * 
     * @return
     */
    public byte[] getMessageBytes()
    {
        if (!isLocalTraceSession())
            return null;

        // this uses a FBA so no need to close()
        @SuppressWarnings("resource")
        final DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
            buffer.writeInt(tls.sessionId);
            return buffer.getData();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    /**
     * Stores a "new session" event in the sessions table. This will allow to track all the subsequent "trace" events.
     * 
     * @param sessionId
     *            the sessionId - unique in a per-host basis
     * @param coordinator
     *            the node that initiated the sesssion
     * @param request
     *            the request that initiated the session (usually the user operation)
     */
    private void newSessionEvent(int sessionId, InetAddress coordinator, String request)
    {
        RowMutation mutation = new RowMutation(TRACE_KEYSPACE, SESSION_CF_KEY_TYPE.decompose(sessionId,
                coordinator));

        // TODO add TTL
        ColumnFamily family = ColumnFamily.create(sessionsCfm);
        family.addColumn(column(SESSION_START, System.currentTimeMillis()));
        family.addColumn(column(SESSION_REQUEST, request));
        mutation.add(family);
        try
        {
            StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
        }
        // log but tracing errors shouldn't affect the caller
        catch (TimeoutException e)
        {
            logger.error("error while storing trace event", e);
            Throwables.propagate(e);
        }
        catch (UnavailableException e)
        {
            logger.error("error while storing trace event", e);
            Throwables.propagate(e);
        }
    }

    /**
     * Includes the provided event in trace, duration is computed with the session's thread local {@link Stopwatch},
     * counting from the beginning of the *LOCAL* session, i.e., in order to compute global durations when sessions span
     * multiple nodes values must be added up. Current time is measured with System.currentTimeMillis().
     */
    public void trace(TraceEvent traceEvent)
    {
        trace(traceEvent.name());
    }

    public void trace(String traceEvent)
    {
        TraceSessionContextThreadLocalState state = sessionContextThreadLocalState.get();
        trace(state.sessionId, state.origin, state.source, state.eventIds.getAndIncrement(), traceEvent,
                state.watch.elapsedTime(TimeUnit.NANOSECONDS), System.currentTimeMillis());
    }

    public void trace(int sessionId, InetAddress coordinator, InetAddress source,
            int eventId, String traceEvent, long duration, long happenedAt)
    {
        RowMutation mutation = new RowMutation(TRACE_KEYSPACE, EVENTS_CF_KEY_TYPE.decompose(sessionId, coordinator,
                eventId));
        // TODO add TTL
        ColumnFamily family = ColumnFamily.create(eventsCfm);
        family.addColumn(column(SOURCE, source));
        // family.addColumn(column(EVENT, traceEvent));
        // family.addColumn(column(DURATION, duration));
        // family.addColumn(column(HAPPENED, happenedAt));
        mutation.add(family);
        try
        {
            mutation.apply();
        }
        // log but tracing errors shouldn't affect the caller
        catch (IOException e)
        {
            logger.error("error while storing trace event", e);
        }
    }

    private static CFMetaData compile(String cql)
    {
        CreateColumnFamilyStatement statement = null;
        try
        {
            statement = (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql)
                    .prepare().statement;

            ColumnFamilyType type = ColumnFamilyType.Standard;
            CFMetaData newCFMD = new CFMetaData(TRACE_KEYSPACE, statement.columnFamily(), type, statement.comparator,
                    null);

            newCFMD.comment("")
                    .readRepairChance(0)
                    .dcLocalReadRepairChance(0)
                    .gcGraceSeconds(0);

            statement.applyPropertiesTo(newCFMD);
            return newCFMD;
        }
        catch (InvalidRequestException e)
        {
            throw Throwables.propagate(e);
        }
        catch (ConfigurationException e)
        {
            throw Throwables.propagate(e);
        }
    }

    private static Column column(String columnName, long value)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBufferUtil.bytes(value));
    }

    private static Column column(String columnName, String value)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBufferUtil.bytes(value));
    }

    private static Column column(String columnName, InetAddress address)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBuffer.wrap(address.getAddress()));
    }

    /**
     * Fetches and lazy initializes the trace context.
     */
    public static TraceSessionContext traceCtx()
    {
        if (ctx == null && !initializing)
        {
            initializing = true;
            ctx = new TraceSessionContext();
        }
        return ctx;
    }

    public static class TraceSessionContextThreadLocalState

    {
        public final Integer sessionId;
        public final InetAddress origin;
        public final InetAddress source;
        public final String messageId;
        public final Stopwatch watch;
        public final AtomicInteger eventIds = new AtomicInteger();

        public TraceSessionContextThreadLocalState(final TraceSessionContextThreadLocalState other)
        {
            this(other.origin, other.source, other.sessionId, other.messageId);
        }

        public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
                final Integer sessionId)
        {
            this(coordinator, source, sessionId, null);
        }

        public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
                final Integer sessionId,
                final String messageId)
        {
            checkNotNull(coordinator);
            checkNotNull(source);
            checkNotNull(sessionId);

            this.origin = coordinator;
            this.source = source;
            this.sessionId = sessionId;
            this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
            this.watch = new Stopwatch();
            this.watch.start();
        }
    }
}