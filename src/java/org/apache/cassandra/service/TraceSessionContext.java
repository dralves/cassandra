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
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Stopwatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A container for per-query thread-local state. Tracks if the current query should be logged in detail.
 */
public class TraceSessionContext
{
    /* column names */
    public static final String SESSION = "session";
    public static final String COORDINATOR = "coordinator";
    public static final String REQUEST = "request";
    public static final String EVENT_ID = "id";
    public static final String SOURCE = "source";
    public static final String EVENT = "event";
    public static final String HAPPENED = "event";
    public static final String DURATION = "event";

    /* keyspace and column families */
    public static final String TRACE_TABLE = "trace";
    public static final String TRACE_SESSIONS_CF_NAME = "trace_sessions";
    public static final String TRACE_EVENTS_CF_NAME = "trace_events";

    /* cql definitions */
    public static final String TRACE_SESSIONS_CFDEF = "CREATE TABLE trace.trace_sessions (" +
            "  session int," +
            "  coordinator inetaddr," +
            "  request text," +
            "  PRIMARY KEY (session, coordinator));";

    public static final String TRACE_EVENTS_CFDEF = "CREATE TABLE trace.trace_events (" +
            "  session     int," +
            "  coordinator inetaddr," +
            "  id     uuid," +
            "  source inetaddr," +
            "  event  text," +
            "  duration long," +
            "  happened_at long," +
            "  PRIMARY KEY (session, coordinator, id));";

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

        private UUID uuid;

        TraceEvent()
        {
            uuid = UUID.nameUUIDFromBytes(ByteBufferUtil.bytes(name()).array());
        }

        public UUID uuid()
        {
            return uuid;
        }

    }

    public static final String SESSION_CONTEXT_HEADER = "SessionContext";

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);
    private static final TraceSessionContext instance = new TraceSessionContext();

    private final AtomicInteger idGenerator = new AtomicInteger(0);
    private ThreadLocal<TraceSessionContextThreadLocalState> sessionContextThreadLocalState = new ThreadLocal<TraceSessionContextThreadLocalState>();
    private Table traceTable;
    private CFMetaData sessionsCfm;
    private CFMetaData eventsCfm;

    private TraceSessionContext()
    {
        this.traceTable = Table.open(TRACE_TABLE);
        if (traceTable.getColumnFamilyStores().size() != 2)
        {
            processInternal(TRACE_EVENTS_CFDEF);
            processInternal(TRACE_SESSIONS_CFDEF);
        }
        this.sessionsCfm = Schema.instance.getCFMetaData(TRACE_TABLE, TRACE_SESSIONS_CF_NAME);
        this.eventsCfm = Schema.instance.getCFMetaData(TRACE_TABLE, TRACE_EVENTS_CF_NAME);
    }

    /**
     * Called from CassandraServer when a query starts, thread local state will be initialised if the ClientState says
     * query details should be logged.
     * 
     * @param cs
     * @return if the connection wide queryDetails was set, returns true and begins tracking the query. false otherwise.
     */
    public boolean startSession(final ClientState cs, String request)
    {
        assert sessionContextThreadLocalState.get() == null;
        if (!cs.getQueryDetails())
            return false;

        int sessionId = idGenerator.incrementAndGet();

        TraceSessionContextThreadLocalState tsctls = new TraceSessionContextThreadLocalState(
                FBUtilities.getLocalAddress(), sessionId);

        sessionContextThreadLocalState.set(tsctls);

        newSessionEvent(sessionId, tsctls.origin, request);
        return true;
    }

    /**
     * Clears the thread local state for the current query.
     */
    public void stopSession()
    {
        if (isTracing())
            logger.info("returning to client, async processing may continue");

        TraceSessionContextThreadLocalState tsctls = sessionContextThreadLocalState.get();

        newTraceEvent(tsctls.sessionId, tsctls.origin, tsctls.origin, TraceEvent.TRACE_SESSION_END,
                tsctls.watch.elapsedTime(TimeUnit.MILLISECONDS), System.currentTimeMillis());

        reset();
    }

    /**
     * Indicates if this query should have it's details logged.
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
        return ((tls != null) && tls.origin.equals(FBUtilities.getLocalAddress())) ? true : false;
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
        sessionContextThreadLocalState.set(new TraceSessionContextThreadLocalState(message.from, sessionId, id));
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

    public void newSessionEvent(int sessionId, InetAddress coordinator, String request)
    {
        long currentTime = System.currentTimeMillis();
        RowMutation mutation = new RowMutation(TRACE_TABLE, ByteBufferUtil.bytes(sessionId));
        // TODO add TTL
        ColumnFamily family = ColumnFamily.create(sessionsCfm);
        family.addColumn(column(SESSION, sessionId, currentTime));
        family.addColumn(column(COORDINATOR, coordinator, currentTime));
        family.addColumn(column(REQUEST, request, currentTime));
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

    public void newTraceEvent(int sessionId, InetAddress coordinator, InetAddress source,
            TraceEvent traceEvent, long duration, long happenedAt)
    {
        newTraceEvent(sessionId, coordinator, source, traceEvent.name(), traceEvent.uuid(), duration, happenedAt);
    }

    public void newTraceEvent(int sessionId, InetAddress coordinator, InetAddress source,
            String traceEvent, UUID id, long duration, long happenedAt)
    {
        long currentTime = System.currentTimeMillis();
        RowMutation mutation = new RowMutation(TRACE_TABLE, ByteBufferUtil.bytes(sessionId));
        // TODO add TTL
        ColumnFamily family = ColumnFamily.create(eventsCfm);
        family.addColumn(column(SESSION, sessionId, currentTime));
        family.addColumn(column(COORDINATOR, coordinator, currentTime));
        family.addColumn(column(EVENT_ID, id, currentTime));
        family.addColumn(column(SOURCE, source, currentTime));
        family.addColumn(column(EVENT, traceEvent, currentTime));
        family.addColumn(column(DURATION, duration, currentTime));
        family.addColumn(column(HAPPENED, happenedAt, currentTime));
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

    private static Column column(String columnName, int value, long timestamp)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBufferUtil.bytes(value), timestamp);
    }

    private static Column column(String columnName, long value, long timestamp)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBufferUtil.bytes(value), timestamp);
    }

    private static Column column(String columnName, InetAddress address, long timestamp)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBuffer.wrap(address.getAddress()), timestamp);
    }

    private static Column column(String columnName, UUID uuid, long timestamp)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBuffer.wrap(UUIDGen.decompose(uuid)), timestamp);
    }

    private static Column column(String columnName, String value, long timestamp)
    {
        return new Column(ByteBufferUtil.bytes(columnName), ByteBufferUtil.bytes(value), timestamp);
    }

    public static TraceSessionContext traceCtx()
    {
        return instance;
    }

    public static class TraceSessionContextThreadLocalState

    {
        public final Integer sessionId;
        public final InetAddress origin;
        public final String messageId;
        public final Stopwatch watch;

        public TraceSessionContextThreadLocalState(final TraceSessionContextThreadLocalState other)
        {
            this(other.origin, other.sessionId, other.messageId);
        }

        public TraceSessionContextThreadLocalState(final InetAddress origin, final Integer sessionId)
        {
            this(origin, sessionId, null);
        }

        public TraceSessionContextThreadLocalState(final InetAddress origin, final Integer sessionId,
                final String messageId)
        {
            checkNotNull(origin);
            checkNotNull(sessionId);

            this.origin = origin;
            this.sessionId = sessionId;
            this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
            this.watch = new Stopwatch();
            this.watch.start();
        }
    }
}