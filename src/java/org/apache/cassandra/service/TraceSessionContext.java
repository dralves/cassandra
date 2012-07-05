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
import static org.apache.cassandra.cql3.QueryProcessor.processInternal;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.db.Table;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A container for per-query thread-local state. Tracks if the current query should be logged in detail.
 */
public class TraceSessionContext
{
    public static final String SESSION_CONTEXT_HEADER = "SessionContext";

    public static final String TRACE_TABLE = "trace";

    public static final String TRACE_SESSIONS_CF_NAME = "trace_sessions";

    public static final String TRACE_SESSIONS_CFDEF = "CREATE TABLE trace.trace_sessions (" +
            "  session int," +
            "  coordinator inetaddr," +
            "  request text," +
            "  PRIMARY KEY (session, coordinator));";

    public static final String TRACE_EVENTS_CF_NAME = "trace_events";

    public static final String TRACE_EVENTS_CFDEF = "CREATE TABLE trace.trace_events (" +
            "  session     int," +
            "  coordinator inetaddr," +
            "  id     uuid," +
            "  source inetaddr," +
            "  event  text," +
            "  happened_at timestamp," +
            "  duration int," +
            "  PRIMARY KEY (session, coordinator, id));";

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    private static ThreadLocal<TraceSessionContextThreadLocalState> sessionContextThreadLocalState = new ThreadLocal<TraceSessionContextThreadLocalState>();

    private TraceSessionContext()
    {
        Table traceTable = Table.open(TRACE_TABLE);
        if (traceTable.getColumnFamilyStores().size() != 2)
        {
            processInternal(TRACE_EVENTS_CFDEF);
            processInternal(TRACE_SESSIONS_CFDEF);
        }
    }

    /**
     * Called from CassandraServer when a query starts, thread local state will be initialised if the ClientState says
     * query details should be logged.
     * 
     * @param cs
     * @return if the connection wide queryDetails was set, returns true and begins tracking the query. false otherwise.
     */
    public static boolean startSession(final ClientState cs)
    {
        assert sessionContextThreadLocalState.get() == null;
        if (cs.getQueryDetails() == false)
            return false;

        sessionContextThreadLocalState.set(new TraceSessionContextThreadLocalState(FBUtilities.getLocalAddress(),
                idGenerator
                        .incrementAndGet()));
        return true;
    }

    /**
     * Clears the thread local state for the current query.
     */
    public static void stopSession()
    {
        if (isQueryDetail())
            logger.info("returning to client, async processing may continue");
        reset();
    }

    /**
     * Indicates if this query should have it's details logged.
     * 
     * @return
     */
    public static boolean isQueryDetail()
    {
        return sessionContextThreadLocalState.get() == null ? false : true;
    }

    /**
     * Indicates if the query originated on this node.
     * 
     * @return
     */
    public static boolean isLocalQuery()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return ((tls != null) && tls.origin.equals(FBUtilities.getLocalAddress())) ? true : false;
    }

    public static Integer getQueryId()
    {
        return isQueryDetail() ? sessionContextThreadLocalState.get().sessionId : null;
    }

    public static InetAddress getOrigin()
    {
        return isQueryDetail() ? sessionContextThreadLocalState.get().origin : null;
    }

    public static String logMessagePrefix()
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
    public static TraceSessionContextThreadLocalState copy()
    {
        final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
        return tls == null ? null : new TraceSessionContextThreadLocalState(tls);
    }

    /**
     * Updates the Query Context for this thread. Call copy() to obtain a copy of a threads query context.
     */
    public static void update(final TraceSessionContextThreadLocalState tls)
    {
        sessionContextThreadLocalState.set(tls);
    }

    public static void reset()
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
    public static void update(final MessageIn<?> message, final byte[] bytes, String id)
    {
        assert ((bytes != null) && bytes.length > 0);

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
    public static byte[] getMessageBytes()
    {
        if (!isLocalQuery())
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

    public static class TraceSessionContextThreadLocalState

    {
        public final Integer sessionId;
        public final InetAddress origin;
        public final String messageId;
        

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
        }
    }
}