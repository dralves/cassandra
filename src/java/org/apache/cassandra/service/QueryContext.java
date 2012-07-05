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

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.util.concurrent.atomic.AtomicInteger;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.utils.FBUtilities;

/**
 * A container for per-query thread-local state. Tracks if the current query should be logged in detail.
 */
public class QueryContext
{
    public static final String QUERY_CONTEXT_HEADER = "QueryContext";

    private static final Logger logger = LoggerFactory.getLogger(QueryContext.class);

    private static final AtomicInteger idGenerator = new AtomicInteger(0);

    private static ThreadLocal<QueryContextTLS> queryContextTLS = new ThreadLocal<QueryContextTLS>();

    private QueryContext()
    {
    }

    /**
     * Called from CassandraServer when a query starts, thread local state will be initialised if the ClientState says
     * query details should be logged.
     * 
     * @param cs
     * @return if the connection wide queryDetails was set, returns true and begins tracking the query. false otherwise.
     */
    public static boolean startQuery(final ClientState cs)
    {
        assert queryContextTLS.get() == null;
        if (cs.getQueryDetails() == false)
            return false;

        queryContextTLS.set(new QueryContextTLS(FBUtilities.getLocalAddress(), idGenerator.incrementAndGet()));
        return true;
    }

    /**
     * Clears the thread local state for the current query.
     */
    public static void stopQuery()
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
        return queryContextTLS.get() == null ? false : true;
    }

    /**
     * Indicates if the query originated on this node.
     * 
     * @return
     */
    public static boolean isLocalQuery()
    {
        final QueryContextTLS tls = queryContextTLS.get();
        return ((tls != null) && tls.origin.equals(FBUtilities.getLocalAddress())) ? true : false;
    }

    public static Integer getQueryId()
    {
        return isQueryDetail() ? queryContextTLS.get().queryId : null;
    }

    public static InetAddress getOrigin()
    {
        return isQueryDetail() ? queryContextTLS.get().origin : null;
    }

    public static String logMessagePrefix()
    {
        final QueryContextTLS tls = queryContextTLS.get();
        if (tls == null)
            return null;

        if (tls.messageId == null)
        {
            return String.format("query %d@%s - ", tls.queryId, tls.origin);
        }
        return String.format("query %d@%s message %s - ", tls.queryId, tls.origin, tls.messageId);
    }

    /**
     * Copies the thread local state, if any. Used when the QueryContext needs to be copied into another thread. Use the
     * update() function to update the thread local state.
     */
    public static QueryContextTLS copy()
    {
        final QueryContextTLS tls = queryContextTLS.get();
        return tls == null ? null : new QueryContextTLS(tls);
    }

    /**
     * Updates the Query Context for this thread. Call copy() to obtain a copy of a threads query context.
     */
    public static void update(final QueryContextTLS tls)
    {
        queryContextTLS.set(tls);
    }

    public static void reset()
    {
        queryContextTLS.set(null);
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
        final Integer queryId;
        try
        {
            queryId = dis.readInt();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
        queryContextTLS.set(new QueryContextTLS(message.from, queryId, id));
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

        final DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            final QueryContextTLS tls = queryContextTLS.get();
            buffer.writeInt(tls.queryId);
            return buffer.getData();
        }
        catch (IOException e)
        {
            throw new IOError(e);
        }
    }

    public static class QueryContextTLS
    {
        public final Integer queryId;
        public final InetAddress origin;
        public final String messageId;

        public QueryContextTLS(final QueryContextTLS other)
        {
            this(other.origin, other.queryId, other.messageId);
        }

        public QueryContextTLS(final InetAddress origin, final Integer queryId)
        {
            this(origin, queryId, null);
        }

        public QueryContextTLS(final InetAddress origin, final Integer queryId, final String messageId)
        {
            assert origin != null;
            assert queryId != null;

            this.origin = origin;
            this.queryId = queryId;
            this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
        }
    }
}