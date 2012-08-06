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
package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkState;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOError;
import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.RejectedExecutionException;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.concurrent.Stage;
import org.apache.cassandra.concurrent.StageManager;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.ColumnNameBuilder;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.cql3.statements.CreateColumnFamilyStatement;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.ExpiringColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.MapType;
import org.apache.cassandra.db.marshal.TimeUUIDType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.io.util.DataOutputBuffer;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.net.MessageIn;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.UUIDGen;

/**
 * A trace session context. Able to track and store trace sessions. A session is usually a user initiated query, and may
 * have multiple local and remote events before it is completed. All events and sessions are stored at table.
 */
public class TraceSessionContext
{
    public static final String TRACE_SESSION_CONTEXT_HEADER = "SessionContext";

    /* keyspace and column families */
    public static final String TRACE_KEYSPACE = "trace";
    public static final String SESSIONS_TABLE = "trace_sessions";
    public static final String EVENTS_TABLE = "trace_events";

    /* key */
    private static final String SESSION_ID = "sessionId";

    /* secondary PK (col prefixes) */
    public static final String COORDINATOR = "coordinator";
    public static final String EVENT_ID = "eventId";

    /* session table columns */
    public static final String SESSION_REQUEST = "request";
    public static final ByteBuffer SESSION_REQUEST_BB = ByteBufferUtil.bytes(SESSION_REQUEST);
    public static final String SESSION_START = "startedAt";
    public static final ByteBuffer SESSION_START_BB = ByteBufferUtil.bytes(SESSION_START);

    /* event table columns */
    public static final String SOURCE = "source";
    public static final ByteBuffer SOURCE_BB = ByteBufferUtil.bytes(SOURCE);
    public static final String TYPE = "type";
    public static final ByteBuffer TYPE_BB = ByteBufferUtil.bytes(TYPE);
    public static final String NAME = "name";
    public static final ByteBuffer NAME_BB = ByteBufferUtil.bytes(NAME);
    public static final String DESCRIPTION = "description";
    public static final ByteBuffer DESCRIPTION_BB = ByteBufferUtil.bytes(DESCRIPTION);
    public static final String DURATION = "duration";
    public static final ByteBuffer DURATION_BB = ByteBufferUtil.bytes(DURATION);
    public static final String HAPPENED = "happened_at";
    public static final ByteBuffer HAPPENED_BB = ByteBufferUtil.bytes(HAPPENED);
    public static final String PAYLOAD_TYPES = "payload_types";
    public static final ByteBuffer PAYLOAD_TYPES_BB = ByteBufferUtil.bytes(PAYLOAD_TYPES);
    public static final String PAYLOAD = "payload";
    public static final ByteBuffer PAYLOAD_BB = ByteBufferUtil.bytes(PAYLOAD);

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);

    public static final MapType PAYLOAD_TYPE = MapType.getInstance(UTF8Type.instance, BytesType.instance);

    public static final CompositeType SESSION_TYPE = CompositeType.getInstance(ImmutableList
            .<AbstractType<?>> of(InetAddressType.instance, UTF8Type.instance
            ));

    public static final CompositeType EVENT_TYPE = CompositeType.getInstance(ImmutableList
            .<AbstractType<?>> of(InetAddressType.instance, TimeUUIDType.instance, UTF8Type.instance
            ));

    public static final CFMetaData sessionsCfm = compile("CREATE TABLE " + TRACE_KEYSPACE + "." + SESSIONS_TABLE
            + " (" +
            "  " + SESSION_ID + "        timeuuid," +
            "  " + COORDINATOR + "       inet," +
            "  " + SESSION_START + "     timestamp," +
            "  " + SESSION_REQUEST + "   text," +
            "  PRIMARY KEY (" + SESSION_ID + ", " + COORDINATOR + "));");

    private static final CFMetaData eventsCfm = compile("CREATE TABLE " + TRACE_KEYSPACE + "." + EVENTS_TABLE + " (" +
            "  " + SESSION_ID + "        timeuuid," +
            "  " + COORDINATOR + "       inet," +
            "  " + EVENT_ID + "          timeuuid," +
            "  " + TYPE + "              text," +
            "  " + SOURCE + "            inet," +
            "  " + NAME + "              text," +
            "  " + DURATION + "          bigint," +
            "  " + HAPPENED + "          timestamp," +
            "  " + DESCRIPTION + "       text," +
            "  " + PAYLOAD_TYPES + "     map<text, text>," +
            "  " + PAYLOAD + "           map<text, blob>," +
            "  PRIMARY KEY (" + SESSION_ID + ", " + COORDINATOR + ", " + EVENT_ID + "));");

    /**
     * Trace session meta events.
     */
    public enum TraceEventEnum
    {
        /**
         * Signals the start of a trace session (is possibly accompanied with arguments and/or argument descriptors)
         */
        TRACE_SESSION_BEGIN,
        /**
         * Signals a new stage runnable was started within this trace.
         */
        STAGE_BEGIN,
        /**
         * Signals a stage runnable finished within this trace.
         */
        STAGE_FINISH,
        /**
         * Signals a locally initiated trace session's end (is possibly accompanied with results and/or result
         * descriptors)
         */
        TRACE_SESSION_END;

        /**
         * overload the default name() method to allow to include a description with the trace event
         */
        public String name(String desc)
        {
            return new StringBuilder().append(name()).append("[").append(desc).append("]").toString();
        }
    }

    private static TraceSessionContext ctx;

    private InetAddress localAddress;
    private int timeToLive = 86400;
    private ThreadLocal<TraceSessionContextThreadLocalState> sessionContextThreadLocalState = new ThreadLocal<TraceSessionContextThreadLocalState>();

    protected TraceSessionContext()
    {
        logger.info("Initializing Trace session context.");
        if (!Iterables.tryFind(Schema.instance.getTables(), new Predicate<String>()
        {
            public boolean apply(String keyspace)
            {
                return keyspace.equals(TRACE_KEYSPACE);
            }

        }).isPresent())
        {
            try
            {
                KSMetaData traceKs = KSMetaData.newKeyspace(TRACE_KEYSPACE, SimpleStrategy.class.getName(),
                        ImmutableMap.of("replication_factor", "1"));
                MigrationManager.announceNewKeyspace(traceKs);
                MigrationManager.announceNewColumnFamily(sessionsCfm);
                MigrationManager.announceNewColumnFamily(eventsCfm);
            }
            catch (ConfigurationException e)
            {
                Throwables.propagate(e);
            }
        }

        this.localAddress = FBUtilities.getLocalAddress();
    }

    public UUID prepareSession()
    {
        return UUIDGen.getUUID(ByteBuffer.wrap(UUIDGen.getTimeUUIDBytes()));
    }

    public void startPreparedSession(UUID sessionId, String request)
    {
        startSession(sessionId, request, System.currentTimeMillis());
    }

    public UUID startSession(String request)
    {
        UUID sessionId = prepareSession();
        startSession(sessionId, request, System.currentTimeMillis());
        return sessionId;
    }

    public void startSession(UUID sessionId, String request, long timestamp)
    {
        assert sessionContextThreadLocalState.get() == null;

        byte[] sessionIdAsBB = TimeUUIDType.instance.decompose(sessionId).array();

        TraceSessionContextThreadLocalState tsctls = new TraceSessionContextThreadLocalState(localAddress,
                localAddress, sessionIdAsBB);

        sessionContextThreadLocalState.set(tsctls);

        newSession(sessionIdAsBB, localAddress, request, timestamp);
    }

    public void stopSession()
    {
        trace(TraceEvent.Type.SESSION_END.builder().build());
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

    public UUID getSessionId()
    {
        return isTracing() ? UUIDGen.getUUID(ByteBuffer.wrap(sessionContextThreadLocalState.get().sessionId)) : null;
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
     */
    public void update(final MessageIn<?> message, String id)
    {
        final byte[] queryContextBytes = (byte[]) message.parameters
                .get(TraceSessionContext.TRACE_SESSION_CONTEXT_HEADER);

        // if the message has no session context header don't do tracing
        if (queryContextBytes == null)
            return;

        checkState(queryContextBytes.length > 0);
        final DataInputStream dis = new DataInputStream(new ByteArrayInputStream(queryContextBytes));
        final byte[] sessionId;
        try
        {
            sessionId = new byte[dis.readInt()];
            dis.read(sessionId);
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
    public byte[] getSessionContextHeader()
    {
        if (!isLocalTraceSession())
            return null;

        // this uses a FBA so no need to close()
        @SuppressWarnings("resource")
        final DataOutputBuffer buffer = new DataOutputBuffer();
        try
        {
            final TraceSessionContextThreadLocalState tls = sessionContextThreadLocalState.get();
            buffer.writeInt(tls.sessionId.length);
            buffer.write(tls.sessionId);
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
    private void newSession(byte[] sessionId, InetAddress coordinator, String request, long startedAt)
    {
        ColumnFamily family = ColumnFamily.create(sessionsCfm);
        ByteBuffer coordinatorAsBb = ByteBuffer.wrap(coordinator.getAddress());
        family.addColumn(column(buildName(sessionsCfm, coordinatorAsBb, SESSION_START_BB), startedAt));
        family.addColumn(column(buildName(sessionsCfm, coordinatorAsBb, SESSION_REQUEST_BB), request));
        store(sessionId, family);
    }

    public UUID trace(TraceEvent event)
    {
        if (isTracing())
        {
            ColumnFamily family = ColumnFamily.create(eventsCfm);
            ByteBuffer coordinatorAsBB = bytes(event.coordinator());
            ByteBuffer eventIdAsBB = ByteBuffer.wrap(event.id());
            family.addColumn(column(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, SOURCE_BB), event.source()));
            family.addColumn(column(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, NAME_BB), event.name()));
            family.addColumn(column(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, DURATION_BB), event.duration()));
            family.addColumn(column(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, HAPPENED_BB), event.timestamp()));
            family.addColumn(column(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, DESCRIPTION_BB),
                    event.description()));
            family.addColumn(typeColumns(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, PAYLOAD_TYPES_BB),
                    event.payloadTypes()));
            family.addColumn(payloadColumn(buildName(eventsCfm, coordinatorAsBB, eventIdAsBB, PAYLOAD_BB),
                    event.rawPayload()));
            store(event.sessionId(), family);
            return UUIDGen.getUUID(ByteBuffer.wrap(event.id()));
        }
        return null;
    }

    private ByteBuffer buildName(CFMetaData meta, ByteBuffer... args)
    {
        ColumnNameBuilder builder = meta.getCfDef().getColumnNameBuilder();
        for (ByteBuffer arg : args)
        {
            builder.add(arg);
        }
        return builder.build();
    }

    private Column column(ByteBuffer columnName, long value)
    {
        return new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), timeToLive);
    }

    private Column column(ByteBuffer columnName, String value)
    {
        return new ExpiringColumn(columnName, ByteBufferUtil.bytes(value), System.currentTimeMillis(), timeToLive);
    }

    private Column column(ByteBuffer columnName, InetAddress address)
    {
        return new ExpiringColumn(columnName, bytes(address), System.currentTimeMillis(), timeToLive);
    }

    private IColumn typeColumns(ByteBuffer columnName, Map<String, AbstractType<?>> payloadTypes)
    {
        return new ExpiringColumn(columnName, MapType.getInstance(BytesType.instance, BytesType.instance).decompose(
                columnName), System.currentTimeMillis(), timeToLive);
    }

    private IColumn payloadColumn(ByteBuffer columnName, Map<ByteBuffer, ByteBuffer> rawPayload)
    {
        return null;
    }

    private ByteBuffer bytes(InetAddress address)
    {
        return ByteBuffer.wrap(address.getAddress());
    }

    /**
     * Separated and made visible so that we can override the actual storage for testing purposes.
     */
    @VisibleForTesting
    protected void store(final byte[] key, final ColumnFamily family)
    {
        try
        {
            StageManager.getStage(Stage.TRACING).execute(new Runnable()
            {
                public void run()
                {
                    RowMutation mutation = new RowMutation(TRACE_KEYSPACE, ByteBuffer.wrap(key));
                    mutation.add(family);
                    try
                    {
                        StorageProxy.mutate(Arrays.asList(mutation), ConsistencyLevel.ANY);
                    }
                    catch (Exception e)
                    {
                        log(key, family, "row mutation failed", e);
                    }
                }
            });
        }
        catch (RejectedExecutionException e)
        {
            log(key, family, "trace storage rejected", e);
        }
    }

    private void log(byte[] key, final ColumnFamily family, String message, Throwable t)
    {

    }

    @VisibleForTesting
    public void setLocalAddress(InetAddress localAddress)
    {
        this.localAddress = localAddress;
    }

    public void setTimeToLive(int timeToLive)
    {
        this.timeToLive = timeToLive;
    }

    private static CFMetaData compile(String cql)
    {
        CreateColumnFamilyStatement statement = null;
        try
        {
            statement = (CreateColumnFamilyStatement) QueryProcessor.parseStatement(cql)
                    .prepare().statement;

            CFMetaData newCFMD = new CFMetaData(TRACE_KEYSPACE, statement.columnFamily(), ColumnFamilyType.Standard,
                    statement.comparator,
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

    @VisibleForTesting
    public static void setCtx(TraceSessionContext context)
    {
        ctx = context;
    }

    /**
     * Fetches and lazy initializes the trace context.
     */
    public static TraceSessionContext traceCtx()
    {
        return ctx;
    }

    public static void initialize()
    {
        ctx = new TraceSessionContext();
    }
}