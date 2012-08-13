package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.tracing.TraceSessionContext.EVENT_TYPE;
import static org.apache.cassandra.tracing.TraceSessionContext.traceCtx;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.base.Supplier;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Sets;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.AbstractCompositeType.CompositeComponent;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BooleanType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.InetAddressType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.db.marshal.UUIDType;
import org.apache.cassandra.tracing.TraceEvent.Type;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;

public class TraceEventBuilder
{

    private static final Logger logger = LoggerFactory.getLogger(TraceSessionContext.class);

    public static Set<TraceEvent> fromColumnFamily(UUID key, ColumnFamily cf)
    {
        Multimap<UUID, IColumn> eventColumns = Multimaps.newListMultimap(
                Maps.<UUID, Collection<IColumn>> newLinkedHashMap(),
                new Supplier<ArrayList<IColumn>>()
                {
                    @Override
                    public ArrayList<IColumn> get()
                    {
                        return Lists.newArrayList();
                    }
                });

        // split the columns by event
        for (IColumn column : cf)
        {
            List<CompositeComponent> components = EVENT_TYPE.deconstruct(column.name());
            UUID decodedEventId = ((UUID) components.get(1).comparator.compose(components.get(1).value));
            eventColumns.put(decodedEventId, column);
        }

        Set<TraceEvent> events = Sets.newLinkedHashSet();
        for (Map.Entry<UUID, Collection<IColumn>> entry : eventColumns.asMap().entrySet())
        {
            TraceEventBuilder builder = new TraceEventBuilder();
            builder.eventId(entry.getKey());
            builder.sessionId(key);
            boolean setCoordinator = false;
            for (IColumn col : entry.getValue())
            {
                List<CompositeComponent> components = EVENT_TYPE.deconstruct(col.name());
                if (!setCoordinator)
                {
                    builder.coordinator((InetAddress) components.get(0).comparator
                            .compose(components.get(0).value));
                    setCoordinator = true;
                }

                String colName = (String) components.get(2).comparator.compose(components.get(2).value);
                if (colName.equals(TraceSessionContext.DESCRIPTION))
                {
                    builder.description(UTF8Type.instance.compose(col.value()));
                    continue;
                }
                if (colName.equals(TraceSessionContext.DURATION))
                {
                    builder.duration(LongType.instance.compose(col.value()));
                    continue;
                }
                if (colName.equals(TraceSessionContext.HAPPENED))
                {
                    builder.timestamp(LongType.instance.compose(col.value()));
                    continue;
                }
                if (colName.equals(TraceSessionContext.NAME))
                {
                    builder.name(UTF8Type.instance.compose(col.value()));
                    continue;
                }
                if (colName.equals(TraceSessionContext.PAYLOAD))
                {
                    String payloadKey = UTF8Type.instance.compose(components.get(3).value);
                    builder.addPayloadRaw(payloadKey, col.value());
                    continue;
                }
                if (colName.equals(TraceSessionContext.PAYLOAD_TYPES))
                {
                    String payloadKey = UTF8Type.instance.compose(components.get(3).value);
                    try
                    {
                        builder.addPayloadTypeRaw(payloadKey,
                                new TypeParser(UTF8Type.instance.compose(col.value())).parse());
                    }
                    catch (ConfigurationException e)
                    {
                        logger.warn("error parsing payload type for payload key: " + payloadKey
                                + ", payload will have BytesType");
                        builder.addPayloadTypeRaw(payloadKey, BytesType.instance);
                    }
                    continue;
                }
                if (colName.equals(TraceSessionContext.SOURCE))
                {
                    builder.source(InetAddressType.instance.compose(col.value()));
                    continue;
                }
                if (colName.equals(TraceSessionContext.TYPE))
                {
                    builder.type(Type.valueOf(UTF8Type.instance.compose(col.value())));
                    continue;
                }
            }
            events.add(builder.build());
        }
        return events;
    }

    private UUID sessionId;
    private UUID eventId;
    private String name;
    private String description;
    private Long duration;
    private Long timestamp;
    private InetAddress coordinator;
    private InetAddress source;
    private Map<String, AbstractType<?>> payloadTypes = Maps.newHashMap();
    private Map<String, ByteBuffer> payload = Maps.newHashMap();
    private TraceEvent.Type type;

    public TraceEventBuilder sessionId(byte[] sessionId)
    {
        // check if isTracing so that we can have noop when not tracing (avoids having to do isTracing checks everywhere
        // on traced code)
        if (isTracing())
            this.sessionId = UUIDType.instance.compose(ByteBuffer.wrap(sessionId));
        return this;
    }

    public TraceEventBuilder sessionId(UUID sessionId)
    {
        if (isTracing())
            this.sessionId = sessionId;
        return this;
    }

    public TraceEventBuilder name(String name)
    {
        if (isTracing())
            this.name = name;
        return this;
    }

    public TraceEventBuilder description(String description)
    {
        if (isTracing())
            this.description = description;
        return this;
    }

    public TraceEventBuilder duration(long duration)
    {
        if (isTracing())
            this.duration = duration;
        return this;
    }

    public TraceEventBuilder timestamp(long timestamp)
    {
        if (isTracing())
            this.timestamp = timestamp;
        return this;
    }

    public TraceEventBuilder eventId(byte[] eventId)
    {
        if (isTracing())
            this.eventId = UUIDType.instance.compose(ByteBuffer.wrap(eventId));
        return this;
    }

    public TraceEventBuilder eventId(UUID eventId)
    {
        if (isTracing())
            this.eventId = eventId;
        return this;
    }

    public TraceEventBuilder coordinator(InetAddress coordinator)
    {
        if (isTracing())
            this.coordinator = coordinator;
        return this;
    }

    public TraceEventBuilder source(InetAddress source)
    {
        if (isTracing())
            this.source = source;
        return this;
    }

    public TraceEventBuilder type(TraceEvent.Type type)
    {
        if (isTracing())
            this.type = type;
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<?> type, T value)
    {
        if (isTracing())
        {
            @SuppressWarnings("unchecked")
            ByteBuffer encoded = ((AbstractType<T>) type).decompose(value);
            this.payloadTypes.put(name, type);
            this.payload.put(name, encoded);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, TEnum thriftEnum)
    {
        // TODO finish serializing thrift enums (important for consistency level)
        return this;
    }

    public TraceEventBuilder addPayload(String name, TBase<?, ?> thriftObject)
    {
        if (isTracing())
        {
            ThriftType type = ThriftType.getInstance(thriftObject.getClass());
            this.payloadTypes.put(name, type);
            this.payload.put(name, type.decompose(thriftObject));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, ByteBuffer byteBuffer)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, BytesType.instance);
            this.payload.put(name, byteBuffer);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, int value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, Int32Type.instance);
            this.payload.put(name, Int32Type.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, long value)
    {
        if (traceCtx().isTracing())
        {
            this.payloadTypes.put(name, LongType.instance);
            this.payload.put(name, LongType.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, boolean value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, BooleanType.instance);
            this.payload.put(name, BooleanType.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, String value)
    {
        if (isTracing())
        {
            this.payloadTypes.put(name, UTF8Type.instance);
            this.payload.put(name, UTF8Type.instance.decompose(value));
        }
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<T> componentsType, List<T> componentList)
    {
        // TODO finish serializing lists
        return this;
    }

    /**
     * Adds a payload without inserting type (which might come later). Used internally for deserialization.
     */
    private TraceEventBuilder addPayloadRaw(String name, ByteBuffer byteBuffer)
    {
        if (isTracing())
            this.payload.put(name, byteBuffer);
        return this;
    }

    private TraceEventBuilder addPayloadTypeRaw(String name, AbstractType<?> type)
    {
        if (isTracing())
            this.payloadTypes.put(name, type);
        return this;
    }

    public TraceEvent build()
    {
        if (isTracing())
        {
            if (coordinator == null)
            {
                coordinator = traceCtx().threadLocalState().origin;
                checkNotNull(coordinator,
                        "coordinator must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (source == null)
            {
                source = traceCtx().threadLocalState().source;
                checkNotNull(source,
                        "source must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (eventId == null)
            {
                eventId(UUIDGen.getTimeUUIDBytes());
            }

            if (type == null)
            {
                type = TraceEvent.Type.MISC;
            }

            if (name == null)
            {
                name = type.name();
            }
            if (description == null)
            {
                description = "";
            }
            if (sessionId == null)
            {
                sessionId = traceCtx().threadLocalState().sessionId;
                checkNotNull(sessionId,
                        "sessionId must be provided or be set at the current thread's TraceSessionContextThreadLocalState");
            }
            if (duration == null)
            {
                duration = traceCtx().threadLocalState().watch.elapsedTime(TimeUnit.NANOSECONDS);
                checkNotNull(duration,
                        "duration must be provided or be measured from the current thread's TraceSessionContextThreadLocalState");

            }
            if (timestamp == null)
            {
                timestamp = System.currentTimeMillis();
            }

            return new TraceEvent(name, description, duration, timestamp, sessionId, eventId, coordinator, source,
                    type,
                    payload, payloadTypes);
        }
        return null;
    }

    public boolean isTracing()
    {
        return traceCtx() != null && traceCtx().isTracing();
    }
}
