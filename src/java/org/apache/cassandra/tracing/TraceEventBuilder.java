package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.tracing.TraceSessionContext.traceCtx;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.Maps;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.LongType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.UUIDGen;
import org.apache.thrift.TBase;
import org.apache.thrift.TEnum;

public class TraceEventBuilder
{

    private byte[] sessionId;
    private String name;
    private String description;
    private Long duration;
    private Long timestamp;
    private byte[] eventId;
    private InetAddress coordinator;
    private InetAddress source;
    private Map<String, AbstractType<?>> payloadTypes = Maps.newHashMap();
    private Map<String, ByteBuffer> payload = Maps.newHashMap();
    private TraceEvent.Type type;

    public TraceEventBuilder()
    {
    }

    public TraceEventBuilder sessionId(byte[] sessionId)
    {
        this.sessionId = sessionId;
        return this;
    }

    public TraceEventBuilder sessionId(UUID sessionId)
    {
        this.sessionId = UUIDGen.decompose(sessionId);
        return this;
    }

    public TraceEventBuilder name(String name)
    {
        this.name = name;
        return this;
    }

    public TraceEventBuilder description(String description)
    {
        this.description = description;
        return this;
    }

    public TraceEventBuilder duration(long duration)
    {
        this.duration = duration;
        return this;
    }

    public TraceEventBuilder timestamp(long timestamp)
    {
        this.timestamp = timestamp;
        return this;
    }

    public TraceEventBuilder eventId(byte[] eventId)
    {
        this.eventId = eventId;
        return this;
    }

    public TraceEventBuilder coordinator(InetAddress coordinator)
    {
        this.coordinator = coordinator;
        return this;
    }

    public TraceEventBuilder source(InetAddress source)
    {
        this.source = source;
        return this;
    }

    public TraceEventBuilder type(TraceEvent.Type type)
    {
        this.type = type;
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<?> type, T value)
    {
        @SuppressWarnings("unchecked")
        ByteBuffer encoded = ((AbstractType<T>) type).decompose(value);
        this.payloadTypes.put(name, type);
        this.payload.put(name, encoded);
        return this;
    }

    public TraceEventBuilder addPayload(String name, TEnum thriftEnum)
    {
        // TODO finish serializing thrift enums (important for consistency level)
        return this;
    }

    public TraceEventBuilder addPayload(String name, TBase<?, ?> thriftObject)
    {
        ThriftObjectType type = ThriftObjectType.getInstance(thriftObject.getClass());
        this.payloadTypes.put(name, type);
        this.payload.put(name, type.decompose(thriftObject));
        return this;
    }

    public TraceEventBuilder addPayload(String name, ByteBuffer byteBuffer)
    {
        this.payloadTypes.put(name, BytesType.instance);
        this.payload.put(name, byteBuffer);
        return this;
    }

    public TraceEventBuilder addPayload(String name, int value)
    {
        this.payloadTypes.put(name, Int32Type.instance);
        this.payload.put(name, Int32Type.instance.decompose(value));
        return this;
    }

    public TraceEventBuilder addPayload(String name, long value)
    {
        this.payloadTypes.put(name, LongType.instance);
        this.payload.put(name, LongType.instance.decompose(value));
        return this;
    }

    public TraceEventBuilder addPayload(String name, String value)
    {
        this.payloadTypes.put(name, UTF8Type.instance);
        this.payload.put(name, UTF8Type.instance.decompose(value));
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<T> componentsType, List<T> componentList)
    {
        // TODO finish serializing lists
        return this;
    }

    public TraceEvent build()
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
            eventId = UUIDGen.getTimeUUIDBytes();
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

        return new TraceEvent(name, description, duration, timestamp, sessionId, eventId, coordinator, source, type,
                payload, payloadTypes);
    }
}
