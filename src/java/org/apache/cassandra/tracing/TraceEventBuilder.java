package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.apache.cassandra.tracing.TraceSessionContext.traceCtx;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.UUIDGen;

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
    private Map<String, AbstractType<?>> payloadTypes;
    private Map<ByteBuffer, ByteBuffer> payload;
    private TraceEvent.Type type;

    public TraceEventBuilder sessionId(byte[] sessionId)
    {
        this.sessionId = sessionId;
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

    public TraceEventBuilder payloadTypes(Map<String, AbstractType<?>> payloadTypes)
    {
        this.payloadTypes = payloadTypes;
        return this;
    }

    @SuppressWarnings({ "unchecked", "rawtypes" })
    public <T> TraceEventBuilder addPayload(String name, Object value)
    {
        ByteBuffer nameAsBB = bytes(name);
        if (value instanceof ByteBuffer)
        {
            payload.put(nameAsBB, (ByteBuffer) value);
            return this;
        }

        if (payloadTypes.containsValue(bytes(name)))
        {
            AbstractType type = payloadTypes.get(nameAsBB);
            payload.put(nameAsBB, type.decompose(value));
            return this;
        }

        throw new IllegalArgumentException("value type must be either mapped in payload types or be of type bytebuffer");
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

        return new TraceEvent(name, description, duration, timestamp, sessionId, eventId, coordinator, source, payload,
                payloadTypes);
    }
}
