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
import org.apache.cassandra.db.marshal.BooleanType;
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
        // check if isTracing so that we can have noop when not tracing (avoids having to do isTracing checks everywhere on traced code)
        if (traceCtx().isTracing())
            this.sessionId = sessionId;
        return this;
    }

    public TraceEventBuilder sessionId(UUID sessionId)
    {
        if (traceCtx().isTracing())
            this.sessionId = UUIDGen.decompose(sessionId);
        return this;
    }

    public TraceEventBuilder name(String name)
    {
        if (traceCtx().isTracing())
            this.name = name;
        return this;
    }

    public TraceEventBuilder description(String description)
    {
        if (traceCtx().isTracing())
            this.description = description;
        return this;
    }

    public TraceEventBuilder duration(long duration)
    {
        if (traceCtx().isTracing())
            this.duration = duration;
        return this;
    }

    public TraceEventBuilder timestamp(long timestamp)
    {
        if (traceCtx().isTracing())
            this.timestamp = timestamp;
        return this;
    }

    public TraceEventBuilder eventId(byte[] eventId)
    {
        if (traceCtx().isTracing())
            this.eventId = eventId;
        return this;
    }

    public TraceEventBuilder coordinator(InetAddress coordinator)
    {
        if (traceCtx().isTracing())
            this.coordinator = coordinator;
        return this;
    }

    public TraceEventBuilder source(InetAddress source)
    {
        if (traceCtx().isTracing())
            this.source = source;
        return this;
    }

    public TraceEventBuilder type(TraceEvent.Type type)
    {
        if (traceCtx().isTracing())
            this.type = type;
        return this;
    }

    public <T> TraceEventBuilder addPayload(String name, AbstractType<?> type, T value)
    {
        if (traceCtx().isTracing())
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
        if (traceCtx().isTracing())
        {
            ThriftObjectType type = ThriftObjectType.getInstance(thriftObject.getClass());
            this.payloadTypes.put(name, type);
            this.payload.put(name, type.decompose(thriftObject));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, ByteBuffer byteBuffer)
    {
        if (traceCtx().isTracing())
        {
            this.payloadTypes.put(name, BytesType.instance);
            this.payload.put(name, byteBuffer);
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, int value)
    {
        if (traceCtx().isTracing())
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
        if (traceCtx().isTracing())
        {
            this.payloadTypes.put(name, BooleanType.instance);
            this.payload.put(name, BooleanType.instance.decompose(value));
        }
        return this;
    }

    public TraceEventBuilder addPayload(String name, String value)
    {
        if (traceCtx().isTracing())
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

    public TraceEvent build()
    {
        if (traceCtx().isTracing())
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

            return new TraceEvent(name, description, duration, timestamp, sessionId, eventId, coordinator, source,
                    type,
                    payload, payloadTypes);
        }
        return null;
    }
}
