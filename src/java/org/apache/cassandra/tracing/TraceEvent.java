package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.db.marshal.AbstractType;

public class TraceEvent
{

    /**
     * A predefined set of events with fixed names and
     */
    public enum Type
    {
        SESSION_START,
        STAGE_START,
        STAGE_FINISH,
        SESSION_END,
        MISC;

        public TraceEventBuilder builder()
        {
            return new TraceEventBuilder().type(this);
        }

    }

    private final String name;
    private final String description;
    private final long duration;
    private final long timestamp;
    private final byte[] sessionId;
    private final byte[] eventId;
    private final InetAddress coordinator;
    private final InetAddress source;
    private final Map<String, ByteBuffer> rawPayload;
    private final Map<String, AbstractType<?>> payloadTypes;
    private final Type type;

    TraceEvent(String name, String description, long duration, long timestamp, byte[] sessionId, byte[] eventId,
            InetAddress coordinator, InetAddress source, Type type, Map<String, ByteBuffer> rawPayload,
            Map<String, AbstractType<?>> payloadTypes)
    {
        this.name = name;
        this.description = description;
        this.duration = duration;
        this.timestamp = timestamp;
        this.sessionId = sessionId;
        this.eventId = eventId;
        this.coordinator = coordinator;
        this.source = source;
        this.rawPayload = rawPayload;
        this.payloadTypes = payloadTypes;
        this.type = type;

    }

    public String name()
    {
        return name;
    }

    public String description()
    {
        return description;
    }

    public long duration()
    {
        return duration;
    }

    public long timestamp()
    {
        return timestamp;
    }

    public byte[] sessionId()
    {
        return sessionId;
    }

    public byte[] id()
    {
        return eventId;
    }

    public InetAddress coordinator()
    {
        return coordinator;
    }

    public InetAddress source()
    {
        return source;
    }

    public Type type()
    {
        return type;
    }

    @SuppressWarnings("unchecked")
    public <T> T getFromPayload(String name)
    {
        if (rawPayload.containsKey(name))
        {
            if (payloadTypes.containsKey(name))
            {
                return (T) payloadTypes.get(name).compose(rawPayload.get(name));
            }
            return (T) rawPayload.get(name);
        }
        return null;
    }

    public Set<String> payloadNames()
    {
        return rawPayload.keySet();
    }

    public Map<String, ByteBuffer> rawPayload()
    {
        return Collections.unmodifiableMap(rawPayload);
    }

    public Map<String, AbstractType<?>> payloadTypes()
    {
        return Collections.unmodifiableMap(payloadTypes);
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + Arrays.hashCode(eventId);
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        TraceEvent other = (TraceEvent) obj;
        if (!Arrays.equals(eventId, other.eventId))
            return false;
        return true;
    }

}