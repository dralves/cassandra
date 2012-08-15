package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Collections;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.UUIDGen;

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
    private final UUID sessionId;
    private final UUID eventId;
    private final InetAddress coordinator;
    private final InetAddress source;
    private final Map<String, ByteBuffer> rawPayload;
    private final Map<String, AbstractType<?>> payloadTypes;
    private final Type type;

    TraceEvent(String name, String description, long duration, long timestamp, UUID sessionId, UUID eventId,
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

    public UUID sessionId()
    {
        return sessionId;
    }

    public ByteBuffer sessionIdAsBB()
    {
        return ByteBuffer.wrap(UUIDGen.decompose(sessionId));
    }

    public UUID id()
    {
        return eventId;
    }

    public ByteBuffer idAsBB()
    {
        return ByteBuffer.wrap(UUIDGen.decompose(eventId));
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
    public String toString()
    {
        StringBuilder builder = new StringBuilder();
        builder.append("TraceEvent [name=").append(name).append(", description=").append(description)
                .append(", duration=").append(duration).append(", timestamp=").append(timestamp).append(", sessionId=")
                .append(sessionId).append(", eventId=").append(eventId).append(", coordinator=").append(coordinator)
                .append(", source=").append(source).append(", rawPayload=").append(rawPayload)
                .append(", payloadTypes=").append(payloadTypes).append(", type=").append(type).append("]");
        return builder.toString();
    }

    @Override
    public int hashCode()
    {
        final int prime = 31;
        int result = 1;
        result = prime * result + eventId.hashCode();
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
        if (!eventId.equals(other.eventId))
            return false;
        return true;
    }

}