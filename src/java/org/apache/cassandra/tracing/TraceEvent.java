package org.apache.cassandra.tracing;

import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TraceEvent
{

    /**
     * A predefined set of events with fixed names and 
     * @author David Alves
     *
     */
    public enum Type
    {
        SESSION_START,
        STAGE_START,
        STAGE_FINISH,
        SESSION_END,
        MISC;
        
        

    }

    private final String name;
    private final String description;
    private final long duration;
    private final long timestamp;
    private final byte[] sessionId;
    private final byte[] eventId;
    private final InetAddress coordinator;
    private final InetAddress source;
    private final Map<ByteBuffer, ByteBuffer> rawPayload;
    private final Map<String, AbstractType<?>> payloadTypes;

    TraceEvent(String name, String description, long duration, long timestamp, byte[] sessionId, byte[] eventId,
            InetAddress coordinator, InetAddress source, Map<ByteBuffer, ByteBuffer> rawPayload,
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

    public byte[] eventId()
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

    @SuppressWarnings("unchecked")
    public <T> T getFromPayload(String name)
    {
        ByteBuffer nameAsBB = ByteBufferUtil.bytes(name);
        if (rawPayload.containsKey(nameAsBB))
        {
            if (payloadTypes.containsKey(nameAsBB))
            {
                return (T) payloadTypes.get(nameAsBB).compose(rawPayload.get(nameAsBB));
            }
            return (T) rawPayload.get(nameAsBB);
        }
        return null;
    }

    public Map<String, AbstractType<?>> payloadTypes()
    {
        return payloadTypes;
    }
}