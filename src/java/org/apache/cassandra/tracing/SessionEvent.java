package org.apache.cassandra.tracing;

import java.util.UUID;

public class SessionEvent
{

    private UUID sessionId;
    private String request;
    private long startedAt;

    public SessionEvent(UUID sessionId, String request, long startedAt)
    {
        this.sessionId = sessionId;
        this.request = request;
        this.startedAt = startedAt;
    }

    public String request()
    {
        return request;
    }

    public UUID sessionId()
    {
        return sessionId;
    }

    public long startedAt()
    {
        return startedAt;
    }

}
