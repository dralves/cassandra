package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;

import com.google.common.base.Stopwatch;

public class TraceSessionContextThreadLocalState
{
    public final byte[] sessionId;
    public final InetAddress origin;
    public final InetAddress source;
    public final String messageId;
    public final Stopwatch watch;

    public TraceSessionContextThreadLocalState(final TraceSessionContextThreadLocalState other)
    {
        this(other.origin, other.source, other.sessionId, other.messageId);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final byte[] sessionId)
    {
        this(coordinator, source, sessionId, null);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final byte[] sessionId,
            final String messageId)
    {
        checkNotNull(coordinator);
        checkNotNull(source);
        checkNotNull(sessionId);

        this.origin = coordinator;
        this.source = source;
        this.sessionId = sessionId;
        this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
        this.watch = new Stopwatch();
        this.watch.start();
    }
}