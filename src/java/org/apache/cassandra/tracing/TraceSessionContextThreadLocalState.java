package org.apache.cassandra.tracing;

import static com.google.common.base.Preconditions.checkNotNull;

import java.net.InetAddress;
import java.util.UUID;

import com.google.common.base.Stopwatch;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TraceSessionContextThreadLocalState
{
    public static final Logger logger = LoggerFactory.getLogger(TraceSessionContextThreadLocalState.class);

    public final UUID sessionId;
    public final InetAddress origin;
    public final InetAddress source;
    public final String messageId;
    public final Stopwatch watch;

    public TraceSessionContextThreadLocalState(final TraceSessionContextThreadLocalState other)
    {
        this(other.origin, other.source, other.sessionId, other.messageId);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final UUID sessionId)
    {
        this(coordinator, source, sessionId, null);
    }

    public TraceSessionContextThreadLocalState(final InetAddress coordinator, final InetAddress source,
            final UUID sessionId,
            final String messageId)
    {
        checkNotNull(coordinator);
        checkNotNull(source);
        checkNotNull(sessionId);
        logger.info("Created new tracing thread local state for session " + sessionId + " in " + source
                + " that started in " + coordinator + ", messageId: " + messageId);
        this.origin = coordinator;
        this.source = source;
        this.sessionId = sessionId;
        this.messageId = ((messageId == null) || (messageId.length() == 0)) ? null : messageId;
        this.watch = new Stopwatch();
        this.watch.start();
    }
}
