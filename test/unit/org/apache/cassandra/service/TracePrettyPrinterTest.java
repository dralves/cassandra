package org.apache.cassandra.service;

import java.io.IOException;
import java.util.UUID;

import org.junit.BeforeClass;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.tracing.TraceSessionContext;

public class TracePrettyPrinterTest extends SchemaLoader
{
    private static UUID sessionId;
    private static TraceSessionContext ctx;

    @BeforeClass
    public static void loadSchemaAndSaveEvents() throws IOException
    {
        SchemaLoader.loadSchema();
        TraceSessionContext.initialize();
        ctx = TraceSessionContext.traceCtx();
        sessionId = ctx.newSession();

    }

}
