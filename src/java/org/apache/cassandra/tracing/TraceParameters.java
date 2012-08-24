package org.apache.cassandra.tracing;

import static org.apache.cassandra.utils.ByteBufferUtil.bytesToHex;

import java.nio.ByteBuffer;


public class TraceParameters
{
    public static String toString(ByteBuffer bytebuffer)
    {
        return bytesToHex(bytebuffer);
    }

}
