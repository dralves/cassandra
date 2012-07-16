/*
 *
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

package org.apache.cassandra.service;

import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertTrue;
import static org.apache.cassandra.service.TraceSessionContext.SESSIONS_TABLE;
import static org.apache.cassandra.service.TraceSessionContext.TRACE_KEYSPACE;
import static org.apache.cassandra.service.TraceSessionContext.traceCtx;

import java.io.IOException;
import java.net.InetAddress;
import java.nio.ByteBuffer;
import java.util.Map;

import com.google.common.base.Throwables;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.db.marshal.CompositeType;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.Util;
import org.apache.cassandra.config.ColumnDefinition;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.utils.FBUtilities;

public class TraceSessionContextTest extends SchemaLoader
{

    private static class LocalTraceSessionContext extends TraceSessionContext
    {

        /**
         * Override the paren't mutation that applies mutation to the cluster to instead apply mutations locally for
         * testing.
         */
        @Override
        protected void mutate(RowMutation mutation)
        {
            try
            {
                mutation.apply();
            }
            catch (IOException e)
            {
                Throwables.propagate(e);
            }
        }
    }

    private static byte[] sessionId;
    private static InetAddress coordinator;

    @BeforeClass
    public static void loadSchema() throws IOException
    {
        SchemaLoader.loadSchema();
        TraceSessionContext.setCtx(new LocalTraceSessionContext());
        coordinator = FBUtilities.getLocalAddress();
        byte[] addAsBytes = coordinator.getAddress();
        InetAddress address = InetAddress.getByAddress(addAsBytes);
        System.out.println("ADDR: " + address);
        sessionId = traceCtx().startSession("test_session");
        assertTrue(traceCtx().isTracing());
        assertTrue(traceCtx().isLocalTraceSession());
        assertNotNull(traceCtx().threadLocalState());
        ColumnFamily family = Table.open(TRACE_KEYSPACE)
                .getColumnFamilyStore(SESSIONS_TABLE)
                .getColumnFamily(
                        QueryFilter.getIdentityFilter(Util.dk(ByteBuffer.wrap(sessionId)), new QueryPath(
                                SESSIONS_TABLE)));
        System.out.println(family.getColumnCount());
        System.out.println(sessionId);
        
        CompositeType ct;
        
        for (IColumn column : family)
        {
            
            
            for (Map.Entry<ByteBuffer, ColumnDefinition> entry : TraceSessionContext.sessionsCfm.getColumn_metadata()
                    .entrySet())
            {
                
                
                System.out.println("K: " + new String(entry.getKey().array()));
                System.out.println("V: " + entry.getValue());
            }
            System.out.println("---");
        }

    }

    @Test
    public void testLocalEvent()
    {
        traceCtx().trace("local_event");
        traceCtx().trace("local_event2");

    }

    public void testRemoteExecutionRequest()
    {

    }

    public void testRemoteExecutionBegin()
    {

    }

    public void testRemoteExecutionEnd()
    {

    }

    public void testRemoteExecutionResponse()
    {

    }

    public void testSessionEnd()
    {

    }
}
