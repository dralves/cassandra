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
import static org.apache.cassandra.service.TraceSessionContext.traceCtx;

import java.net.InetAddress;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;
import org.apache.cassandra.db.Table;
import org.apache.cassandra.db.filter.QueryFilter;
import org.apache.cassandra.utils.FBUtilities;

public class TraceSessionContextTest extends SchemaLoader
{
    private static int sessionId;
    private static InetAddress coordinator;

    @BeforeClass
    public static void setUp()
    {
        coordinator = FBUtilities.getLocalAddress();
        sessionId = traceCtx().startSession("test_session");
        assertTrue(traceCtx().isTracing());
        assertTrue(traceCtx().isLocalTraceSession());
        assertNotNull(traceCtx().threadLocalState());
        // TODO assert that stuff is stored
    }

    @Test
    public void testLocalEvent()
    {
        traceCtx().trace("local_event");
        traceCtx().trace("local_event2");
        Table.open(TraceSessionContext.TRACE_KEYSPACE).getColumnFamilyStore(TraceSessionContext.EVENTS_TABLE)
                .getColumnFamily(new QueryFilter(null, null, null));

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
