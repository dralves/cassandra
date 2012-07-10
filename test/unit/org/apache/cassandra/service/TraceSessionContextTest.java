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

import static org.apache.cassandra.service.TraceSessionContext.traceCtx;

import org.junit.BeforeClass;
import org.junit.Test;

import org.apache.cassandra.SchemaLoader;

public class TraceSessionContextTest extends SchemaLoader
{
    private static ClientState state = new ClientState();
    private int sessionId;

    @BeforeClass
    public static void setUp()
    {
        state.setQueryDetails(true);
        traceCtx().startSession(state, "test_request");
        // TODO assert that stuff is there
    }

    @Test
    public void testLocalEvent()
    {
        
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
