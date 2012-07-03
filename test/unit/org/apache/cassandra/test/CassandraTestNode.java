/**
 * Copyright 2011 Alexey Ragozin
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.cassandra.test;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.util.Map;
import java.util.concurrent.Callable;

import org.apache.cassandra.stress.util.CassandraClient;
import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * @author Alexey Ragozin (alexey.ragozin@gmail.com)
 */
public class CassandraTestNode
{

    private CassandraTestCluster cluster;

    private Isolate isolate;
    private boolean started;
    private boolean terminated;

    public CassandraTestNode(CassandraTestCluster cluster, String name, Isolate isolate)
    {
        this.cluster = cluster;
        this.isolate = isolate;
    }

    public String getName()
    {
        return isolate.getName();
    }

    public Isolate getIsolate()
    {
        return isolate;
    }

    public void addToClasspath(URL path)
    {
        isolate.addToClasspath(path);
    }

    public void removeFromClasspath(URL path)
    {
        isolate.removeFromClasspath(path);
    }

    public void setProp(String prop, String value)
    {
        isolate.setProp(prop, value);
    }

    public void setProp(Map<String, String> props)
    {
        isolate.setProp(props);
    }

    public void start()
    {
        if (!started)
        {
            isolate.start();
            started = true;
        }
    }

    public void start(final Class<?> main, final String... args)
    {
        if (!started)
        {
            start();
        }
        final String name = getName();
        isolate.exec(new Runnable()
        {
            @Override
            public void run()
            {
                Thread t = new CassandraTestNodeMain(args, main);
                t.setDaemon(true);
                t.setName(name + "-Main");
                t.start();
            }
        });
    }

    public void exec(Runnable task)
    {
        if (!started)
        {
            start();
        }
        isolate.exec(task);
    }

    public <V> V exec(Callable<V> task)
    {
        if (!started)
        {
            start();
        }
        return isolate.exec(task);
    }

    public <V> V export(Callable<V> task)
    {
        if (!started)
        {
            start();
        }
        return isolate.export(task);
    }

    public void suspend()
    {
        isolate.suspend();
    }

    public void resume()
    {
        isolate.resume();
    }

    public void shutdown()
    {
        if (!terminated)
        {
            kill();
        }
    }

    public void kill()
    {
        if (!started)
        {
            throw new IllegalStateException("can't kill not started node " + getName());
        }
        if (!terminated)
        {
            isolate.stop();
            isolate = null;
            terminated = true;
            cluster.remove(this);
        }
    }

    private static class CassandraTestNodeMain extends Thread
    {
        private final String[] args;
        private final Class<?> main;

        private CassandraTestNodeMain(String[] args, Class<?> main)
        {
            this.args = args;
            this.main = main;
        }

        @Override
        public void run()
        {
            try
            {
                Method mm = main.getDeclaredMethod("main", String[].class);
                mm.setAccessible(true);
                try
                {
                    mm.invoke(null, ((Object) args));
                }
                catch (InvocationTargetException e)
                {
                    if (e.getCause() instanceof ThreadDeath)
                    {
                        // ignore;
                    }
                    else
                    {
                        throw e;
                    }
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Failed to execute main() at " + main.getName(), e);
            }
        }
    }

    public Client client()
    {
        TSocket socket = new TSocket("127.0.0.1", 9160);
        TTransport transport = new TFramedTransport(socket);
        return new CassandraClient(new TBinaryProtocol(transport));
    }
}
