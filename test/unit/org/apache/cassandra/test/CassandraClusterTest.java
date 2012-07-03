package org.apache.cassandra.test;

import org.junit.Test;

import org.apache.thrift.TException;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.cassandra.thrift.CassandraDaemon;

public class CassandraClusterTest
{

    @Test
    public void testRunThreeNodeCluster() throws TException
    {
        CassandraTestCluster cluster = new CassandraTestCluster("three-node", "org.apache.cassandra");
        cluster.node("node1").start(CassandraDaemon.class);
        cluster.node("node2").start(CassandraDaemon.class);
        cluster.node("node3").start(CassandraDaemon.class);

        Client client = cluster.node("node1").client();
        client.send_describe_keyspaces();
    }

}
