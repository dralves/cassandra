package org.apache.cassandra.stress.util;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra.Client;
import org.apache.thrift.protocol.TProtocol;

public class CassandraClient extends Client
{
    public Map<Integer, Integer> preparedStatements = new HashMap<Integer, Integer>();
    
    public CassandraClient(TProtocol protocol)
    {
        super(protocol);
    }
}
