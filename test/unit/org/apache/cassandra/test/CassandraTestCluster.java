package org.apache.cassandra.test;

import java.net.URL;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CassandraTestCluster
{
    private String clusterName;
    private String[] packages;

    private Map<String, CassandraTestNode> nodes = new HashMap<String, CassandraTestNode>();

    private Map<String, String> props = new HashMap<String, String>();
    private List<URL> incClasspath = new ArrayList<URL>();
    private List<URL> excClasspath = new ArrayList<URL>();

    public CassandraTestCluster(String name, String... packages)
    {
        this.clusterName = name;
        this.packages = packages;
    }

    public String getClusterProp(String prop)
    {
        return props.get(prop);
    }

    public void setProp(String prop, String value)
    {
        props.put(prop, value);
        for (CassandraTestNode node : nodes.values())
        {
            node.setProp(prop, value);
        }
    }

    public void addToClasspath(URL url)
    {
        incClasspath.add(url);
        for (CassandraTestNode node : nodes.values())
        {
            node.addToClasspath(url);
        }
    }

    public void removeFromClasspath(URL url)
    {
        excClasspath.add(url);
        for (CassandraTestNode node : nodes.values())
        {
            node.removeFromClasspath(url);
        }
    }

    public CassandraTestNode node(String name)
    {
        if (!nodes.containsKey(name))
        {
            createViNode(name);
        }
        return nodes.get(name);
    }

    private void createViNode(String name)
    {
        Isolate is = new Isolate(clusterName + "." + name, packages);
        is.setProp(props);
        for (URL u : incClasspath)
        {
            is.addToClasspath(u);
        }
        for (URL u : excClasspath)
        {
            is.removeFromClasspath(u);
        }

        CassandraTestNode vnode = new CassandraTestNode(this, name, is);
        nodes.put(name, vnode);
    }

    void remove(CassandraTestNode node)
    {
        nodes.values().remove(node);
    }

    public void shutdown()
    {
        for (CassandraTestNode node : new ArrayList<CassandraTestNode>(nodes.values()))
        {
            node.shutdown();
        }
        nodes.clear();
        for (int i = 0; i != 3; ++i)
        {
            System.gc();
            Runtime.getRuntime().runFinalization();
            try
            {
                Thread.sleep(10);
            }
            catch (InterruptedException e)
            {
            }
        }
    }

    public void kill()
    {
        for (CassandraTestNode node : new ArrayList<CassandraTestNode>(nodes.values()))
        {
            node.kill();
        }
    }

    public void setProp(Map<String, String> props)
    {
        this.props.putAll(props);
        for (CassandraTestNode node : nodes.values())
        {
            node.setProp(props);
        }
    }
}
