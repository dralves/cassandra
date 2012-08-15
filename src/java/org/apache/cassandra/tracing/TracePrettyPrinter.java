package org.apache.cassandra.tracing;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import com.google.common.base.Supplier;
import com.google.common.collect.ComparisonChain;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.collect.Multimaps;
import com.google.common.collect.Ordering;
import com.google.common.collect.Sets;

import org.apache.cassandra.thrift.ConsistencyLevel;

public class TracePrettyPrinter
{

    public static void printSingleSessionTrace(UUID sessionId, Set<TraceEvent> events, PrintStream out)
    {
        TraceEvent first = Iterables.get(events, 0);
        Integer clValue = first.getFromPayload("consistency_level");
        ConsistencyLevel cl = null;
        if (clValue != null)
        {
            cl = ConsistencyLevel.valueOf(first.getFromPayload("consistency_level") + "");
        }
        InetAddress coordinator = first.coordinator();
        String eventName = first.name();

        TraceEvent last = null;
        Multimap<InetAddress, TraceEvent> eventsPerNode = Multimaps.newListMultimap(
                Maps.<InetAddress, Collection<TraceEvent>> newLinkedHashMap(),
                new Supplier<ArrayList<TraceEvent>>()
                {
                    @Override
                    public ArrayList<TraceEvent> get()
                    {
                        return Lists.newArrayList();
                    }
                });

        for (TraceEvent event : events)
        {
            last = event;
            eventsPerNode.put(event.source(), event);
        }

        long totalDuration = last.duration();

        out.println("Session Summary: " + sessionId);
        out.println("Total interacting nodes: " + eventsPerNode.keys().size() + " {" + eventsPerNode.keys() + "}");
        out.println("Total duration: " + totalDuration);
        out.println("Coordinator: " + coordinator);
        out.println("Replicas: " + Sets.difference(eventsPerNode.keySet(), ImmutableSet.of(coordinator)));
        out.println("Request: " + eventName);
        if (cl != null)
        {
            out.println("Consistency Level: " + cl.name());
        }

        List<Map.Entry<InetAddress, Collection<TraceEvent>>> orderedPerDuration = new Ordering<Map.Entry<InetAddress, Collection<TraceEvent>>>()
        {
            public int compare(Map.Entry<InetAddress, Collection<TraceEvent>> entry1,
                    Map.Entry<InetAddress, Collection<TraceEvent>> entry2)
            {
                List<TraceEvent> entry1asList = ((List<TraceEvent>) entry1.getValue());
                List<TraceEvent> entry2asList = ((List<TraceEvent>) entry2.getValue());
                return ComparisonChain
                        .start()
                        .compare(entry1asList.get(entry1asList.size() - 1).duration(),
                                entry2asList.get(entry2asList.size() - 1).duration()).result();
            }
        }.sortedCopy(eventsPerNode.asMap().entrySet());

        for (Map.Entry<InetAddress, Collection<TraceEvent>> entries : orderedPerDuration)
        {
            for (TraceEvent event : entries.getValue())
            {
                System.out.println(event);
            }
        }

    }

    public void printMultiSessionTraceForRequestType()
    {
        // print latencies avg, stddev, 95% and 99%

        // print the top 5 latencies (to enable individual tracing)

    }
}
