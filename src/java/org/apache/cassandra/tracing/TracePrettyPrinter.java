package org.apache.cassandra.tracing;

import java.io.PrintStream;
import java.net.InetAddress;
import java.text.DecimalFormat;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.common.base.Supplier;
import com.google.common.collect.*;

import org.apache.commons.math.stat.descriptive.DescriptiveStatistics;


public class TracePrettyPrinter
{

    public static void printSingleSessionTrace(UUID sessionId, List<TraceEvent> events, PrintStream out)
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

    public static void printMultiSessionTraceForRequestType(String requestName, Map<UUID, List<TraceEvent>> sessions,
            PrintStream out)
    {

        DescriptiveStatistics latencySstats = new DescriptiveStatistics();
        for (List<TraceEvent> events : sessions.values())
        {
            TraceEvent first = events.get(0);
            TraceEvent last = events.get(events.size() - 1);
            latencySstats.addValue(last.duration() - first.duration());
        }

        out.println("Summary for sessions of request: " + requestName);
        out.println("Total Sessions: " + sessions.values().size());
        out.println("            ==============================================================");
        out.println("            |    Avg.    |   StdDev.  |   Max.   |    Min.   |     99%   |");
        out.println("==========================================================================");
        out.println("|   Lat.    | " + nanosToFormattedMillis(latencySstats.getMean()) + " | "
                + nanosToFormattedMillis(latencySstats.getStandardDeviation()) + " | "
                + nanosToFormattedMillis(latencySstats.getMax()) + " | "
                + nanosToFormattedMillis(latencySstats.getMin()) + " | "
                + nanosToFormattedMillis(latencySstats.getPercentile(99))
                + " | ");
        // print the top 5 latencies (to enable individual tracing)

    }

    private static DecimalFormat format = new DecimalFormat("##.###");

    private static String nanosToFormattedMillis(double value)
    {
        long val = Math.round(value);
        return format.format(TimeUnit.MILLISECONDS.convert(val, TimeUnit.NANOSECONDS));
    }
}
