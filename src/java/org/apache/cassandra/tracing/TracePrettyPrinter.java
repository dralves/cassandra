package org.apache.cassandra.tracing;

import java.io.PrintStream;
import java.net.InetAddress;
import java.util.*;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.thrift.ConsistencyLevel;

import com.google.common.base.Function;
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
            int consistencyLevel = first.getFromPayload("consistency_level");
            cl = ConsistencyLevel.findByValue(consistencyLevel);
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

        Map<InetAddress, Collection<TraceEvent>> eventsPerNodeAsMap = eventsPerNode.asMap();

        for (TraceEvent event : events)
        {
            last = event;
            eventsPerNode.put(event.source(), event);
        }

        long totalDuration = last.duration();

        out.println();
        out.println("Session Summary: " + sessionId);
        out.println("Total interacting nodes: " + eventsPerNodeAsMap.size() + " {" + eventsPerNodeAsMap.keySet() + "}");
        out.println("Total duration: " + totalDuration + " nano sec");
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

        out.println("Request Timeline:");
        for (Map.Entry<InetAddress, Collection<TraceEvent>> entries : orderedPerDuration)
        {

        }
    }

    private static final String LINE = "|----------------------------------------------------------------------------" +
            "-----------------------|";

    public static void printMultiSessionTraceForRequestType(String requestName, Map<UUID, List<TraceEvent>> sessions,
            PrintStream out)
    {

        DescriptiveStatistics latencySstats = new DescriptiveStatistics();
        int totalEvents = 0;
        UUID minSessionId = null;
        UUID maxSessionId = null;
        long min = Long.MAX_VALUE;
        long max = 0;
        for (List<TraceEvent> events : sessions.values())
        {
            TraceEvent first = events.get(0);
            TraceEvent last = events.get(events.size() - 1);
            long duration = TimeUnit.MILLISECONDS.convert(last.duration() - first.duration(),
                    TimeUnit.NANOSECONDS);
            if (duration < min)
            {
                minSessionId = first.sessionId();
                min = duration;
            }
            if (duration > max)
            {
                maxSessionId = first.sessionId();
                max = duration;
            }
            latencySstats.addValue(duration);
            totalEvents += events.size();

        }

        out.println("Summary for sessions of request: " + requestName);
        out.println("Total Sessions: " + sessions.values().size());
        out.println("Total Events: " + totalEvents);
        out.println("                       ==============================================================================");
        out.println("                       |    Avg.    |   StdDev.  |   Max.     |    Min.    |     99%   |     Unit   |");
        out.println("=====================================================================================================");
        printStatsLine(out, "Latency", latencySstats, "msec");
        printRequestSpecificInfo(requestName, sessions, out);
        out.println();
        out.println("Quickest Request sessionId: " + minSessionId);
        out.println("Slowest  Request sessionId: " + maxSessionId);
        out.println();
    }

    private static void printRequestSpecificInfo(String requestName, Map<UUID, List<TraceEvent>> traceEvents,
            PrintStream out)
    {
        if (requestName.equals("batch_mutate"))
            printBatchMutateInfo(traceEvents, out);
    }

    private static void printStatsLine(PrintStream out, String name, DescriptiveStatistics stats, String unit)
    {
        out.println("| " + formatString(name) + " | " + formatNumber(stats.getMean()) + " | "
                + formatNumber(stats.getStandardDeviation()) + " | " + formatNumber(stats.getMax()) + " | "
                + formatNumber(stats.getMin()) + " | " + formatNumber(stats.getPercentile(99.0)) + "| "
                + String.format("%10s", unit) + " |");
        out.println(LINE);
    }

    private static void printBatchMutateInfo(Map<UUID, List<TraceEvent>> traceEvents, PrintStream out)
    {
        final DescriptiveStatistics keysStats = new DescriptiveStatistics();
        final DescriptiveStatistics mutationStats = new DescriptiveStatistics();
        final DescriptiveStatistics deletionStats = new DescriptiveStatistics();
        final DescriptiveStatistics columnStats = new DescriptiveStatistics();
        final DescriptiveStatistics counterStats = new DescriptiveStatistics();
        final DescriptiveStatistics superColumnStats = new DescriptiveStatistics();
        final DescriptiveStatistics writtenSizeStats = new DescriptiveStatistics();
        applyToAll(traceEvents, new Function<TraceEvent, Void>()
        {

            @Override
            public Void apply(TraceEvent event)
            {
                if (event.type() == TraceEvent.Type.SESSION_START)
                {
                    keysStats.addValue((Integer) event.getFromPayload("total_keys"));
                    mutationStats.addValue((Integer) event.getFromPayload("total_mutations"));
                    deletionStats.addValue((Integer) event.getFromPayload("total_deletions"));
                    columnStats.addValue((Integer) event.getFromPayload("total_columns"));
                    counterStats.addValue((Integer) event.getFromPayload("total_counters"));
                    superColumnStats.addValue((Integer) event.getFromPayload("total_super_columns"));
                    writtenSizeStats.addValue((Long) event.getFromPayload("total_written_size"));
                }
                return null;
            }
        });
        printStatsLine(out, "Batch Rows", keysStats, "amount/req");
        printStatsLine(out, "Mutations", mutationStats, "amount/req");
        printStatsLine(out, "Deletions", deletionStats, "amount/req");
        printStatsLine(out, "Columns", columnStats, "amount/req");
        printStatsLine(out, "Counters", counterStats, "amount/req");
        printStatsLine(out, "Super Col.", superColumnStats, "amount/req");
        printStatsLine(out, "Written Bytes", writtenSizeStats, "amount/req");
    }

    private static String formatNumber(double number)
    {
        return String.format("%10.2f", number);
    }

    private static String formatString(String name)
    {
        return String.format("%-20s", name);
    }

    private static void applyToAll(Map<UUID, List<TraceEvent>> traceEvents, Function<TraceEvent, ?> function)
    {
        Iterable<TraceEvent> all = Iterables.concat(traceEvents.values());
        for (TraceEvent event : all)
        {
            function.apply(event);
        }
    }
}
