/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.unsafe.impl.batchimport.staging;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongFunction;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.Format;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.io.os.OsBeanUtil;
import org.neo4j.kernel.monitoring.VmPauseMonitor;
import org.neo4j.kernel.monitoring.VmPauseMonitor.VmPauseInfo;
import org.neo4j.logging.NullLog;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.Long.max;
import static java.lang.System.currentTimeMillis;
import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.helpers.Format.date;
import static org.neo4j.helpers.Format.duration;

/**
 * Sits in the background and collect stats about stages that are executing.
 * Reacts to console input from {@link System#in} and on certain commands print various things.
 * Commands (complete all with ENTER):
 * <p>
 * <table border="1">
 *   <tr>
 *     <th>Command</th>
 *     <th>Description</th>
 *   </tr>
 *   <tr>
 *     <th>i</th>
 *     <th>Print {@link SpectrumExecutionMonitor compact information} about each executed stage up to this point</th>
 *   </tr>
 * </table>
 */
public class OnDemandDetailsExecutionMonitor implements ExecutionMonitor
{
    interface Monitor
    {
        void detailsPrinted();
    }

    private final List<StageDetails> details = new ArrayList<>();
    private final PrintStream out;
    private final InputStream in;
    private final Map<String,Pair<String,Runnable>> actions = new HashMap<>();
    private final VmPauseTimeAccumulator vmPauseTimeAccumulator = new VmPauseTimeAccumulator();
    private final VmPauseMonitor vmPauseMonitor;
    private final Monitor monitor;

    private StageDetails current;
    private boolean printDetailsOnDone;

    public OnDemandDetailsExecutionMonitor( PrintStream out, InputStream in, Monitor monitor, JobScheduler jobScheduler )
    {
        this.out = out;
        this.in = in;
        this.monitor = monitor;
        this.actions.put( "i", Pair.of( "Print more detailed information", this::printDetails ) );
        this.actions.put( "c", Pair.of( "Print more detailed information about current stage", this::printDetailsForCurrentStage ) );
        this.vmPauseMonitor = new VmPauseMonitor( Duration.ofMillis( 100 ), Duration.ofMillis( 100 ),
                NullLog.getInstance(), jobScheduler, vmPauseTimeAccumulator );
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        out.println( "InteractiveReporterInteractions command list (end with ENTER):" );
        actions.forEach( ( key, action ) -> out.println( "  " + key + ": " + action.first() ) );
        out.println();
        vmPauseMonitor.start();
    }

    @Override
    public void start( StageExecution execution )
    {
        details.add( current = new StageDetails( execution, vmPauseTimeAccumulator ) );
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        current.collect();
    }

    @Override
    public void done( boolean successful, long totalTimeMillis, String additionalInformation )
    {
        if ( printDetailsOnDone )
        {
            printDetails();
        }
        vmPauseMonitor.stop();
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 500;
    }

    @Override
    public void check( StageExecution execution )
    {
        current.collect();
        reactToUserInput();
    }

    private void printDetails()
    {
        printDetailsHeadline();
        long totalTime = 0;
        for ( StageDetails stageDetails : details )
        {
            stageDetails.print( out );
            totalTime += stageDetails.totalTimeMillis;
        }

        printIndented( out, "Environment information:" );
        printIndented( out, "  Free physical memory: " + bytes( OsBeanUtil.getFreePhysicalMemory() ) );
        printIndented( out, "  Max VM memory: " + bytes( Runtime.getRuntime().maxMemory() ) );
        printIndented( out, "  Free VM memory: " + bytes( Runtime.getRuntime().freeMemory() ) );
        printIndented( out, "  VM stop-the-world time: " + duration( vmPauseTimeAccumulator.getPauseTime() ) );
        printIndented( out, "  Duration: " + duration( totalTime ) );
        out.println();
    }

    private void printDetailsHeadline()
    {
        out.println();
        out.println();
        printIndented( out, "******** DETAILS " + date() + " ********" );
        out.println();

        // Make sure that if user is interested in details then also print the entire details set when import is done
        printDetailsOnDone = true;
        monitor.detailsPrinted();
    }

    private void printDetailsForCurrentStage()
    {
        printDetailsHeadline();
        if ( !details.isEmpty() )
        {
            details.get( details.size() - 1 ).print( out );
        }
    }

    private static void printIndented( PrintStream out, String string )
    {
        out.println( "\t" + string );
    }

    private void reactToUserInput()
    {
        try
        {
            if ( in.available() > 0 )
            {
                // don't close this read, since we really don't want to close the underlying System.in
                BufferedReader reader = new BufferedReader( new InputStreamReader( System.in ) );
                String line = reader.readLine();
                Pair<String,Runnable> action = actions.get( line );
                if ( action != null )
                {
                    action.other().run();
                }
            }
        }
        catch ( IOException e )
        {
            e.printStackTrace( out );
        }
    }

    private static class StageDetails
    {
        private final StageExecution execution;
        private final long startTime;
        private final VmPauseTimeAccumulator vmPauseTimeAccumulator;
        private final long baseVmPauseTime;

        private long memoryUsage;
        private long ioThroughput;
        private long totalTimeMillis;
        private long stageVmPauseTime;
        private long doneBatches;

        StageDetails( StageExecution execution, VmPauseTimeAccumulator vmPauseTimeAccumulator )
        {
            this.execution = execution;
            this.vmPauseTimeAccumulator = vmPauseTimeAccumulator;
            this.baseVmPauseTime = vmPauseTimeAccumulator.getPauseTime();
            this.startTime = currentTimeMillis();
        }

        void print( PrintStream out )
        {
            printIndented( out, execution.name() );
            StringBuilder builder = new StringBuilder();
            SpectrumExecutionMonitor.printSpectrum( builder, execution, SpectrumExecutionMonitor.DEFAULT_WIDTH, DetailLevel.NO );
            printIndented( out, builder.toString() );
            printValue( out, memoryUsage, "Memory usage", Format::bytes );
            printValue( out, ioThroughput, "I/O throughput", value -> bytes( value ) + "/s" );
            printValue( out, stageVmPauseTime, "VM stop-the-world time", Format::duration );
            printValue( out, totalTimeMillis, "Duration", Format::duration );
            printValue( out, doneBatches, "Done batches", String::valueOf );

            out.println();
        }

        private static void printValue( PrintStream out, long value, String description, LongFunction<String> toStringConverter )
        {
            if ( value > 0 )
            {
                printIndented( out, description + ": " + toStringConverter.apply( value ) );
            }
        }

        void collect()
        {
            totalTimeMillis = currentTimeMillis() - startTime;
            stageVmPauseTime = vmPauseTimeAccumulator.getPauseTime() - baseVmPauseTime;
            long lastDoneBatches = doneBatches;
            for ( Step<?> step : execution.steps() )
            {
                StepStats stats = step.stats();
                Stat memoryUsageStat = stats.stat( Keys.memory_usage );
                if ( memoryUsageStat != null )
                {
                    memoryUsage = max( memoryUsage, memoryUsageStat.asLong() );
                }
                Stat ioStat = stats.stat( Keys.io_throughput );
                if ( ioStat != null )
                {
                    ioThroughput = ioStat.asLong();
                }
                lastDoneBatches = stats.stat( Keys.done_batches ).asLong();
            }
            doneBatches = lastDoneBatches;
        }
    }

    private static class VmPauseTimeAccumulator implements Consumer<VmPauseInfo>
    {
        private final AtomicLong totalPauseTime = new AtomicLong();

        @Override
        public void accept( VmPauseInfo pauseInfo )
        {
            totalPauseTime.addAndGet( pauseInfo.getPauseTime() );
        }

        long getPauseTime()
        {
            return totalPauseTime.get();
        }
    }
}
