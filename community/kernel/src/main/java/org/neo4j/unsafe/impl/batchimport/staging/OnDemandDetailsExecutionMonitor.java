/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.Stat;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.Long.max;
import static java.lang.String.format;
import static java.lang.System.currentTimeMillis;

import static org.neo4j.helpers.Format.bytes;

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
    private final List<StageDetails> details = new ArrayList<>();
    private final PrintStream out;
    private final Map<String,Pair<String,Runnable>> actions = new HashMap<>();
    private StageDetails current;
    private boolean printDetailsOnDone;

    public OnDemandDetailsExecutionMonitor( PrintStream out )
    {
        this.out = out;
        this.actions.put( "i", Pair.of( "Print more detailed information", this::printDetails ) );
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        out.println( "Interactive command list (end with ENTER):" );
        actions.forEach( ( key, action ) -> out.println( "  " + key + ": " + action.first() ) );
        out.println();
    }

    @Override
    public void start( StageExecution execution )
    {
        details.add( current = new StageDetails( execution ) );
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        current.collect();
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        if ( printDetailsOnDone )
        {
            printDetails();
        }
    }

    @Override
    public long nextCheckTime()
    {
        return currentTimeMillis() + 200;
    }

    @Override
    public void check( StageExecution execution )
    {
        current.collect();
        reactToUserInput();
    }

    private void printDetails()
    {
        out.println( format( "%n************** DETAILS **************%n" ) );
        for ( StageDetails stageDetails : details )
        {
            stageDetails.print( out );
        }

        // Make sure that if user is interested in details then also print the entire details set when import is done
        printDetailsOnDone = true;
    }

    private void reactToUserInput()
    {
        try
        {
            if ( System.in.available() > 0 )
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
        private long memoryUsage;
        private long ioThroughput;

        StageDetails( StageExecution execution )
        {
            this.execution = execution;
        }

        void print( PrintStream out )
        {
            out.println( execution.name() );
            StringBuilder builder = new StringBuilder();
            SpectrumExecutionMonitor.printSpectrum( builder, execution, SpectrumExecutionMonitor.DEFAULT_WIDTH, DetailLevel.NO );
            out.println( builder.toString() );
            if ( memoryUsage > 0 )
            {
                out.println( "Memory usage: " + bytes( memoryUsage ) );
            }
            if ( ioThroughput > 0 )
            {
                out.println( "I/O throughput: " + bytes( ioThroughput ) + "/s" );
            }

            out.println();
        }

        void collect()
        {
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
            }
        }
    }
}
