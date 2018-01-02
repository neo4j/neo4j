/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.PrintStream;

import org.neo4j.unsafe.impl.batchimport.stats.DetailLevel;
import org.neo4j.unsafe.impl.batchimport.stats.Keys;
import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;

import static org.neo4j.helpers.Format.duration;

/**
 * An {@link ExecutionMonitor} that prints very detailed information about each {@link Stage} and the
 * {@link Step steps} therein.
 */
public class DetailedExecutionMonitor extends ExecutionMonitor.Adapter
{
    private final PrintStream out;

    public DetailedExecutionMonitor( PrintStream out )
    {
        this( out, 5 );
    }

    public DetailedExecutionMonitor( PrintStream out, long intervalSeconds )
    {
        super( intervalSeconds, SECONDS );
        this.out = out;
    }

    @Override
    public void start( StageExecution[] executions )
    {
        StringBuilder names = new StringBuilder();
        for ( StageExecution execution : executions )
        {
            names.append( names.length() > 0 ? ", " : "" ).append( execution.getStageName() );
        }
        out.println( format( "%n>>>>> EXECUTING STAGE(s) %s <<<<<%n", names ) );
    }

    @Override
    public void end( StageExecution[] executions, long totalTimeMillis )
    {
        out.println( "Stage total time " + duration( totalTimeMillis ) );
    }

    @Override
    public void check( StageExecution[] executions )
    {
        boolean first = true;
        for ( StageExecution execution : executions )
        {
            printStats( execution, first );
            first = false;
        }
    }

    private void printStats( StageExecution execution, boolean first )
    {
        Step<?> bottleNeck = execution.stepsOrderedBy( Keys.avg_processing_time, false ).iterator().next().first();

        StringBuilder builder = new StringBuilder();
        int i = 0;
        for ( Step<?> step : execution.steps() )
        {
            StepStats stats = step.stats();
            builder.append( i > 0 ? format( "%n  " ) : (first ? "--" : " -") )
                   .append( stats.toString( DetailLevel.BASIC ) )
                   .append( step == bottleNeck ? "  <== BOTTLE NECK" : "" );
            i++;
        }

        String toPrint = builder.toString();
        printAndBackUpAgain( toPrint );
    }

    private void printAndBackUpAgain( String toPrint )
    {
        out.println( toPrint );
    }
}
