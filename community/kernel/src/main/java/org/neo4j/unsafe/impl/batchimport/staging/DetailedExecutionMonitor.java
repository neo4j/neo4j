/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.helpers.Format.duration;

import java.io.PrintStream;

import org.neo4j.unsafe.impl.batchimport.stats.StepStats;

/**
 * An {@link ExecutionMonitor} that prints very detailed information about each {@link Stage} and the
 * {@link Step steps} therein.
 */
public class DetailedExecutionMonitor extends PollingExecutionMonitor
{
    private final PrintStream out;

    public DetailedExecutionMonitor( PrintStream out )
    {
        super( SECONDS.toMillis( 2 ) );
        this.out = out;
    }

    public DetailedExecutionMonitor()
    {
        this( System.out );
    }

    @Override
    protected void start( StageExecution execution )
    {
        out.println( format( "%n>>>>> EXECUTING STAGE %s <<<<<%n", execution.getStageName() ) );
    }

    @Override
    protected void end( StageExecution execution, long totalTimeMillis )
    {
        out.println( "Stage total time " + duration( totalTimeMillis ) );
    }

    @Override
    protected void poll( StageExecution execution )
    {
        printStats( execution );
    }

    @Override
    public void done()
    {
        out.println( "IMPORT DONE" );
    }

    private void printStats( StageExecution execution )
    {
        StringBuilder builder = new StringBuilder();
        int i = 0;
        for ( StepStats stats : execution.stats() )
        {
            builder.append( i > 0 ? format( "%n" ) : "" )
                   .append( stats.toString() )
                   ;
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
