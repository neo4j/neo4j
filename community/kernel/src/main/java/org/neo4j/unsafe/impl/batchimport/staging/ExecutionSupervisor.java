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

import org.neo4j.helpers.Clock;

import static java.lang.Math.max;
import static java.lang.Math.min;
import static java.lang.Thread.sleep;

/**
 * Supervises a {@link StageExecution} until it is no longer {@link StageExecution#stillExecuting() executing}.
 * Meanwhile it feeds information about the execution to an {@link ExecutionMonitor}.
 */
public class ExecutionSupervisor
{
    private final Clock clock;
    private final ExecutionMonitor monitor;

    public ExecutionSupervisor( Clock clock, ExecutionMonitor monitor )
    {
        this.clock = clock;
        this.monitor = monitor;
    }

    public ExecutionSupervisor( ExecutionMonitor monitor )
    {
        this( Clock.SYSTEM_CLOCK, monitor );
    }

    /**
     * Supervises {@link StageExecution}, provides continuous information to the {@link ExecutionMonitor}
     * and returns when the execution is done or an error occurs, in which case an exception is thrown.
     *
     * Made synchronized to ensure that only one set of executions take place at any given time
     * and also to make sure the calling thread goes through a memory barrier (useful both before and after execution).
     *
     * @param executions {@link StageExecution} instances to supervise simultaneously.
     */
    public synchronized void supervise( StageExecution... executions )
    {
        long startTime = currentTimeMillis();
        start( executions );

        while ( anyStillExecuting( executions ) )
        {
            finishAwareSleep( executions );
            monitor.check( executions );
        }
        end( executions, currentTimeMillis()-startTime );
    }

    private long currentTimeMillis()
    {
        return clock.currentTimeMillis();
    }

    private boolean anyStillExecuting( StageExecution[] executions )
    {
        for ( StageExecution execution : executions )
        {
            if ( execution.stillExecuting() )
            {
                return true;
            }
        }
        return false;
    }

    protected void end( StageExecution[] executions, long totalTimeMillis )
    {
        monitor.end( executions, totalTimeMillis );
    }

    protected void start( StageExecution[] executions )
    {
        monitor.start( executions );
    }

    private void finishAwareSleep( StageExecution[] executions )
    {
        long endTime = monitor.nextCheckTime();
        while ( currentTimeMillis() < endTime )
        {
            if ( !anyStillExecuting( executions ) )
            {
                break;
            }

            try
            {
                sleep( min( 10, max( 0, endTime-currentTimeMillis() ) ) );
            }
            catch ( InterruptedException e )
            {
                for ( StageExecution execution : executions )
                {
                    execution.panic( e );
                }
                break;
            }
        }
    }
}
