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

/**
 * {@link ExecutionMonitor} that wraps several other monitors. Each wrapper monitor can still specify
 * individual poll frequencies and this {@link MultiExecutionMonitor} will make that happen.
 */
public class MultiExecutionMonitor implements ExecutionMonitor
{
    private final Clock clock;
    private final ExecutionMonitor[] monitors;
    private final long[] endTimes;

    public MultiExecutionMonitor( ExecutionMonitor... monitors )
    {
        this( Clock.SYSTEM_CLOCK, monitors );
    }

    public MultiExecutionMonitor( Clock clock, ExecutionMonitor... monitors )
    {
        this.clock = clock;
        this.monitors = monitors;
        this.endTimes = new long[monitors.length];
        fillEndTimes();
    }

    @Override
    public void start( StageExecution[] executions )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.start( executions );
        }
    }

    @Override
    public void end( StageExecution[] executions, long totalTimeMillis )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.end( executions, totalTimeMillis );
        }
    }

    @Override
    public void done( long totalTimeMillis, String additionalInformation )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.done( totalTimeMillis, additionalInformation );
        }
    }

    @Override
    public long nextCheckTime()
    {
        // Find the lowest of all end times
        long low = endTimes[0];
        for ( int i = 1; i < monitors.length; i++ )
        {
            long thisLow = endTimes[i];
            if ( thisLow < low )
            {
                low = thisLow;
            }
        }
        return low;
    }

    private void fillEndTimes()
    {
        for ( int i = 0; i < monitors.length; i++ )
        {
            endTimes[i] = monitors[i].nextCheckTime();
        }
    }

    @Override
    public void check( StageExecution[] executions )
    {
        long currentTimeMillis = clock.currentTimeMillis();
        for ( int i = 0; i < monitors.length; i++ )
        {
            if ( currentTimeMillis >= endTimes[i] )
            {
                monitors[i].check( executions );
                endTimes[i] = monitors[i].nextCheckTime();
            }
        }
    }
}
