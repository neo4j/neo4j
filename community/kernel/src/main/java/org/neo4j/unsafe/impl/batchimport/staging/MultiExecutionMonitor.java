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

import java.time.Clock;

import org.neo4j.graphdb.DependencyResolver;
import org.neo4j.time.Clocks;

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
        this( Clocks.systemClock(), monitors );
    }

    public MultiExecutionMonitor( Clock clock, ExecutionMonitor... monitors )
    {
        this.clock = clock;
        this.monitors = monitors;
        this.endTimes = new long[monitors.length];
        fillEndTimes();
    }

    @Override
    public void initialize( DependencyResolver dependencyResolver )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.initialize( dependencyResolver );
        }
    }

    @Override
    public void start( StageExecution execution )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.start( execution );
        }
    }

    @Override
    public void end( StageExecution execution, long totalTimeMillis )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.end( execution, totalTimeMillis );
        }
    }

    @Override
    public void done( boolean successful, long totalTimeMillis, String additionalInformation )
    {
        for ( ExecutionMonitor monitor : monitors )
        {
            monitor.done( successful, totalTimeMillis, additionalInformation );
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
    public void check( StageExecution execution )
    {
        long currentTimeMillis = clock.millis();
        for ( int i = 0; i < monitors.length; i++ )
        {
            if ( currentTimeMillis >= endTimes[i] )
            {
                monitors[i].check( execution );
                endTimes[i] = monitors[i].nextCheckTime();
            }
        }
    }
}
