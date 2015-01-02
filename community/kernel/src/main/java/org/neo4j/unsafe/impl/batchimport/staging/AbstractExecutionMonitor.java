/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;

abstract class AbstractExecutionMonitor implements ExecutionMonitor
{
    private final Clock clock;
    private final long intervalMillis;

    protected AbstractExecutionMonitor( Clock clock, long time, TimeUnit unit )
    {
        this.clock = clock;
        this.intervalMillis = unit.toMillis( time );
    }

    protected AbstractExecutionMonitor( long time, TimeUnit unit )
    {
        this( Clock.SYSTEM_CLOCK, time, unit );
    }

    @Override
    public long nextCheckTime()
    {
        return clock.currentTimeMillis() + intervalMillis;
    }

    @Override
    public void start( StageExecution[] executions )
    {   // Do nothing by default
    }

    @Override
    public void end( StageExecution[] executions, long totalTimeMillis )
    {   // Do nothing by default
    }

    @Override
    public void done( long totalTimeMillis )
    {   // Do nothing by default
    }
}
