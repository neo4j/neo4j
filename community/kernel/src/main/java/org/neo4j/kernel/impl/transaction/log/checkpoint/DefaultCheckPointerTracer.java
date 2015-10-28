/*
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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.concurrent.TimeUnit;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;

public class DefaultCheckPointerTracer implements CheckPointTracer, CheckPointerMonitor
{
    private final Clock clock;

    private volatile long startTimeNanos;
    private volatile long endTimeNanos;
    private volatile long totalTimeNanos;

    private LogCheckPointEvent logCheckPointEvent = new LogCheckPointEvent()
    {
        @Override
        public void close()
        {
            endTimeNanos = clock.nanoTime();
            totalTimeNanos = endTimeNanos - startTimeNanos;
        }

        @Override
        public LogForceWaitEvent beginLogForceWait()
        {

            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            return LogForceEvent.NULL;
        }
    };

    public DefaultCheckPointerTracer()
    {
        this( Clock.SYSTEM_CLOCK );
    }

    DefaultCheckPointerTracer( Clock clock )
    {
        this.clock = clock;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        startTimeNanos = clock.nanoTime();
        return logCheckPointEvent;
    }

    @Override
    public long lastCheckPointStartTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( startTimeNanos );
    }

    @Override
    public long lastCheckPointEndTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( endTimeNanos );
    }

    @Override
    public long lastCheckPointTotalTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( totalTimeNanos );
    }
}
