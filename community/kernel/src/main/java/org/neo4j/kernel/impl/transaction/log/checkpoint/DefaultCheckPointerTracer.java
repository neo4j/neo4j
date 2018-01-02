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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.helpers.Clock;
import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;

public class DefaultCheckPointerTracer implements CheckPointTracer, CheckPointerMonitor
{
    private final Clock clock;
    private final AtomicLong counter = new AtomicLong();
    private final AtomicLong accumulatedTotalTimeNanos = new AtomicLong();

    private volatile long startTimeNanos;

    private LogCheckPointEvent logCheckPointEvent = new LogCheckPointEvent()
    {
        @Override
        public void close()
        {
            accumulatedTotalTimeNanos.addAndGet( clock.nanoTime() - startTimeNanos );
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
        counter.incrementAndGet();
        return logCheckPointEvent;
    }

    @Override
    public long numberOfCheckPointEvents()
    {
        return counter.get();
    }

    @Override
    public long checkPointAccumulatedTotalTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( accumulatedTotalTimeNanos.get() );
    }
}
