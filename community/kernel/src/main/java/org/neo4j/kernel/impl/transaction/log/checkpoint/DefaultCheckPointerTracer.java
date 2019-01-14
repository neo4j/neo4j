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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public class DefaultCheckPointerTracer implements CheckPointTracer, CheckPointerMonitor
{
    public interface Monitor
    {
        void lastCheckPointEventDuration( long millis );
    }

    private final SystemNanoClock clock;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;

    private final AtomicLong counter = new AtomicLong();
    private final AtomicLong accumulatedTotalTimeNanos = new AtomicLong();

    private volatile long startTimeNanos;

    private LogCheckPointEvent logCheckPointEvent = new LogCheckPointEvent()
    {
        @Override
        public void close()
        {
            updateCountersAndNotifyListeners();
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

    public DefaultCheckPointerTracer( Monitor monitor, JobScheduler jobScheduler )
    {
        this( Clocks.nanoClock(), monitor, jobScheduler );
    }

    public DefaultCheckPointerTracer( SystemNanoClock clock, Monitor monitor, JobScheduler jobScheduler )
    {
        this.clock = clock;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        startTimeNanos = clock.nanos();
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

    private void updateCountersAndNotifyListeners()
    {
        final long lastEventTime = clock.nanos() - startTimeNanos;

        // update counters
        counter.incrementAndGet();
        accumulatedTotalTimeNanos.addAndGet( lastEventTime );

        // notify async
        jobScheduler.schedule( JobScheduler.Groups.metricsEvent, () ->
        {
            long millis = TimeUnit.NANOSECONDS.toMillis( lastEventTime );
            monitor.lastCheckPointEventDuration( millis );
        } );
    }
}
