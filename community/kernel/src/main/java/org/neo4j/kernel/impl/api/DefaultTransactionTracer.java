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
package org.neo4j.kernel.impl.api;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;
import org.neo4j.time.SystemNanoClock;

public class DefaultTransactionTracer implements TransactionTracer, LogRotationMonitor
{
    public interface Monitor
    {
        void lastLogRotationEventDuration( long millis );
    }

    private final SystemNanoClock clock;
    private final Monitor monitor;
    private final JobScheduler jobScheduler;

    private final AtomicLong counter = new AtomicLong();
    private final AtomicLong accumulatedTotalTimeNanos = new AtomicLong();

    private long startTimeNanos;

    private final LogRotateEvent logRotateEvent = this::updateCountersAndNotifyListeners;

    private final LogAppendEvent logAppendEvent = new LogAppendEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public void setLogRotated( boolean logRotated )
        {

        }

        @Override
        public LogRotateEvent beginLogRotate()
        {
            startTimeNanos = clock.nanos();
            return logRotateEvent;
        }

        @Override
        public SerializeTransactionEvent beginSerializeTransaction()
        {
            return SerializeTransactionEvent.NULL;
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

    private final CommitEvent commitEvent = new CommitEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public LogAppendEvent beginLogAppend()
        {
            return logAppendEvent;
        }

        @Override
        public StoreApplyEvent beginStoreApply()
        {
            return StoreApplyEvent.NULL;
        }
    };

    private final TransactionEvent transactionEvent = new TransactionEvent()
    {

        @Override
        public void setSuccess( boolean success )
        {
        }

        @Override
        public void setFailure( boolean failure )
        {
        }

        @Override
        public CommitEvent beginCommitEvent()
        {
            return commitEvent;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void setTransactionWriteState( String transactionWriteState )
        {
        }

        @Override
        public void setReadOnly( boolean wasReadOnly )
        {
        }
    };

    public DefaultTransactionTracer( Monitor monitor, JobScheduler jobScheduler )
    {
        this( Clocks.nanoClock(), monitor, jobScheduler );
    }

    public DefaultTransactionTracer( SystemNanoClock clock, Monitor monitor, JobScheduler jobScheduler )
    {
        this.clock = clock;
        this.monitor = monitor;
        this.jobScheduler = jobScheduler;
    }

    @Override
    public TransactionEvent beginTransaction()
    {
        return transactionEvent;
    }

    @Override
    public long numberOfLogRotationEvents()
    {
        return counter.get();
    }

    @Override
    public long logRotationAccumulatedTotalTimeMillis()
    {
        return TimeUnit.NANOSECONDS.toMillis( accumulatedTotalTimeNanos.get() );
    }

    private void updateCountersAndNotifyListeners()
    {
        counter.incrementAndGet();
        long lastEventTime = clock.nanos() - startTimeNanos;
        accumulatedTotalTimeNanos.addAndGet( lastEventTime );
        jobScheduler.schedule( JobScheduler.Groups.metricsEvent, () ->
        {
            long millis = TimeUnit.NANOSECONDS.toMillis( lastEventTime );
            monitor.lastLogRotationEventDuration( millis );
        } );
    }
}
