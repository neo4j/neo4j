/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.stresstests.transaction.checkpoint.tracers;

import org.HdrHistogram.Histogram;

import java.io.PrintStream;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.impl.transaction.tracing.CheckPointTracer;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionTracer;

public class TimerTransactionTracer implements TransactionTracer, CheckPointTracer
{
    private static volatile long logForceBegin;
    private static volatile long logCheckPointBegin;
    private static volatile long logRotateBegin;
    private static final Histogram logForceTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );
    private static final Histogram logRotateTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );
    private static final Histogram logCheckPointTimes = new Histogram( 1000, TimeUnit.MINUTES.toNanos( 45 ), 0 );

    public static void printStats( PrintStream out )
    {
        printStat( out, "Log force millisecond percentiles:", logForceTimes );
        printStat( out, "Log rotate millisecond percentiles:", logRotateTimes );
        printStat( out, "Log check point millisecond percentiles:", logCheckPointTimes );
    }

    private static void printStat( PrintStream out, String message, Histogram histogram )
    {
        out.println( message );
        histogram.outputPercentileDistribution( out, 1000000.0 );
        out.println();
    }

    private static final LogForceEvent LOG_FORCE_EVENT = new LogForceEvent()
    {
        @Override
        public void close()
        {
            long elapsedNanos = System.nanoTime() - logForceBegin;
            logForceTimes.recordValue( elapsedNanos );
        }
    };

    private static final LogCheckPointEvent LOG_CHECK_POINT_EVENT = new LogCheckPointEvent()
    {
        @Override
        public LogForceWaitEvent beginLogForceWait()
        {
            return LogForceWaitEvent.NULL;
        }

        @Override
        public LogForceEvent beginLogForce()
        {
            logForceBegin = System.nanoTime();
            return LOG_FORCE_EVENT;
        }

        @Override
        public void close()
        {
            long elapsedNanos = System.nanoTime() - logCheckPointBegin;
            logCheckPointTimes.recordValue( elapsedNanos );
        }
    };

    private static final LogRotateEvent LOG_ROTATE_EVENT = new LogRotateEvent()
    {
        @Override
        public void close()
        {
            long elapsedNanos = System.nanoTime() - logRotateBegin;
            logRotateTimes.recordValue( elapsedNanos );
        }
    };

    private static final LogAppendEvent LOG_APPEND_EVENT = new LogAppendEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public void setLogRotated( boolean b )
        {
        }

        @Override
        public LogRotateEvent beginLogRotate()
        {
            logRotateBegin = System.nanoTime();
            return LOG_ROTATE_EVENT;
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
            logForceBegin = System.nanoTime();
            return LOG_FORCE_EVENT;
        }
    };

    private static final CommitEvent COMMIT_EVENT = new CommitEvent()
    {
        @Override
        public void close()
        {
        }

        @Override
        public LogAppendEvent beginLogAppend()
        {
            return LOG_APPEND_EVENT;
        }

        @Override
        public StoreApplyEvent beginStoreApply()
        {
            return StoreApplyEvent.NULL;
        }

        @Override
        public void setTransactionId( long l )
        {
        }
    };

    private static final TransactionEvent TRANSACTION_EVENT = new TransactionEvent()
    {
        @Override
        public void setSuccess( boolean b )
        {
        }

        @Override
        public void setFailure( boolean b )
        {
        }

        @Override
        public CommitEvent beginCommitEvent()
        {
            return COMMIT_EVENT;
        }

        @Override
        public void close()
        {
        }

        @Override
        public void setTransactionType( String s )
        {
        }

        @Override
        public void setReadOnly( boolean b )
        {
        }
    };

    @Override
    public TransactionEvent beginTransaction()
    {
        return TRANSACTION_EVENT;
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        logCheckPointBegin = System.nanoTime();
        return LOG_CHECK_POINT_EVENT;
    }
}
