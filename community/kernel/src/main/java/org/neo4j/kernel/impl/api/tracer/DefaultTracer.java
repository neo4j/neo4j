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
package org.neo4j.kernel.impl.api.tracer;

import java.util.concurrent.atomic.AtomicLong;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.tracing.CommitEvent;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogFileCreateEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;
import org.neo4j.kernel.impl.transaction.tracing.SerializeTransactionEvent;
import org.neo4j.kernel.impl.transaction.tracing.StoreApplyEvent;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;

import static org.neo4j.kernel.impl.transaction.log.entry.LogHeader.LOG_HEADER_SIZE;

/**
 * Tracer used to trace database scoped events, like transaction logs rotations, checkpoints, transactions etc
 */
public class DefaultTracer implements DatabaseTracer
{
    private final AtomicLong appendedBytes = new AtomicLong();

    private final CountingLogRotateEvent countingLogRotateEvent = new CountingLogRotateEvent();
    private final LogFileCreateEvent logFileCreateEvent = () -> appendedBytes.addAndGet( LOG_HEADER_SIZE );
    private final CountingLogCheckPointEvent logCheckPointEvent = new CountingLogCheckPointEvent( this::appendLogBytes );
    private final LogAppendEvent logAppendEvent = new DefaultLogAppendEvent();
    private final CommitEvent commitEvent = new DefaultCommitEvent();
    private final TransactionEvent transactionEvent = new DefaultTransactionEvent();

    public DefaultTracer()
    {
        //empty
    }

    @Override
    public TransactionEvent beginTransaction()
    {
        return transactionEvent;
    }

    @Override
    public long appendedBytes()
    {
        return appendedBytes.get();
    }

    @Override
    public long numberOfLogRotations()
    {
        return countingLogRotateEvent.numberOfLogRotations();
    }

    @Override
    public long logRotationAccumulatedTotalTimeMillis()
    {
        return countingLogRotateEvent.logRotationAccumulatedTotalTimeMillis();
    }

    @Override
    public long lastLogRotationTimeMillis()
    {
        return countingLogRotateEvent.lastLogRotationTimeMillis();
    }

    @Override
    public long numberOfCheckPoints()
    {
        return logCheckPointEvent.numberOfCheckPoints();
    }

    @Override
    public long checkPointAccumulatedTotalTimeMillis()
    {
        return logCheckPointEvent.checkPointAccumulatedTotalTimeMillis();
    }

    @Override
    public long lastCheckpointTimeMillis()
    {
        return logCheckPointEvent.lastCheckpointTimeMillis();
    }

    @Override
    public LogCheckPointEvent beginCheckPoint()
    {
        return logCheckPointEvent;
    }

    private void appendLogBytes( LogPosition logPositionBeforeAppend, LogPosition logPositionAfterAppend )
    {
        if ( logPositionAfterAppend.getLogVersion() != logPositionBeforeAppend.getLogVersion() )
        {
            throw new IllegalStateException( "Appending to several log files is not supported." );
        }
        appendedBytes.addAndGet( logPositionAfterAppend.getByteOffset() - logPositionBeforeAppend.getByteOffset() );
    }

    @Override
    public LogFileCreateEvent createLogFile()
    {
        return logFileCreateEvent;
    }

    private class DefaultTransactionEvent implements TransactionEvent
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
    }

    private class DefaultCommitEvent implements CommitEvent
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
    }

    private class DefaultLogAppendEvent implements LogAppendEvent
    {
        @Override
        public void appendToLogFile( LogPosition logPositionBeforeAppend, LogPosition logPositionAfterAppend )
        {
            appendLogBytes( logPositionBeforeAppend, logPositionAfterAppend );
        }

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
            return countingLogRotateEvent;
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
    }
}
