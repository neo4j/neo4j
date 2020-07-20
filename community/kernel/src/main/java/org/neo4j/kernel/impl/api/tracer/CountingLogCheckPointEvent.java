/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import java.util.function.BiConsumer;

import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogRotateEvent;

/**
 * Log checkpoint event that counts number of checkpoint that occurred and amount of time elapsed
 * for all of them and for the last one.
 */
class CountingLogCheckPointEvent implements LogCheckPointEvent
{
    private final AtomicLong checkpointCounter = new AtomicLong();
    private final AtomicLong accumulatedCheckpointTotalTimeMillis = new AtomicLong();
    private final BiConsumer<LogPosition,LogPosition> logFileAppendConsumer;
    private final CountingLogRotateEvent countingLogRotateEvent;
    private volatile long lastCheckpointTimeMillis;

    CountingLogCheckPointEvent( BiConsumer<LogPosition,LogPosition> logFileAppendConsumer, CountingLogRotateEvent countingLogRotateEvent )
    {
        this.logFileAppendConsumer = logFileAppendConsumer;
        this.countingLogRotateEvent = countingLogRotateEvent;
    }

    @Override
    public void checkpointCompleted( long checkpointMillis )
    {
        checkpointCounter.incrementAndGet();
        accumulatedCheckpointTotalTimeMillis.addAndGet( checkpointMillis );
        lastCheckpointTimeMillis = checkpointMillis;
    }

    @Override
    public void close()
    {
        //empty
    }

    @Override
    public void appendToLogFile( LogPosition positionBeforeCheckpoint, LogPosition positionAfterCheckpoint )
    {
        logFileAppendConsumer.accept( positionBeforeCheckpoint, positionAfterCheckpoint );
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

    long numberOfCheckPoints()
    {
        return checkpointCounter.get();
    }

    long checkPointAccumulatedTotalTimeMillis()
    {
        return accumulatedCheckpointTotalTimeMillis.get();
    }

    long lastCheckpointTimeMillis()
    {
        return lastCheckpointTimeMillis;
    }

    @Override
    public LogRotateEvent beginLogRotate()
    {
        return countingLogRotateEvent;
    }
}
