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
package org.neo4j.kernel.impl.transaction.tracing;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Represents the process of appending a check point to the transaction log.
 */
public interface LogCheckPointEvent extends LogForceEvents, LogRotateEvents, AutoCloseable
{
    LogCheckPointEvent NULL = new LogCheckPointEvent()
    {
        @Override
        public LogRotateEvent beginLogRotate()
        {
            return LogRotateEvent.NULL;
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

        @Override
        public void checkpointCompleted( long checkpointMillis )
        {
        }

        @Override
        public void close()
        {
        }

        @Override
        public void appendToLogFile( LogPosition positionBeforeCheckpoint, LogPosition positionAfterCheckpoint )
        {

        }
    };

    /**
     * Notify about completion of checkpoint that took {@code checkpointMillis} to complete
     * @param checkpointMillis checkpoint duration
     */
    void checkpointCompleted( long checkpointMillis );

    /**
     * Marks the end of the check pointing process.
     */
    @Override
    void close();

    /**
     * Notify about checkpoint append into the current log file.
     * New data is appended to the end of the log file and located between {@code positionBeforeCheckpoint} and {@code positionAfterCheckpoint}
     * @param positionBeforeCheckpoint start position
     * @param positionAfterCheckpoint end position
     */
    void appendToLogFile( LogPosition positionBeforeCheckpoint, LogPosition positionAfterCheckpoint );
}
