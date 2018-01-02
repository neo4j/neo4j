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
package org.neo4j.kernel.impl.transaction.tracing;

/**
 * Represents the process of turning the state of a committing transaction into a sequence of commands, and appending
 * them to the transaction log.
 */
public interface LogAppendEvent extends LogForceEvents, AutoCloseable
{
    class Empty implements LogAppendEvent
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
            return LogRotateEvent.NULL;
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

    LogAppendEvent NULL = new Empty();

    /**
     * Mark the end of the process of appending a transaction to the transaction log.
     */
    @Override
    void close();

    /**
     * Note whether or not the log was rotated by the appending of this transaction to the log.
     */
    void setLogRotated( boolean logRotated );

    /**
     * Begin a log rotation as part of this appending to the transaction log.
     */
    LogRotateEvent beginLogRotate();

    /**
     * Begin serializing and writing out the commands for this transaction.
     */
    SerializeTransactionEvent beginSerializeTransaction();
}
