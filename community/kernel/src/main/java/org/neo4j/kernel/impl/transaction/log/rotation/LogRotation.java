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
package org.neo4j.kernel.impl.transaction.log.rotation;

import java.io.IOException;

import org.neo4j.kernel.impl.transaction.tracing.LogAppendEvent;

/**
 * Used to check if a log rotation is needed, and also to execute a log rotation.
 *
 * The implementation also makes sure that stores are forced to disk.
 *
 */
public interface LogRotation
{
    interface Monitor
    {
        void startedRotating( long currentVersion );

        void finishedRotating( long currentVersion );
    }

    LogRotation NO_ROTATION = new LogRotation()
    {
        @Override
        public boolean rotateLogIfNeeded( LogAppendEvent logAppendEvent ) throws IOException
        {
            return false;
        }

        @Override
        public void rotateLogFile() throws IOException
        {
        }
    };

    /**
     * @param logAppendEvent A trace event for the current log append operation.
     */
    boolean rotateLogIfNeeded( LogAppendEvent logAppendEvent ) throws IOException;

    /**
     * Force a log rotation.
     *
     * @throws IOException
     */
    void rotateLogFile() throws IOException;
}
