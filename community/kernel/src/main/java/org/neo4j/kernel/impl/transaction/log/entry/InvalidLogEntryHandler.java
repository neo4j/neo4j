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
package org.neo4j.kernel.impl.transaction.log.entry;

import org.neo4j.kernel.impl.transaction.log.LogPosition;

/**
 * Decides what happens to invalid log entries read by {@link LogEntryReader}.
 */
public abstract class InvalidLogEntryHandler
{
    /**
     * Allows no invalid log entries.
     */
    public static final InvalidLogEntryHandler STRICT = new InvalidLogEntryHandler()
    {
    };

    /**
     * Log entry couldn't be read correctly. Could be invalid log entry in the log.
     *
     * @param e error during reading a log entry.
     * @param position {@link LogPosition} of the start of the log entry attempted to be read.
     * @return {@code true} if this error is accepted, otherwise {@code false} which means the exception
     * causing this will be thrown by the caller.
     */
    public boolean handleInvalidEntry( Exception e, LogPosition position )
    {   // consider invalid by default
        return false;
    }

    /**
     * Tells this handler that, given that there were invalid entries, handler thinks they are OK
     * to skip and that one or more entries after a bad section could be read then a certain number
     * of bytes contained invalid log data and were therefore skipped. Log entry reading continues
     * after this call.
     *
     * @param bytesSkipped number of bytes skipped.
     */
    public void bytesSkipped( long bytesSkipped )
    {   // do nothing by default
    }
}
