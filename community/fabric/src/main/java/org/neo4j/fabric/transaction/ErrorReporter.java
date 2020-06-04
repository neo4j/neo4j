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
package org.neo4j.fabric.transaction;

import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.logging.Log;
import org.neo4j.logging.internal.LogService;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Classification.DatabaseError;

/**
 * Report received exceptions into the appropriate log (console or debug) and delivery stacktraces to debug.log.
 */
public class ErrorReporter
{
    private final Log userLog;
    private final Log debugLog;

    public ErrorReporter( LogService logging )
    {
        this.userLog = logging.getUserLog( ErrorReporter.class );
        this.debugLog = logging.getInternalLog( ErrorReporter.class );
    }

    /**
     * Writes logs about database errors.
     * Short one-line message is written to both user and internal log.
     * Large message with stacktrace is written to internal log.
     */
    public void report( String message, Throwable error, Status defaultStatus )
    {
        Status status = defaultStatus;
        if ( error instanceof Status.HasStatus )
        {
            status = ((Status.HasStatus) error).status();
        }

        if ( status.code().classification() == DatabaseError )
        {
            String logMessage = format( "Unexpected error [%s]: %s.", status.code().serialize(), message );

            // Writing to user log gets duplicated to the internal log
            userLog.error( logMessage );

            // Write to internal log with full stack trace
            debugLog.error( logMessage, error );
        }
    }
}
