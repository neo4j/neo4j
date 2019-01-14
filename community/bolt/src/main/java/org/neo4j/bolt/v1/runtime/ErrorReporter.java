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
package org.neo4j.bolt.v1.runtime;

import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.kernel.impl.logging.StoreLogService;
import org.neo4j.logging.DuplicatingLogProvider;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static org.neo4j.kernel.api.exceptions.Status.Classification.DatabaseError;

/**
 * Report received exceptions into the appropriate log (console or debug) and delivery stacktraces to debug.log.
 */
class ErrorReporter
{
    private final Log userLog;
    private final Log debugLog;

    ErrorReporter( LogService logging )
    {
        this.userLog = logging.getUserLog( ErrorReporter.class );
        this.debugLog = logging.getInternalLog( ErrorReporter.class );
    }

    ErrorReporter( Log userLog, Log debugLog )
    {
        this.userLog = userLog;
        this.debugLog = debugLog;
    }

    /**
     * Writes logs about database errors.
     * Short one-line message is written to both user and internal log.
     * Large message with stacktrace (if available) is written to internal log.
     *
     * @param error the error to log.
     * @see StoreLogService
     * @see DuplicatingLogProvider
     */
    public void report( Neo4jError error )
    {
        if ( error.status().code().classification() == DatabaseError )
        {
            String message = format( "Client triggered an unexpected error [%s]: %s, reference %s.",
                    error.status().code().serialize(), error.message(), error.reference() );

            // Writing to user log gets duplicated to the internal log
            userLog.error( message );

            // If cause/stacktrace is available write it to the internal log
            if ( error.cause() != null )
            {
                debugLog.error( message, error.cause() );
            }
        }
    }
}
