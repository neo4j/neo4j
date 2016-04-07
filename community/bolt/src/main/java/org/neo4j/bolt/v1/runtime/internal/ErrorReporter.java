/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.bolt.v1.runtime.internal;

import org.neo4j.helpers.Exceptions;
import org.neo4j.kernel.impl.logging.LogService;
import org.neo4j.logging.Log;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;

/**
 * Report received exceptions into the appropriate log (console or debug) and format exception stack traces in debug.log
 * humanely.
 */
class ErrorReporter
{
    private static final String EMPTY_STRING = "";

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

    public void report( Neo4jError error )
    {
        if ( !error.status().code().classification().publishable() )
        {
            userLog.error( format( "Client triggered an unexpected error: %s. See debug.log for more details.",
                    error.cause().getMessage() ) );
        }

        if(somethingToLog(error))
        {
            debugLog.error( formatFixedWidth( error.cause() ) );
        }
    }

    private boolean somethingToLog( Neo4jError error )
    {
        return error != null && error.cause() != null;
    }

    private String formatFixedWidth( Throwable cause )
    {
        // replaceAll call inserts a line break every 100 characters
        return new String( Exceptions.stringify( cause ).getBytes( UTF_8 ) )
                .replaceAll( "(.{100})", "$1" + System.lineSeparator() );
    }
}
