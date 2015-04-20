/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.kernel.logging;

import org.neo4j.kernel.impl.util.StringLogger;

import static org.neo4j.kernel.logging.LogMarker.CONSOLE_MARK;

/**
 * Logger that will log to a console in addition to a normal {@link StringLogger}.
 * Messages logged in here are user facing so extra consideration should be made about its contents.
 */
public class ConsoleLogger
{
    private final StringLogger realLogger;

    public ConsoleLogger( StringLogger realLogger )
    {
        this.realLogger = realLogger;
    }

    /**
     * Logs a message to the user.
     * @param message the message to log.
     */
    public void log( String message )
    {
        realLogger.logMessage( message, CONSOLE_MARK );
    }

    /**
     * Logs a message to the user.
     * @param format the format in {@link String#format(String, Object...)}.
     * @param parameters the parameters that go into the format.
     */
    public void log( String format, Object... parameters )
    {
        log( String.format( format, parameters ) );
    }

    /**
     * Logs a warning to the user.
     * @param message warning message
     */
    public void warn( String message )
    {
        log( message );
    }

    /**
     * Logs a warning to the user.
     * @param format the format in {@link String#format(String, Object...)}.
     * @param parameters the parameters that go into the format.
     */
    public void warn( String format, Object... parameters )
    {
        log( format, parameters );
    }

    /**
     * Logs a warning, including a cause to the user.
     * @param message the warning message.
     * @param warning the cause of the warning
     * @deprecated since users shouldn't have to see stack traces. Stack traces are for
     * developers, not for users. This method exists due to removing any of other logging frameworks
     * that would log to the console.
     */
    @Deprecated
    public void warn( String message, Throwable warning )
    {
        realLogger.logMessage( message, warning, false, CONSOLE_MARK );
    }

    /**
     * Logs an error to the user.
     * @param message error message
     */
    public void error( String message )
    {
        log( message );
    }

    /**
     * Logs an error to the user.
     * @param format the format in {@link String#format(String, Object...)}.
     * @param parameters the parameters that go into the format.
     */
    public void error( String format, Object... parameters )
    {
        log( format, parameters );
    }

    /**
     * Logs an error, including a cause to the user.
     * @param message the warning message.
     * @param error the cause of the error
     * @deprecated since users shouldn't have to see stack traces. Stack traces are for
     * developers, not for users. This method exists due to removing any of other logging frameworks
     * that would log to the console.
     */
    @Deprecated
    public void error( String message, Throwable error )
    {
        realLogger.logMessage( message, error, true, CONSOLE_MARK );
    }

    public static final ConsoleLogger DEV_NULL = new ConsoleLogger( StringLogger.DEV_NULL );
}
