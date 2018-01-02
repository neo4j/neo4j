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
package org.neo4j.logging;

import org.neo4j.function.Consumer;

/**
 * A log into which various levels of messages can be written
 */
public interface Log
{
    /**
     * @return true if the current log level enables debug logging
     */
    boolean isDebugEnabled();

    /**
     * @return a {@link Logger} instance for writing debug messages
     */
    Logger debugLogger();

    /**
     * Shorthand for {@code debugLogger().log( message )}
     *
     * @param message The message to be written
     */
    void debug( String message );

    /**
     * Shorthand for {@code debugLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void debug( String message, Throwable throwable );

    /**
     * Shorthand for {@code debugLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void debug( String format, Object... arguments );

    /**
     * @return a {@link Logger} instance for writing info messages
     */
    Logger infoLogger();

    /**
     * Shorthand for {@code infoLogger().log( message )}
     *
     * @param message The message to be written
     */
    void info( String message );

    /**
     * Shorthand for {@code infoLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void info( String message, Throwable throwable );

    /**
     * Shorthand for {@code infoLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void info( String format, Object... arguments );

    /**
     * @return a {@link Logger} instance for writing warn messages
     */
    Logger warnLogger();

    /**
     * Shorthand for {@code warnLogger().log( message )}
     *
     * @param message The message to be written
     */
    void warn( String message );

    /**
     * Shorthand for {@code warnLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void warn( String message, Throwable throwable );

    /**
     * Shorthand for {@code warnLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void warn( String format, Object... arguments );

    /**
     * @return a {@link Logger} instance for writing error messages
     */
    Logger errorLogger();

    /**
     * Shorthand for {@code errorLogger().log( message )}
     *
     * @param message The message to be written
     */
    void error( String message );

    /**
     * Shorthand for {@code errorLogger().log( message, throwable )}
     *
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void error( String message, Throwable throwable );

    /**
     * Shorthand for {@code errorLogger().log( format, arguments )}
     *
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the {@param format}
     */
    void error( String format, Object... arguments );

    /**
     * Used to temporarily log several messages in bulk. The implementation may choose to
     * disable flushing, and may also block other operations until the bulk update is completed.
     *
     * @param consumer A consumer for the bulk {@link Log}
     */
    void bulk( Consumer<Log> consumer );
}
