/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.logging;

import org.neo4j.annotations.api.PublicApi;

/**
 * A log into which various levels of messages can be written
 */
@PublicApi
public interface Log {
    /**
     * @return true if the current log level enables debug logging
     */
    boolean isDebugEnabled();

    /**
     * @param message The message to be written
     */
    void debug(String message);

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void debug(String message, Throwable throwable);

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void debug(String format, Object... arguments);

    /**
     * @param message The message to be written
     */
    void info(String message);

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void info(String message, Throwable throwable);

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void info(String format, Object... arguments);

    /**
     * @param message The message to be written
     */
    void warn(String message);

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void warn(String message, Throwable throwable);

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the format
     */
    void warn(String format, Object... arguments);

    /**
     * @param message The message to be written
     */
    void error(String message);

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void error(String message, Throwable throwable);

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the {@code format}
     */
    void error(String format, Object... arguments);
}
