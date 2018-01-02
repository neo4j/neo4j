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
 * A log into which messages can be written
 */
public interface Logger
{
    /**
     * @param message The message to be written
     */
    void log( String message );

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    void log( String message, Throwable throwable );

    /**
     * @param format    A string format for writing a message
     * @param arguments Arguments to substitute into the message according to the {@param format}
     */
    void log( String format, Object... arguments );

    /**
     * Used to temporarily write several messages in bulk. The implementation may choose to
     * disable flushing, and may also block other operations until the bulk update is completed.
     *
     * @param consumer A callback operation that accepts an equivalent {@link Logger}
     */
    void bulk( Consumer<Logger> consumer );
}
