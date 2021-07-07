/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.logging.log4j;

import org.neo4j.logging.Log;

/**
 * Extends the logging API with a few methods that are not part of the public API yet.
 */
public interface LogExtended extends Log
{
    /**
     * @param message The message to be written
     */
    default void debug( Neo4jLogMessage message )
    {
        debug( message.getFormattedMessage() );
    }

    /**
     * @param supplier The supplier of the message to be written
     */
    default void debug( Neo4jMessageSupplier supplier )
    {
        debug( supplier.get() );
    }

    /**
     * @param message The message to be written
     */
    default void info( Neo4jLogMessage message )
    {
        info( message.getFormattedMessage() );
    }

    /**
     * @param supplier The supplier of the message to be written
     */
    default void info( Neo4jMessageSupplier supplier )
    {
        info( supplier.get() );
    }

    /**
     * @param message The message to be written
     */
    default void warn( Neo4jLogMessage message )
    {
        warn( message.getFormattedMessage());
    }

    /**
     * @param supplier The supplier of the message to be written
     */
    default void warn( Neo4jMessageSupplier supplier )
    {
        warn( supplier.get() );
    }

    /**
     * @param message The message to be written
     */
    default void error( Neo4jLogMessage message )
    {
        error( message.getFormattedMessage() );
    }

    /**
     * @param supplier The supplier of the message to be written
     */
    default void error( Neo4jMessageSupplier supplier )
    {
        error( supplier.get() );
    }

    /**
     * @param message   The message to be written
     * @param throwable An exception that will also be written
     */
    default void error( Neo4jLogMessage message, Throwable throwable )
    {
        error( message.getFormattedMessage(), throwable );
    }
}
