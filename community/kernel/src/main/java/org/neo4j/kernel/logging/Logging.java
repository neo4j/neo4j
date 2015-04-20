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
import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * Logging service that is used for creating loggers with specific names.
 *
 * Individual loggers can end up in different targets potentially.
 */
public interface Logging extends Lifecycle
{
    /**
     * @param loggingClass the context for the return logger.
     * @return a {@link StringLogger} that logs messages with the {@code loggingClass} as context.
     */
    StringLogger getMessagesLog( Class loggingClass );

    /**
     * 
     * @param loggingClass
     * @return a {@link ConsoleLogger} that logs message with the {@code loggingClass} as context.
     * Messages logged with a {@link ConsoleLogger} will be logged to a console 
     */
    ConsoleLogger getConsoleLog( Class loggingClass );
}
