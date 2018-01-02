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
package org.neo4j.kernel.impl.logging;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

/**
 * Logging service that is used to obtain loggers for different output purposes
 */
public interface LogService
{
    /**
     * @return a {@link org.neo4j.logging.LogProvider} that providers loggers for user visible messages.
     */
    LogProvider getUserLogProvider();

    /**
     * Equivalent to {@code {@link #getUserLogProvider}()( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link org.neo4j.logging.Log} that logs user visible messages with the {@code loggingClass} as context.
     */
    Log getUserLog( Class loggingClass );

    /**
     * @return a {@link LogProvider} that providers loggers for internal messages.
     */
    LogProvider getInternalLogProvider();

    /**
     * Equivalent to {@code {@link #getInternalLogProvider}()( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link Log} that logs internal messages with the {@code loggingClass} as context.
     */
    Log getInternalLog( Class loggingClass );
}
