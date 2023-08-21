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
package org.neo4j.logging.internal;

import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * Logging service that is used to obtain loggers for different output purposes
 */
public interface LogService {
    /**
     * @return a {@link InternalLogProvider} that providers loggers for user visible messages. Usually backed by user-logs.xml.
     */
    InternalLogProvider getUserLogProvider();

    /**
     * Equivalent to {@code getUserLogProvider().getLog( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link InternalLog} that logs user visible messages with the {@code loggingClass} as context.
     */
    InternalLog getUserLog(Class<?> loggingClass);

    /**
     * @return a {@link InternalLogProvider} that providers loggers for internal messages. Usually backed by server-logs.xml.
     */
    InternalLogProvider getInternalLogProvider();

    /**
     * Equivalent to {@code #getInternalLogProvider().getLog( loggingClass )}
     * @param loggingClass the context for the return logger.
     * @return a {@link InternalLog} that logs internal messages with the {@code loggingClass} as context.
     */
    InternalLog getInternalLog(Class<?> loggingClass);
}
