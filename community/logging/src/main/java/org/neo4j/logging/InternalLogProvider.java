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

import java.io.Closeable;
import org.neo4j.logging.log4j.LoggerTarget;

/**
 * Used to obtain a {@link InternalLog} for a specified context
 */
public interface InternalLogProvider extends LogProvider, Closeable {

    /**
     * @param loggingClass the context for the returned {@link InternalLog}.
     * @return a {@link InternalLog} that logs messages with the {@code loggingClass} as the context.
     */
    @Override
    InternalLog getLog(Class<?> loggingClass);

    /**
     * @param name the context for the returned {@link InternalLog}.
     * @return a {@link InternalLog} that logs messages with the specified name as the context.
     */
    @Override
    InternalLog getLog(String name);

    /**
     * @param target The target appender for special cases. If you want a generic logger use {@link LoggerTarget#ROOT_LOGGER}.
     * @return a {@link InternalLog} that logs messages to the specific target.
     */
    InternalLog getLog(LoggerTarget target);

    @Override
    default void close() {
        // Most loggers don't need closing
    }
}
