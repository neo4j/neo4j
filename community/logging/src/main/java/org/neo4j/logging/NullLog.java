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

/**
 * A {@link InternalLog} implementation that discards all messages
 */
public final class NullLog implements InternalLog {
    private static final NullLog INSTANCE = new NullLog();

    private NullLog() {}

    /**
     * @return A singleton {@link NullLog} instance
     */
    public static NullLog getInstance() {
        return INSTANCE;
    }

    @Override
    public boolean isDebugEnabled() {
        return false;
    }

    @Override
    public void debug(String message) {}

    @Override
    public void debug(String message, Throwable throwable) {}

    @Override
    public void debug(String format, Object... arguments) {}

    @Override
    public void info(String message) {}

    @Override
    public void info(String message, Throwable throwable) {}

    @Override
    public void info(String format, Object... arguments) {}

    @Override
    public void warn(String message) {}

    @Override
    public void warn(String message, Throwable throwable) {}

    @Override
    public void warn(String format, Object... arguments) {}

    @Override
    public void error(String message) {}

    @Override
    public void error(String message, Throwable throwable) {}

    @Override
    public void error(String format, Object... arguments) {}

    @Override
    public void debug(Neo4jLogMessage message) {}

    @Override
    public void debug(Neo4jMessageSupplier supplier) {}

    @Override
    public void info(Neo4jLogMessage message) {}

    @Override
    public void info(Neo4jMessageSupplier supplier) {}

    @Override
    public void warn(Neo4jLogMessage message) {}

    @Override
    public void warn(Neo4jMessageSupplier supplier) {}

    @Override
    public void error(Neo4jLogMessage message) {}

    @Override
    public void error(Neo4jMessageSupplier supplier) {}

    @Override
    public void error(Neo4jLogMessage message, Throwable throwable) {}
}
