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
 * A {@link InternalLog} implementation that duplicates all messages to other Log instances
 */
public class DuplicatingLog implements InternalLog {
    private final InternalLog log1;
    private final InternalLog log2;

    public DuplicatingLog(InternalLog log1, InternalLog log2) {
        this.log1 = log1;
        this.log2 = log2;
    }

    @Override
    public boolean isDebugEnabled() {
        return log1.isDebugEnabled() || log2.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        log1.debug(message);
        log2.debug(message);
    }

    @Override
    public void debug(String message, Throwable throwable) {
        log1.debug(message, throwable);
        log2.debug(message, throwable);
    }

    @Override
    public void debug(String format, Object... arguments) {
        log1.debug(format, arguments);
        log2.debug(format, arguments);
    }

    @Override
    public void info(String message) {
        log1.info(message);
        log2.info(message);
    }

    @Override
    public void info(String message, Throwable throwable) {
        log1.info(message, throwable);
        log2.info(message, throwable);
    }

    @Override
    public void info(String format, Object... arguments) {
        log1.info(format, arguments);
        log2.info(format, arguments);
    }

    @Override
    public void warn(String message) {
        log1.warn(message);
        log2.warn(message);
    }

    @Override
    public void warn(String message, Throwable throwable) {
        log1.warn(message, throwable);
        log2.warn(message, throwable);
    }

    @Override
    public void warn(String format, Object... arguments) {
        log1.warn(format, arguments);
        log2.warn(format, arguments);
    }

    @Override
    public void error(String message) {
        log1.error(message);
        log2.error(message);
    }

    @Override
    public void error(String message, Throwable throwable) {
        log1.error(message, throwable);
        log2.error(message, throwable);
    }

    @Override
    public void error(String format, Object... arguments) {
        log1.error(format, arguments);
        log2.error(format, arguments);
    }

    @Override
    public void debug(Neo4jLogMessage message) {
        log1.debug(message);
        log2.debug(message);
    }

    @Override
    public void debug(Neo4jMessageSupplier supplier) {
        log1.debug(supplier);
        log2.debug(supplier);
    }

    @Override
    public void info(Neo4jLogMessage message) {
        log1.info(message);
        log2.info(message);
    }

    @Override
    public void info(Neo4jMessageSupplier supplier) {
        log1.info(supplier);
        log2.info(supplier);
    }

    @Override
    public void warn(Neo4jLogMessage message) {
        log1.warn(message);
        log2.warn(message);
    }

    @Override
    public void warn(Neo4jMessageSupplier supplier) {
        log1.warn(supplier);
        log2.warn(supplier);
    }

    @Override
    public void error(Neo4jLogMessage message) {
        log1.error(message);
        log2.error(message);
    }

    @Override
    public void error(Neo4jMessageSupplier supplier) {
        log1.error(supplier);
        log2.error(supplier);
    }

    @Override
    public void error(Neo4jLogMessage message, Throwable throwable) {
        log1.error(message, throwable);
        log2.error(message, throwable);
    }
}
