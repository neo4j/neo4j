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

import static java.util.Objects.requireNonNull;

import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.Neo4jMessageSupplier;

public class PrefixedLog implements InternalLog {
    private final String prefix;
    private final InternalLog delegate;

    PrefixedLog(String prefix, InternalLog delegate) {
        requireNonNull(prefix, "prefix must be a string");
        requireNonNull(delegate, "delegate log cannot be null");
        this.prefix = "[" + prefix + "] ";
        this.delegate = delegate;
    }

    @Override
    public boolean isDebugEnabled() {
        return delegate.isDebugEnabled();
    }

    @Override
    public void debug(String message) {
        delegate.debug(withPrefix(message));
    }

    @Override
    public void debug(String message, Throwable throwable) {
        delegate.debug(withPrefix(message), throwable);
    }

    @Override
    public void debug(String format, Object... arguments) {
        delegate.debug(withPrefix(format), arguments);
    }

    @Override
    public void info(String message) {
        delegate.info(withPrefix(message));
    }

    @Override
    public void info(String message, Throwable throwable) {
        delegate.info(withPrefix(message), throwable);
    }

    @Override
    public void info(String format, Object... arguments) {
        delegate.info(withPrefix(format), arguments);
    }

    @Override
    public void warn(String message) {
        delegate.warn(withPrefix(message));
    }

    @Override
    public void warn(String message, Throwable throwable) {
        delegate.warn(withPrefix(message), throwable);
    }

    @Override
    public void warn(String format, Object... arguments) {
        delegate.warn(withPrefix(format), arguments);
    }

    @Override
    public void error(String message) {
        delegate.error(withPrefix(message));
    }

    @Override
    public void error(String message, Throwable throwable) {
        delegate.error(withPrefix(message), throwable);
    }

    @Override
    public void error(String format, Object... arguments) {
        delegate.error(withPrefix(format), arguments);
    }

    @Override
    public void debug(Neo4jLogMessage message) {
        delegate.debug(new PrefixedNeo4jLogMessage(message));
    }

    @Override
    public void debug(Neo4jMessageSupplier supplier) {
        delegate.debug(() -> new PrefixedNeo4jLogMessage(supplier.get()));
    }

    @Override
    public void info(Neo4jLogMessage message) {
        delegate.info(new PrefixedNeo4jLogMessage(message));
    }

    @Override
    public void info(Neo4jMessageSupplier supplier) {
        delegate.info(() -> new PrefixedNeo4jLogMessage(supplier.get()));
    }

    @Override
    public void warn(Neo4jLogMessage message) {
        delegate.warn(new PrefixedNeo4jLogMessage(message));
    }

    @Override
    public void warn(Neo4jMessageSupplier supplier) {
        delegate.warn(() -> new PrefixedNeo4jLogMessage(supplier.get()));
    }

    @Override
    public void error(Neo4jLogMessage message) {
        delegate.error(new PrefixedNeo4jLogMessage(message));
    }

    @Override
    public void error(Neo4jMessageSupplier supplier) {
        delegate.error(() -> new PrefixedNeo4jLogMessage(supplier.get()));
    }

    @Override
    public void error(Neo4jLogMessage message, Throwable throwable) {
        delegate.error(new PrefixedNeo4jLogMessage(message), throwable);
    }

    private String withPrefix(String message) {
        return prefix + message;
    }

    private class PrefixedNeo4jLogMessage implements Neo4jLogMessage {
        private final Neo4jLogMessage delegate;

        private PrefixedNeo4jLogMessage(Neo4jLogMessage delegate) {
            this.delegate = delegate;
        }

        @Override
        public String getFormattedMessage() {
            return withPrefix(delegate.getFormattedMessage());
        }

        @Override
        public String getFormat() {
            return withPrefix(delegate.getFormat());
        }

        @Override
        public Object[] getParameters() {
            return delegate.getParameters();
        }

        @Override
        public Throwable getThrowable() {
            return delegate.getThrowable();
        }
    }
}
