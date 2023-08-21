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

import org.neo4j.logging.log4j.LoggerTarget;

public class ExternalLogProviderWrapper implements InternalLogProvider {
    private final LogProvider delegate;

    public ExternalLogProviderWrapper(LogProvider delegate) {
        this.delegate = delegate;
    }

    @Override
    public InternalLog getLog(Class<?> loggingClass) {
        return new ExternalLogWrapper(delegate.getLog(loggingClass));
    }

    @Override
    public InternalLog getLog(String name) {
        return new ExternalLogWrapper(delegate.getLog(name));
    }

    @Override
    public InternalLog getLog(LoggerTarget target) {
        return new ExternalLogWrapper(delegate.getLog(target.getTarget()));
    }

    private static class ExternalLogWrapper implements InternalLog {
        private final Log delegate;

        ExternalLogWrapper(Log delegate) {
            this.delegate = delegate;
        }

        @Override
        public void debug(Neo4jLogMessage message) {
            delegate.debug(message.getFormattedMessage());
        }

        @Override
        public void debug(Neo4jMessageSupplier supplier) {
            delegate.debug(supplier.get().getFormattedMessage());
        }

        @Override
        public void info(Neo4jLogMessage message) {
            delegate.info(message.getFormattedMessage());
        }

        @Override
        public void info(Neo4jMessageSupplier supplier) {
            delegate.info(supplier.get().getFormattedMessage());
        }

        @Override
        public void warn(Neo4jLogMessage message) {
            delegate.warn(message.getFormattedMessage());
        }

        @Override
        public void warn(Neo4jMessageSupplier supplier) {
            delegate.warn(supplier.get().getFormattedMessage());
        }

        @Override
        public void error(Neo4jLogMessage message) {
            delegate.error(message.getFormattedMessage());
        }

        @Override
        public void error(Neo4jMessageSupplier supplier) {
            delegate.error(supplier.get().getFormattedMessage());
        }

        @Override
        public void error(Neo4jLogMessage message, Throwable throwable) {
            delegate.error(message.getFormattedMessage(), throwable);
        }

        @Override
        public boolean isDebugEnabled() {
            return delegate.isDebugEnabled();
        }

        @Override
        public void debug(String message) {
            delegate.debug(message);
        }

        @Override
        public void debug(String message, Throwable throwable) {
            delegate.debug(message, throwable);
        }

        @Override
        public void debug(String format, Object... arguments) {
            delegate.debug(format, arguments);
        }

        @Override
        public void info(String message) {
            delegate.info(message);
        }

        @Override
        public void info(String message, Throwable throwable) {
            delegate.info(message, throwable);
        }

        @Override
        public void info(String format, Object... arguments) {
            delegate.info(format, arguments);
        }

        @Override
        public void warn(String message) {
            delegate.warn(message);
        }

        @Override
        public void warn(String message, Throwable throwable) {
            delegate.warn(message, throwable);
        }

        @Override
        public void warn(String format, Object... arguments) {
            delegate.warn(format, arguments);
        }

        @Override
        public void error(String message) {
            delegate.error(message);
        }

        @Override
        public void error(String message, Throwable throwable) {
            delegate.error(message, throwable);
        }

        @Override
        public void error(String format, Object... arguments) {
            delegate.error(format, arguments);
        }
    }
}
