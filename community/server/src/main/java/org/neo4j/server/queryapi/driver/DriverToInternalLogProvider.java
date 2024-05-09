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
package org.neo4j.server.queryapi.driver;

import org.neo4j.driver.Logger;
import org.neo4j.driver.Logging;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * A {@link Logging} implementation which maps driver log output to neo4j's {@link InternalLogProvider\}.
 */
class DriverToInternalLogProvider implements Logging {

    private final InternalLogProvider logProvider;

    DriverToInternalLogProvider(InternalLogProvider logProvider) {
        this.logProvider = logProvider;
    }

    @Override
    public Logger getLog(String name) {
        return new DriverLogMappingLogger(logProvider.getLog(name));
    }

    static class DriverLogMappingLogger implements Logger {

        private final InternalLog log;

        DriverLogMappingLogger(InternalLog log) {
            this.log = log;
        }

        @Override
        public void error(String message, Throwable cause) {
            log.error(message, cause);
        }

        @Override
        public void info(String message, Object... params) {
            log.info(message, params);
        }

        @Override
        public void warn(String message, Object... params) {
            log.warn(message, params);
        }

        @Override
        public void warn(String message, Throwable cause) {
            log.warn(message, cause);
        }

        @Override
        public void debug(String message, Object... params) {
            log.debug(message, params);
        }

        @Override
        public void debug(String message, Throwable throwable) {
            log.debug(message, throwable);
        }

        @Override
        public void trace(String message, Object... params) {
            log.debug(message, params);
        }

        @Override
        public boolean isTraceEnabled() {
            return false;
        }

        @Override
        public boolean isDebugEnabled() {
            return log.isDebugEnabled();
        }
    }
}
