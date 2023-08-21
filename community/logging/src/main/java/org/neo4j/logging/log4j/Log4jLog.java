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
package org.neo4j.logging.log4j;

import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.ExtendedLoggerWrapper;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jLogMessage;
import org.neo4j.logging.Neo4jMessageSupplier;

/**
 * A {@link InternalLog} implementation that uses the Log4j configuration the logger is connected to.
 */
public class Log4jLog extends ExtendedLoggerWrapper implements InternalLog {
    /**
     * Package-private specifically to not leak Logger outside logging module. Should not be used outside of the logging module - {@link
     * Log4jLogProvider#getLog} should be used instead.
     */
    Log4jLog(ExtendedLogger logger) {
        super(logger, logger.getName(), logger.getMessageFactory());
    }

    @Override
    public boolean isDebugEnabled() {
        return logger.isDebugEnabled();
    }

    @Override
    public void debug(Neo4jLogMessage message) {
        logger.debug(message);
    }

    @Override
    public void debug(Neo4jMessageSupplier supplier) {
        logger.debug(supplier);
    }

    @Override
    public void info(Neo4jLogMessage message) {
        logger.info(message);
    }

    @Override
    public void info(Neo4jMessageSupplier supplier) {
        logger.info(supplier);
    }

    @Override
    public void warn(Neo4jLogMessage message) {
        logger.warn(message);
    }

    @Override
    public void warn(Neo4jMessageSupplier supplier) {
        logger.warn(supplier);
    }

    @Override
    public void error(Neo4jLogMessage message) {
        logger.error(message);
    }

    @Override
    public void error(Neo4jMessageSupplier supplier) {
        logger.error(supplier);
    }

    @Override
    public void error(Neo4jLogMessage message, Throwable throwable) {
        logger.error(message, throwable);
    }
}
