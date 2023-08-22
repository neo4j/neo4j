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

import java.io.Closeable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.core.Logger;
import org.apache.logging.log4j.message.StringFormatterMessageFactory;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.apache.logging.log4j.spi.LoggerContext;
import org.neo4j.io.IOUtils;

/**
 * Facade for Log4j LoggerContext.
 */
public class Neo4jLoggerContext implements Closeable {
    private final LoggerContext ctx;
    private final Closeable additionalClosable;
    private final String configSourceInfo;

    public Neo4jLoggerContext(LoggerContext ctx, Closeable additionalClosable, String configSourceInfo) {
        this.ctx = ctx;
        this.additionalClosable = additionalClosable;
        this.configSourceInfo = configSourceInfo;
    }

    /**
     * Package-private specifically to not leak {@link Logger} outside logging module.
     * Should not be used outside of the logging module.
     */
    public ExtendedLogger getLogger(Class<?> clazz) {
        // StringFormatterMessageFactory will recognize printf syntax, default is anchor {} which we don't use
        return ctx.getLogger(clazz, StringFormatterMessageFactory.INSTANCE);
    }

    /**
     * Package-private specifically to not leak {@link Logger} outside logging module.
     * Should not be used outside of the logging module.
     */
    ExtendedLogger getLogger(String name) {
        // StringFormatterMessageFactory will recognize printf syntax, default is anchor {} which we don't use
        return ctx.getLogger(name, StringFormatterMessageFactory.INSTANCE);
    }

    /**
     * Package-private specifically to not leak {@link LoggerContext} outside logging module.
     * Should not be used outside of the logging module.
     */
    LoggerContext getLoggerContext() {
        return ctx;
    }

    boolean haveExternalResources() {
        return additionalClosable != null;
    }

    @Override
    public void close() {
        LogManager.shutdown(ctx);
        if (additionalClosable != null) {
            IOUtils.closeAllSilently(additionalClosable);
        }
    }

    public String getConfigSourceInfo() {
        return configSourceInfo;
    }
}
