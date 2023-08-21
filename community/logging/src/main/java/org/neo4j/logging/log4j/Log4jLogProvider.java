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

import java.io.OutputStream;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.Level;

/**
 * A {@link InternalLogProvider} implementation that uses the Log4j configuration ctx is connected to.
 */
public class Log4jLogProvider implements InternalLogProvider {
    private final Neo4jLoggerContext ctx;

    public Log4jLogProvider(Neo4jLoggerContext ctx) {
        this.ctx = ctx;
    }

    public Log4jLogProvider(OutputStream out) {
        this(out, Level.INFO);
    }

    public Log4jLogProvider(OutputStream out, Level level) {
        this(LogConfig.createBuilderToOutputStream(out, level).build());
    }

    @SuppressWarnings("unused") // Will be used by new procedure
    public void updateLogLevel(Level newLevel) {
        LogConfig.updateLogLevel(newLevel, ctx);
    }

    @Override
    public Log4jLog getLog(Class<?> loggingClass) {
        return new Log4jLog(ctx.getLogger(loggingClass));
    }

    @Override
    public Log4jLog getLog(String name) {
        return new Log4jLog(ctx.getLogger(name));
    }

    @Override
    public Log4jLog getLog(LoggerTarget target) {
        return new Log4jLog(ctx.getLogger(target.getTarget()));
    }

    @Override
    public void close() {
        ctx.close();
    }
}
