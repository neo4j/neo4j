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
package org.neo4j.shell.log;

import java.util.Locale;
import java.util.logging.Handler;
import java.util.logging.LogManager;
import java.util.logging.SimpleFormatter;
import org.neo4j.shell.cli.CliArgs;

/**
 * Cypher Shell logger, logs things that should not be visible for users (unless they specifically ask to see logs).
 */
public interface Logger {
    void info(String message);

    void info(String message, Throwable cause);

    void warn(String message, Throwable cause);

    void warn(Throwable cause);

    void error(String message, Throwable cause);

    void error(Throwable cause);

    static Logger create() {
        return ShellLogger.INSTANCE;
    }

    /**
     * Setup logging. We do this in runtime because we have an argument to change log handler.
     */
    static void setupLogging(CliArgs args) {
        args.logHandler().ifPresentOrElse(Logger::setupLogging, Logger::disableLogging);
    }

    private static void setupLogging(Handler handler) {
        disableLogging(); // Start fresh

        final var level = java.util.logging.Level.ALL;

        System.setProperty(
                "java.util.logging.SimpleFormatter.format",
                "%1$tY-%1$tm-%1$td %1$tH:%1$tM:%1$tS %4$-6s %2$s %5$s%6$s%n");

        var rootLogger = LogManager.getLogManager().getLogger("");
        rootLogger.setLevel(level);

        handler.setLevel(level);
        handler.setFormatter(new SimpleFormatter());
        rootLogger.addHandler(handler);
    }

    private static void disableLogging() {
        java.util.logging.Logger.getLogger("org.jline").setLevel(java.util.logging.Level.OFF);
        final var rootLogger = LogManager.getLogManager().getLogger("");

        rootLogger.setLevel(java.util.logging.Level.OFF);

        for (var h : rootLogger.getHandlers()) {
            h.setLevel(java.util.logging.Level.OFF);
            rootLogger.removeHandler(h);
        }
    }

    enum Level {
        ERROR(java.util.logging.Level.SEVERE),
        WARNING(java.util.logging.Level.WARNING),
        INFO(java.util.logging.Level.INFO),
        DEBUG(java.util.logging.Level.FINE),
        ALL(java.util.logging.Level.ALL),
        OFF(java.util.logging.Level.OFF);

        private final java.util.logging.Level javaLevel;

        Level(java.util.logging.Level javaLevel) {
            this.javaLevel = javaLevel;
        }

        public java.util.logging.Level javaLevel() {
            return javaLevel;
        }

        public static Level from(String value) {
            return Level.valueOf(value.toUpperCase(Locale.ROOT));
        }

        public static Level defaultActiveLevel() {
            return DEBUG;
        }
    }
}

class ShellLogger implements Logger {
    static final ShellLogger INSTANCE = new ShellLogger(java.util.logging.Logger.getLogger("org.neo4j.shell"));

    private final java.util.logging.Logger log;

    ShellLogger(java.util.logging.Logger log) {
        this.log = log;
    }

    @Override
    public void info(String message) {
        log.log(Level.INFO.javaLevel(), message);
    }

    @Override
    public void info(String message, Throwable cause) {
        log.log(Level.INFO.javaLevel(), message, cause);
    }

    @Override
    public void warn(String message, Throwable cause) {
        log.log(Level.WARNING.javaLevel(), message, cause);
    }

    @Override
    public void warn(Throwable cause) {
        log.log(Level.WARNING.javaLevel(), cause.getMessage(), cause);
    }

    @Override
    public void error(String message, Throwable cause) {
        log.log(Level.ERROR.javaLevel(), message, cause);
    }

    @Override
    public void error(Throwable cause) {
        log.log(Level.ERROR.javaLevel(), cause.getMessage(), cause);
    }
}
