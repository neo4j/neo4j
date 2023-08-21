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

import static java.util.Map.entry;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.shell.log.Logger.Level.ERROR;
import static org.neo4j.shell.log.Logger.Level.INFO;
import static org.neo4j.shell.log.Logger.Level.WARNING;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.log.Logger.Level;

class LoggerTest {
    @Test
    void logInfo() {
        testLog(INFO, log -> log.info("info"), "info", null);
        testLog(INFO, log -> log.info("info", new RuntimeException("info cause")), "info", "info cause");
        testLogLevelFiltering(INFO);
    }

    @Test
    void logWarning() {
        testLog(WARNING, log -> log.warn(new RuntimeException("warning cause")), "warning cause", "warning cause");
        testLog(WARNING, log -> log.warn("warning", new RuntimeException("warning cause")), "warning", "warning cause");
        testLogLevelFiltering(WARNING);
    }

    @Test
    void logError() {
        testLog(ERROR, log -> log.error(new RuntimeException("error cause")), "error cause", "error cause");
        testLog(ERROR, log -> log.error("error", new RuntimeException("error cause")), "error", "error cause");
        testLogLevelFiltering(ERROR);
    }

    private void testLog(
            Level level, Consumer<Logger> logStatement, String expectedMessage, String expectedExceptionMessage) {
        final var setup = setupLogging(level);

        logStatement.accept(setup.log());

        assertThat(setup.handler().records).hasSize(1);
        final var record = setup.handler().records.get(0);
        assertThat(record.getMessage()).isEqualTo(expectedMessage);
        if (expectedExceptionMessage != null) {
            assertThat(record.getThrown()).hasMessage(expectedExceptionMessage);
        } else {
            assertThat(record.getThrown()).isNull();
        }
    }

    private void testLogLevelFiltering(Level targetLevel) {
        final var setup = setupLogging(targetLevel);
        final var log = setup.log();

        final var statements = List.<Map.Entry<Level, Runnable>>of(
                entry(INFO, () -> log.info("info")),
                entry(INFO, () -> log.info("info", new RuntimeException("info cause"))),
                entry(WARNING, () -> log.warn("warning", new RuntimeException("warning cause"))),
                entry(WARNING, () -> log.warn(new RuntimeException("warning cause"))),
                entry(ERROR, () -> log.error("error", new RuntimeException("error cause"))),
                entry(ERROR, () -> log.error(new RuntimeException("error cause"))));

        statements.forEach(s -> s.getValue().run());

        final var expectedStatements = statements.stream()
                .map(Map.Entry::getKey)
                .filter(l -> l.javaLevel().intValue() >= targetLevel.javaLevel().intValue())
                .toList();
        assertThat(setup.handler().records).hasSameSizeAs(expectedStatements);
    }

    private TestSetup setupLogging(Level level) {
        final var javaLogger = java.util.logging.Logger.getLogger("test-logger");

        javaLogger.setUseParentHandlers(false);
        for (final var defaultHandler : javaLogger.getHandlers()) {
            javaLogger.removeHandler(defaultHandler);
        }

        final var handler = new TestLogHandler();
        handler.setLevel(level.javaLevel());
        javaLogger.addHandler(handler);
        return new TestSetup(new ShellLogger(javaLogger), handler);
    }

    private record TestSetup(Logger log, TestLogHandler handler) {}
}

class TestLogHandler extends Handler {
    List<LogRecord> records = new ArrayList<>();

    @Override
    public void publish(LogRecord record) {
        if (isLoggable(record)) {
            this.records.add(record);
        }
    }

    @Override
    public void flush() {}

    @Override
    public void close() throws SecurityException {}
}
