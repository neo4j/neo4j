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
package org.neo4j.server.logging.slf4j;

import static org.assertj.core.api.Assertions.assertThat;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.status.StatusData;
import org.apache.logging.log4j.status.StatusListener;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.logging.log4j.Log4jLogProvider;
import org.neo4j.test.extension.ForkingTestExtension;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.helpers.NOPLogger;

// We need to fork because log configuration goes to statics
@ExtendWith(ForkingTestExtension.class)
class SLF4JLoggingIT {
    private final StatusLogListener statusLogListener = new StatusLogListener();

    @BeforeEach
    void setUp() {
        StatusLogger.getLogger().registerListener(statusLogListener);
    }

    @AfterEach
    void tearDown() {
        StatusLogger.getLogger().removeListener(statusLogListener);
    }

    @Test
    void useAssignedLogProvider() {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        Log4jLogProvider logProvider = new Log4jLogProvider(outputStream);
        SLF4JLogBridge.setInstantiationContext(logProvider, List.of("org.neo4j.server"));

        var underlyingLogger = logProvider.getLog(SLF4JLoggingIT.class);
        var wrappedLogger = LoggerFactory.getLogger(SLF4JLoggingIT.class);
        wrappedLogger.info("Test");
        var excludedLogger = LoggerFactory.getLogger("not.in.filter.Clazz");
        excludedLogger.info("Should not be written!");

        assertThat(wrappedLogger.isTraceEnabled()).isEqualTo(underlyingLogger.isTraceEnabled());
        assertThat(wrappedLogger.isDebugEnabled()).isEqualTo(underlyingLogger.isDebugEnabled());
        assertThat(wrappedLogger.isInfoEnabled()).isEqualTo(underlyingLogger.isInfoEnabled());
        assertThat(wrappedLogger.isWarnEnabled()).isEqualTo(underlyingLogger.isWarnEnabled());
        assertThat(wrappedLogger.isErrorEnabled()).isEqualTo(underlyingLogger.isErrorEnabled());

        assertThat(outputStream.toString()).contains("SLF4JLoggingIT] Test").doesNotContain("Should not be written!");
        assertThat(statusLogListener.logLines)
                .contains(
                        "Initializing [SLF4JLogBridge] with neo4j log provider with prefix filter [org.neo4j.server].");
    }

    @Test
    void notCallingSetProviderShouldFallbackToNOP() {
        Logger logger = LoggerFactory.getLogger(SLF4JLoggingIT.class);
        assertThat(logger).isInstanceOf(NOPLogger.class);
        assertThat(statusLogListener.logLines).contains("Initializing [SLF4JLogBridge] with NOP provider.");
    }

    static class StatusLogListener implements StatusListener {

        final List<String> logLines = new ArrayList<>();

        @Override
        public void log(StatusData data) {
            logLines.add(data.getMessage().getFormattedMessage());
        }

        @Override
        public Level getStatusLevel() {
            return Level.DEBUG;
        }

        @Override
        public void close() {}
    }
}
