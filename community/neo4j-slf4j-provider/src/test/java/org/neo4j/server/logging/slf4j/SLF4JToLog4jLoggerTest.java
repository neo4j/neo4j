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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;

import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.message.Message;
import org.apache.logging.log4j.spi.ExtendedLogger;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.mockito.Mockito;
import org.slf4j.event.Level;

class SLF4JToLog4jLoggerTest {
    private static final Throwable TRACE_EXCEPTION = new RuntimeException("TRACE LEVEL");
    private static final Throwable DEBUG_EXCEPTION = new RuntimeException("DEBUG LEVEL");
    private static final Throwable INFO_EXCEPTION = new RuntimeException("INFO LEVEL");
    private static final Throwable WARN_EXCEPTION = new RuntimeException("WARN LEVEL");
    private static final Throwable ERROR_EXCEPTION = new RuntimeException("ERROR LEVEL");

    private final ExtendedLogger logger = Mockito.mock(ExtendedLogger.class);
    private final ArrayList<String> logMessages = new ArrayList<>();

    @BeforeEach
    void setUp() {
        doAnswer(i -> {
                    Message msg = i.getArgument(3);
                    Throwable t = i.getArgument(4);
                    logMessages.add(msg.getFormattedMessage() + (t != null ? " " + t.getMessage() : ""));
                    return null;
                })
                .when(logger)
                .logMessage(any(), any(), any(), any(), any());
    }

    @ParameterizedTest
    @EnumSource
    void respectLogLevels(Level level) {
        setLogLevelOfDelegate(level, logger);

        SLF4JToLog4jLogger log = new SLF4JToLog4jLogger(null, logger, "test");
        logAll(log);
        List<String> expectedForLevel = getExpectedForLevel(level);
        assertThat(logMessages).hasSameElementsAs(expectedForLevel);
    }

    @SuppressWarnings("PlaceholderCountMatchesArgumentCount")
    @Test
    void dontCrashOnMalformedLogging() {
        setLogLevelOfDelegate(Level.TRACE, logger);
        SLF4JToLog4jLogger log = new SLF4JToLog4jLogger(null, logger, "test");
        log.trace("Too many arguments {}", 1, 2);
        log.trace("Too few arguments {} {}", 1);
        log.trace("No anchors", 1, 2, 3);
        assertThat(logMessages).containsExactly("Too many arguments 1", "Too few arguments 1 {}", "No anchors");
    }

    private void logAll(SLF4JToLog4jLogger log) {
        log.trace("trace");
        log.trace("trace {}", "arg");
        log.trace("trace {} {}", "arg1", "arg2");
        log.trace("trace {} {} {}", 1, 2, 3);
        log.trace("trace {} {} {}", 1, 2, 3, TRACE_EXCEPTION);
        log.trace("trace", TRACE_EXCEPTION);

        log.debug("debug");
        log.debug("debug {}", "arg");
        log.debug("debug {} {}", "arg1", "arg2");
        log.debug("debug {} {} {}", 1, 2, 3);
        log.debug("debug {} {} {}", 1, 2, 3, DEBUG_EXCEPTION);
        log.debug("debug", DEBUG_EXCEPTION);

        log.info("info");
        log.info("info {}", "arg");
        log.info("info {} {}", "arg1", "arg2");
        log.info("info {} {} {}", 1, 2, 3);
        log.info("info {} {} {}", 1, 2, 3, INFO_EXCEPTION);
        log.info("info", INFO_EXCEPTION);

        log.warn("warn");
        log.warn("warn {}", "arg");
        log.warn("warn {} {}", "arg1", "arg2");
        log.warn("warn {} {} {}", 1, 2, 3, WARN_EXCEPTION);
        log.warn("warn", WARN_EXCEPTION);

        log.error("error");
        log.error("error {}", "arg");
        log.error("error {} {}", "arg1", "arg2");
        log.error("error {} {} {}", 1, 2, 3, ERROR_EXCEPTION);
        log.error("error", ERROR_EXCEPTION);
    }

    private List<String> getExpectedForLevel(Level level) {
        List<String> ret = new ArrayList<>();

        if (level.toInt() <= Level.TRACE.toInt()) {
            ret.add("trace");
            ret.add("trace arg");
            ret.add("trace arg1 arg2");
            ret.add("trace 1 2 3");
            ret.add("trace 1 2 3 TRACE LEVEL");
            ret.add("trace TRACE LEVEL");
        }
        if (level.toInt() <= Level.DEBUG.toInt()) {
            ret.add("debug");
            ret.add("debug arg");
            ret.add("debug arg1 arg2");
            ret.add("debug 1 2 3");
            ret.add("debug 1 2 3 DEBUG LEVEL");
            ret.add("debug DEBUG LEVEL");
        }
        if (level.toInt() <= Level.INFO.toInt()) {
            ret.add("info");
            ret.add("info arg");
            ret.add("info arg1 arg2");
            ret.add("info 1 2 3");
            ret.add("info 1 2 3 INFO LEVEL");
            ret.add("info INFO LEVEL");
        }
        if (level.toInt() <= Level.WARN.toInt()) {
            ret.add("warn");
            ret.add("warn arg");
            ret.add("warn arg1 arg2");
            ret.add("warn 1 2 3 WARN LEVEL");
            ret.add("warn WARN LEVEL");
        }
        ret.add("error");
        ret.add("error arg");
        ret.add("error arg1 arg2");
        ret.add("error 1 2 3 ERROR LEVEL");
        ret.add("error ERROR LEVEL");

        return ret;
    }

    private static void setLogLevelOfDelegate(Level level, ExtendedLogger logger) {
        doReturn(false).when(logger).isTraceEnabled();
        doReturn(false).when(logger).isDebugEnabled();
        doReturn(false).when(logger).isInfoEnabled();
        doReturn(false).when(logger).isWarnEnabled();
        doReturn(false).when(logger).isErrorEnabled();
        switch (level) {
            case TRACE:
                doReturn(true).when(logger).isTraceEnabled();
            case DEBUG:
                doReturn(true).when(logger).isDebugEnabled();
            case INFO:
                doReturn(true).when(logger).isInfoEnabled();
            case WARN:
                doReturn(true).when(logger).isWarnEnabled();
            case ERROR:
                doReturn(true).when(logger).isErrorEnabled();
        }
    }
}
