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

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.logging.LogAssertions.assertThat;

import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.time.Clocks;
import org.neo4j.time.FakeClock;

public class CappedLoggerTest {

    public interface LogMethod {
        void log(CappedLogger logger, String msg);

        void log(CappedLogger logger, String msg, Throwable cause);
    }

    private static Stream<Arguments> argumentsProvider() {
        LogMethod debug = new LogMethod() {
            @Override
            public void log(CappedLogger logger, String msg) {
                logger.debug(msg);
            }

            @Override
            public void log(CappedLogger logger, String msg, Throwable cause) {
                logger.debug(msg, cause);
            }
        };
        LogMethod info = new LogMethod() {
            @Override
            public void log(CappedLogger logger, String msg) {
                logger.info(msg);
            }

            @Override
            public void log(CappedLogger logger, String msg, Throwable cause) {
                logger.info(msg, cause);
            }
        };
        LogMethod warn = new LogMethod() {
            @Override
            public void log(CappedLogger logger, String msg) {
                logger.warn(msg);
            }

            @Override
            public void log(CappedLogger logger, String msg, Throwable cause) {
                logger.warn(msg, cause);
            }
        };
        LogMethod error = new LogMethod() {
            @Override
            public void log(CappedLogger logger, String msg) {
                logger.error(msg);
            }

            @Override
            public void log(CappedLogger logger, String msg, Throwable cause) {
                logger.error(msg, cause);
            }
        };
        return Stream.of(
                Arguments.of(debug, "debug"),
                Arguments.of(info, "info"),
                Arguments.of(warn, "warn"),
                Arguments.of(error, "error"));
    }

    private AssertableLogProvider logProvider;
    private CappedLogger logger;

    @BeforeEach
    public void setUp() {
        logProvider = new AssertableLogProvider();
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("argumentsProvider")
    public void mustLogExceptions(LogMethod logMethod, String name) {
        logger = new CappedLogger(
                logProvider.getLog(CappedLogger.class), 1, TimeUnit.MILLISECONDS, Clocks.systemClock());
        var exception = new ArithmeticException("EXCEPTION");
        logMethod.log(logger, "MESSAGE", exception);
        assertThat(logProvider).containsMessageWithException("MESSAGE", exception);
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("argumentsProvider")
    public void mustThrowOnZeroTimeLimit(LogMethod logMethod, String name) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CappedLogger(logProvider.getLog(CappedLogger.class), 0, MILLISECONDS, Clocks.systemClock()));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("argumentsProvider")
    public void mustThrowOnNegativeTimeLimit(LogMethod logMethod, String name) {
        assertThrows(
                IllegalArgumentException.class,
                () -> new CappedLogger(logProvider.getLog(CappedLogger.class), -1, MILLISECONDS, Clocks.systemClock()));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("argumentsProvider")
    public void mustNotLogMessagesWithinConfiguredTimeLimit(LogMethod logMethod, String name) {
        FakeClock clock = getDefaultFakeClock();
        logger = new CappedLogger(logProvider.getLog(CappedLogger.class), 1, TimeUnit.MILLISECONDS, clock);
        logMethod.log(logger, "### AAA ###");
        logMethod.log(logger, "### BBB ###");
        clock.forward(1, TimeUnit.MILLISECONDS);
        logMethod.log(logger, "### CCC ###");

        assertThat(logProvider).containsMessages("### AAA ###");
        assertThat(logProvider).forClass(CappedLogger.class).doesNotContainMessage("### BBB ###");
        assertThat(logProvider).containsMessages("### CCC ###");
    }

    private static FakeClock getDefaultFakeClock() {
        return Clocks.fakeClock(1000, TimeUnit.MILLISECONDS);
    }
}
