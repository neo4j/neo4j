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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.logging.log4j.LogConfigTest.DATE_PATTERN;

import java.util.regex.Pattern;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.logging.Level;

class Log4jLogTest extends Log4jLogTestBase {
    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldWriteMessage(LogMethod logMethod, Level level) {
        logMethod.log(log, "my message");

        assertThat(outContent.toString()).matches(format(DATE_PATTERN + " %-5s \\[className\\] my message%n", level));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldWriteMessageAndThrowable(LogMethod logMethod, Level level) {
        Throwable throwable = newThrowable("stacktrace");
        logMethod.log(log, "my message", throwable);
        String throwableName = throwable.getClass().getName();

        assertThat(outContent.toString())
                .matches(format(
                        DATE_PATTERN + " %-5s \\[className\\] my message%n" + Pattern.quote(throwableName)
                                + ": stacktrace%n",
                        level));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldWriteMessageWithFormat(LogMethod logMethod, Level level) {
        logMethod.log(log, "my %s message %d", "long", 1);

        assertThat(outContent.toString())
                .matches(format(DATE_PATTERN + " %-5s \\[className\\] my long message 1%n", level));
    }

    @ParameterizedTest(name = "{1}")
    @MethodSource("logMethods")
    void shouldNotLogAnythingOnNone(LogMethod logMethod, Level level) {
        try (Neo4jLoggerContext context =
                LogConfig.createBuilderToOutputStream(outContent, Level.NONE).build()) {
            Log4jLog log = new Log4jLog(context.getLogger("className"));
            logMethod.log(log, "my message");

            assertThat(outContent.toString()).isEmpty();
        }
    }
}
