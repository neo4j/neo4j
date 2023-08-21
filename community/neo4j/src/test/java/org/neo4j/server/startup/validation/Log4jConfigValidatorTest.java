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
package org.neo4j.server.startup.validation;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import org.apache.logging.log4j.status.StatusLogger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.Config;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.SuppressOutput;
import org.neo4j.test.extension.SuppressOutputExtension;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.test.utils.TestDirectory;
import org.xml.sax.SAXParseException;

@ResourceLock(Resources.SYSTEM_OUT)
@ResourceLock(Resources.SYSTEM_ERR)
@TestDirectoryExtension
@ExtendWith(SuppressOutputExtension.class)
class Log4jConfigValidatorTest {
    @Inject
    private TestDirectory testDirectory;

    @Inject
    private SuppressOutput suppressedOutput;

    @Test
    void shouldReportAllErrors() throws IOException {
        // Log4j validation requires that a Neo4j config has been loaded
        var config = mock(Config.class);

        var validator = spy(new Log4jConfigValidator(() -> config, "test", testDirectory.file("test.xml")));

        var errors = List.of(new Exception("error 1"), new Exception("error 2"));
        doAnswer(invocation -> {
                    for (var error : errors) {
                        StatusLogger.getLogger().error("log message " + error.getMessage(), error);
                    }
                    return null;
                })
                .when(validator)
                .loadConfig(any(Path.class));

        var issues = validator.validate();

        assertThat(issues).zipSatisfy(errors, (issue, error) -> {
            assertThat(issue.isError()).isTrue();
            assertThat(issue.getMessage()).isEqualTo("Error: log message " + error.getMessage());
            assertThat(issue.getThrowable()).isSameAs(error);
        });
    }

    @Test
    void shouldUseThrowableMessageIfSAXParseException() throws IOException {
        // Log4j validation requires that a Neo4j config has been loaded
        var config = mock(Config.class);

        var validator = spy(new Log4jConfigValidator(() -> config, "test", testDirectory.file("test.xml")));

        var error = new SAXParseException("throwable message", null);
        doAnswer(invocation -> {
                    StatusLogger.getLogger().error("log message", error);
                    return null;
                })
                .when(validator)
                .loadConfig(any(Path.class));

        var issue = validator.validate().get(0);
        assertThat(issue.getMessage())
                .isEqualTo("Error at %d:%d: %s"
                        .formatted(error.getLineNumber(), error.getColumnNumber(), error.getMessage()));
        assertThat(issue.getThrowable()).isEqualTo(error);
    }

    @Test
    void shouldRestoreStateOnException() {
        // The validator silences stdout and stderr; make sure that they are restored even if an exception is thrown.
        // It also causes logging to StatusLogger; make sure that is cleared.
        var logger = StatusLogger.getLogger();
        logger.clear();

        // Log4j validation requires a valid Neo4j config
        var config = mock(Config.class);

        var validator = spy(new Log4jConfigValidator(() -> config, "test", testDirectory.file("test.xml")));

        var exception = new RuntimeException("error");
        doAnswer(invocation -> {
                    logger.error("test");
                    throw exception;
                })
                .when(validator)
                .loadConfig(any(Path.class));

        var stdoutBefore = System.out;
        var stderrBefore = System.err;
        assertThatThrownBy(validator::validate).isSameAs(exception);
        assertThat(System.out).isSameAs(stdoutBefore);
        assertThat(System.err).isSameAs(stderrBefore);

        assertThat(logger.getStatusData()).isEmpty();
        assertThat(suppressedOutput.getOutputVoice().isEmpty()).isTrue();
        assertThat(suppressedOutput.getErrorVoice().isEmpty()).isTrue();
    }

    private static String[] shouldFilterOutNonsenseErrors() {
        return Log4jConfigValidator.NONSENSE_ERRORS;
    }

    @ParameterizedTest
    @MethodSource
    void shouldFilterOutNonsenseErrors(String nonsenseError) throws IOException {
        // Log4j validation requires that a Neo4j config has been loaded
        var config = mock(Config.class);
        var logger = StatusLogger.getLogger();

        var validator = spy(new Log4jConfigValidator(() -> config, "test", testDirectory.file("test.xml")));

        doAnswer(invocation -> {
                    logger.error(nonsenseError);
                    return null;
                })
                .when(validator)
                .loadConfig(any(Path.class));

        var issues = validator.validate();

        assertThat(issues).isEmpty();
    }
}
