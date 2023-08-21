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
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;

import java.nio.file.Path;
import java.util.LinkedList;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.Neo4jMessageSupplier;

class Neo4jConfigValidatorTest {
    @Test
    void shouldReportAllWarnings() {
        var warnings = List.of(new Exception("warning1"), new Exception("warning2"));

        var config = spy(Config.defaults());
        doAnswer(invocation -> {
                    var log = (InternalLog) invocation.getArgument(0);
                    for (var warning : warnings) {
                        log.warn(warning.getMessage(), warning);
                    }
                    return null;
                })
                .when(config)
                .setLogger(any(InternalLog.class));

        var validator = new Neo4jConfigValidator(() -> config, Path.of("neo4j.conf"));
        var issues = validator.validate();

        assertThat(issues).zipSatisfy(warnings, (issue, warning) -> {
            assertThat(issue.isError()).isFalse();
            assertThat(issue.getMessage()).isEqualTo("Warning: " + warning.getMessage());
            assertThat(issue.getThrowable()).isSameAs(warning);
        });
    }

    @Test
    void shouldReportErrorWithMessageFromCause() {
        var cause = new IllegalArgumentException("error");
        var exception = new CommandFailedException("ignore this", cause);

        var validator = new Neo4jConfigValidator(
                () -> {
                    throw exception;
                },
                Path.of("neo4j.conf"));
        var issues = validator.validate();

        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.isError()).isTrue();
            assertThat(issue.getMessage()).isEqualTo("Error: %s", cause.getMessage());
            assertThat(issue.getThrowable()).isSameAs(exception);
        });
    }

    @Test
    void allOverloadsOfWarnShouldEndUpInIssueList() {
        List<ConfigValidationIssue> issues = new LinkedList<>();
        var logger = new Neo4jConfigValidator.IssueCollectingLogger(issues);

        String testMessage = "test";
        String testFormat = "format %s string";
        String testFormattedString = testFormat.formatted(testMessage);
        Throwable testThrowable = new Throwable();

        logger.warn(testMessage);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Warning: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.warn(testFormat, testMessage);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Warning: " + testFormattedString);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.warn(testMessage, testThrowable);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Warning: " + testMessage);
            assertThat(issue.getThrowable()).isSameAs(testThrowable);
        });
        issues.clear();

        logger.warn(Neo4jMessageSupplier.forMessage(testMessage));
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Warning: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.warn(() -> Neo4jMessageSupplier.forMessage(testMessage));
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Warning: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();
    }

    @Test
    void allOverloadsOfErrorShouldEndUpInIssueList() {
        List<ConfigValidationIssue> issues = new LinkedList<>();
        var logger = new Neo4jConfigValidator.IssueCollectingLogger(issues);

        String testMessage = "test";
        String testFormat = "format %s string";
        String testFormattedString = testFormat.formatted(testMessage);
        Throwable testThrowable = new Throwable();

        logger.error(testMessage);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.error(testFormat, testMessage);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testFormattedString);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.error(testMessage, testThrowable);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testMessage);
            assertThat(issue.getThrowable()).isSameAs(testThrowable);
        });
        issues.clear();

        logger.error(Neo4jMessageSupplier.forMessage(testMessage));
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();

        logger.error(Neo4jMessageSupplier.forMessage(testMessage), testThrowable);
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testMessage);
            assertThat(issue.getThrowable()).isSameAs(testThrowable);
        });
        issues.clear();

        logger.error(() -> Neo4jMessageSupplier.forMessage(testMessage));
        assertThat(issues).hasSize(1).first().satisfies(issue -> {
            assertThat(issue.getMessage()).isEqualTo("Error: " + testMessage);
            assertThat(issue.getThrowable()).isNull();
        });
        issues.clear();
    }
}
