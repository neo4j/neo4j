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
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.ERRORS;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.OK;
import static org.neo4j.server.startup.validation.ConfigValidationSummary.ValidationResult.WARNINGS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;

class ConfigValidationHelperTest {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final PrintStream out = new PrintStream(output);
    private final TestConfigValidatorFactory factory = new TestConfigValidatorFactory();

    @Test
    void shouldReportNoIssues() {
        var helper = new ConfigValidationHelper(factory);
        var summary = helper.validateAll(null);

        summary.print(out, false);
        summary.printClosingStatement(out);
        assertThat(output.toString())
                .isEqualToNormalizingNewlines(
                        """
                        Validating Neo4j
                        No issues found.

                        Validating user Log4j
                        No issues found.

                        Validating server Log4j
                        No issues found.

                        Configuration file validation successful.
                        """);
    }

    @Test
    void shouldNotFailOnWarnings() {
        factory.neo4jIssues.add(new ConfigValidationIssue(null, "warning 1", false, null));
        factory.log4jServerIssues.add(new ConfigValidationIssue(null, "warning 2", false, null));
        factory.log4jUserIssues.add(new ConfigValidationIssue(null, "warning 3", false, null));

        var helper = new ConfigValidationHelper(factory);
        var summary = helper.validateAll(null);

        summary.print(out, false);
        summary.printClosingStatement(out);
        assertThat(output.toString())
                .isEqualToNormalizingNewlines(
                        """
                        Validating Neo4j
                        1 issue found.
                        Warning: warning 1

                        Validating user Log4j
                        1 issue found.
                        Warning: warning 3

                        Validating server Log4j
                        1 issue found.
                        Warning: warning 2

                        Configuration file validation successful (with warnings).
                        """);
    }

    @Test
    void shouldNotPerformLog4jValidationIfNeo4jValidationContainsErrors() {
        factory.neo4jIssues.add(new ConfigValidationIssue(null, "error", true, null));

        var helper = new ConfigValidationHelper(factory);
        var summary = helper.validateAll(null);
        summary.print(out, false);
        summary.printClosingStatement(out);

        assertThat(output.toString())
                .isEqualToNormalizingNewlines(
                        """
                                Validating Neo4j
                                1 issue found.
                                Error: error

                                Skipping Log4j validation due to previous issues.

                                Configuration file validation failed.
                                """);
    }

    @Test
    void shouldHandleIOException() {
        var factory = spy(this.factory);
        when(factory.getNeo4jValidator(any())).thenReturn(new ConfigValidator() {
            @Override
            public List<ConfigValidationIssue> validate() throws IOException {
                throw new IOException("exception");
            }

            @Override
            public String getLabel() {
                return "Neo4j";
            }
        });

        var helper = new ConfigValidationHelper(factory);
        var summary = helper.validateAll(null);
        summary.print(out, false);
        summary.printClosingStatement(out);
        assertThat(output.toString())
                .isEqualToNormalizingNewlines(
                        """
                Error when validating Neo4j: exception

                Skipping Log4j validation due to previous issues.

                Configuration file validation failed.
                """);
    }

    @Test
    void shouldCombineResultsCorrectly() {
        assertThat(OK.and(OK)).isEqualTo(OK);
        assertThat(OK.and(WARNINGS)).isEqualTo(WARNINGS);
        assertThat(OK.and(ERRORS)).isEqualTo(ERRORS);
        assertThat(WARNINGS.and(OK)).isEqualTo(WARNINGS);
        assertThat(WARNINGS.and(WARNINGS)).isEqualTo(WARNINGS);
        assertThat(WARNINGS.and(ERRORS)).isEqualTo(ERRORS);
        assertThat(ERRORS.and(OK)).isEqualTo(ERRORS);
        assertThat(ERRORS.and(WARNINGS)).isEqualTo(ERRORS);
        assertThat(ERRORS.and(ERRORS)).isEqualTo(ERRORS);
    }

    private static class TestConfigValidatorFactory implements ConfigValidator.Factory {
        public List<ConfigValidationIssue> neo4jIssues = new ArrayList<>();
        public List<ConfigValidationIssue> log4jUserIssues = new ArrayList<>();
        public List<ConfigValidationIssue> log4jServerIssues = new ArrayList<>();

        @Override
        public ConfigValidator getNeo4jValidator(Supplier<Config> config) {
            return new ConfigValidator() {
                @Override
                public List<ConfigValidationIssue> validate() throws IOException {
                    return neo4jIssues;
                }

                @Override
                public String getLabel() {
                    return "Neo4j";
                }
            };
        }

        @Override
        public ConfigValidator getLog4jUserValidator(Supplier<Config> config) {
            return new ConfigValidator() {
                @Override
                public List<ConfigValidationIssue> validate() throws IOException {
                    return log4jUserIssues;
                }

                @Override
                public String getLabel() {
                    return "user Log4j";
                }
            };
        }

        @Override
        public ConfigValidator getLog4jServerValidator(Supplier<Config> config) {
            return new ConfigValidator() {
                @Override
                public List<ConfigValidationIssue> validate() throws IOException {
                    return log4jServerIssues;
                }

                @Override
                public String getLabel() {
                    return "server Log4j";
                }
            };
        }
    }
}
