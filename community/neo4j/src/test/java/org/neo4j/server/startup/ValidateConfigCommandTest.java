/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.ERRORS;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.OK;
import static org.neo4j.server.startup.ValidateConfigCommand.ValidationResult.WARNINGS;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.server.startup.validation.ConfigValidationIssue;
import org.neo4j.server.startup.validation.ConfigValidator;

class ValidateConfigCommandTest {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final ByteArrayOutputStream errorOutput = new ByteArrayOutputStream();
    private EnhancedExecutionContext ctx;
    private final TestConfigValidatorFactory factory = new TestConfigValidatorFactory();

    @BeforeEach
    void setup() {
        ctx = mock(EnhancedExecutionContext.class);
        when(ctx.createDbmsBootloader()).thenReturn(mock(Bootloader.Dbms.class));
        when(ctx.out()).thenReturn(new PrintStream(output));
        when(ctx.err()).thenReturn(new PrintStream(errorOutput));
    }

    @Test
    void shouldReportNoIssues() {
        var command = new ValidateConfigCommand(ctx, factory);
        assertThatCode(command::execute).doesNotThrowAnyException();

        var output = this.output.toString();
        assertThat(output)
                .containsSubsequence(
                        "Validating Neo4j",
                        "No issues found.",
                        "Validating server Log4j",
                        "No issues found.",
                        "Validating user Log4j",
                        "No issues found.",
                        "Validation successful.");
        assertThat(errorOutput.size()).isEqualTo(0);
    }

    @Test
    void shouldNotFailOnWarnings() {
        factory.neo4jIssues.add(new ConfigValidationIssue(null, "warning 1", false, null));
        factory.log4jServerIssues.add(new ConfigValidationIssue(null, "warning 2", false, null));
        factory.log4jUserIssues.add(new ConfigValidationIssue(null, "warning 3", false, null));

        var command = new ValidateConfigCommand(ctx, factory);
        assertThatCode(command::execute).doesNotThrowAnyException();

        var output = this.output.toString();
        assertThat(output)
                .containsSubsequence(
                        "Validating Neo4j",
                        "1 issue found.",
                        "Warning: warning 1",
                        "Validating server Log4j",
                        "1 issue found.",
                        "Warning: warning 2",
                        "Validating user Log4j",
                        "1 issue found.",
                        "Warning: warning 3",
                        "Validation successful (with warnings).");
        assertThat(errorOutput.size()).isEqualTo(0);
    }

    @Test
    void shouldNotPerformLog4jValidationIfNeo4jValidationContainsErrors() {
        factory.neo4jIssues.add(new ConfigValidationIssue(null, "error", true, null));

        var command = new ValidateConfigCommand(ctx, factory);
        assertThatThrownBy(command::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Configuration contains errors");

        var output = this.output.toString();
        assertThat(output)
                .containsSubsequence(
                        "Validating Neo4j", "1 issue found.", "Error: error", "Skipping Log4j", "Validation failed.");
        assertThat(errorOutput.size()).isEqualTo(0);
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

        var command = new ValidateConfigCommand(ctx, factory);
        assertThatThrownBy(command::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessageContaining("Configuration contains errors");

        var output = this.output.toString();
        assertThat(output).containsSubsequence("Validating Neo4j", "Error: exception", "Validation failed.");
        assertThat(errorOutput.size()).isEqualTo(0);
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
        public ConfigValidator getNeo4jValidator(Bootloader bootloader) {
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
        public ConfigValidator getLog4jUserValidator(Bootloader bootloader) {
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
        public ConfigValidator getLog4jServerValidator(Bootloader bootloader) {
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
