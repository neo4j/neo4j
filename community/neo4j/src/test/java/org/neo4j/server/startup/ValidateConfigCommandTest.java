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
package org.neo4j.server.startup;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import java.util.function.Supplier;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.cli.CommandFailedException;
import org.neo4j.configuration.Config;
import org.neo4j.server.startup.validation.ConfigValidationHelper;
import org.neo4j.server.startup.validation.ConfigValidationSummary;

class ValidateConfigCommandTest {
    private final ByteArrayOutputStream output = new ByteArrayOutputStream();
    private final Bootloader.Dbms bootloader = mock(Bootloader.Dbms.class);
    private final EnhancedExecutionContext context = mock(EnhancedExecutionContext.class);

    private final TestConfigValidationHelper helper = new TestConfigValidationHelper();
    private final ValidateConfigCommand command = new ValidateConfigCommand(context, helper);

    @BeforeEach
    void setup() {
        when(context.out()).thenReturn(new PrintStream(output));
        when(context.createDbmsBootloader()).thenReturn(bootloader);
    }

    @Test
    void shouldPrintValidationSummary() throws IOException {
        command.execute();
        assertThat(output.toString())
                .containsSubsequence(
                        TestConfigValidationHelper.SUMMARY_STRING, TestConfigValidationHelper.CLOSING_STATEMENT_STRING);
    }

    @Test
    void shouldThrowIfErrors() {
        helper.result = ConfigValidationSummary.ValidationResult.ERRORS;
        assertThatThrownBy(command::execute)
                .isInstanceOf(CommandFailedException.class)
                .hasMessage("Configuration contains errors.");
    }

    private static class TestConfigValidationHelper extends ConfigValidationHelper {
        public static final String SUMMARY_STRING = "Summary.";
        public static final String CLOSING_STATEMENT_STRING = "Closing statement.";

        public ConfigValidationSummary.ValidationResult result = ConfigValidationSummary.ValidationResult.OK;

        public TestConfigValidationHelper() {
            super(Path.of("neo4j.conf"));
        }

        public class TestConfigValidationSummary extends ConfigValidationSummary {
            @Override
            public void print(PrintStream out, boolean verbose) {
                out.println(SUMMARY_STRING);
            }

            @Override
            public void printClosingStatement(PrintStream out) {
                out.println(CLOSING_STATEMENT_STRING);
            }

            @Override
            public ConfigValidationSummary.ValidationResult result() {
                return result;
            }
        }

        @Override
        public ConfigValidationSummary validateAll(Supplier<Config> config) {
            return new TestConfigValidationSummary();
        }
    }
}
