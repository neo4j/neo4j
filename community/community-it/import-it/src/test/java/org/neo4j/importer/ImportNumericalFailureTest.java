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
package org.neo4j.importer;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.params.provider.Arguments.arguments;
import static org.neo4j.cli.CommandTestUtils.withSuppressedOutput;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.internal.batchimport.input.InputException;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import picocli.CommandLine;

@Neo4jLayoutExtension
class ImportNumericalFailureTest {
    @Inject
    private DatabaseLayout databaseLayout;

    static List<Arguments> parameters() {
        List<Arguments> params = new ArrayList<>();
        for (String type : Arrays.asList("int", "long", "short", "byte", "float", "double")) {
            for (String val : Arrays.asList(
                    " 1 7 ", " -1 7 ", " - 1 ", "   ", "   -  ", "-", "1. 0", "1 .", ".", "1E 10", " . 1")) {
                // Only include decimals for floating point
                if (val.contains(".") && !(type.equals("float") || type.equals("double"))) {
                    continue;
                }

                final String error = (type.equals("float") || type.equals("double"))
                        ? "Not a number: \"" + val + "\""
                        : "Not an integer: \"" + val + "\"";

                params.add(arguments(type, val, error));
            }
        }
        return params;
    }

    @ParameterizedTest
    @MethodSource(value = "parameters")
    void failImportOnInvalidData(String type, String val, String expectedError) throws Exception {

        Path data = file(databaseLayout, "whitespace.csv");
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.println(":LABEL,adult:" + type);
            writer.println("PERSON," + val);
        }

        assertThatThrownBy(() -> runImport(
                        databaseLayout.databaseDirectory().toAbsolutePath(),
                        "--report-file",
                        databaseLayout.path("import.report").toAbsolutePath().toString(),
                        "--quote",
                        "'",
                        "--nodes",
                        data.toAbsolutePath().toString()))
                .satisfies(e -> assertExceptionContains(e, expectedError, InputException.class));
    }

    private static Path file(DatabaseLayout databaseLayout, String name) {
        return databaseLayout.path(name);
    }

    private static void runImport(Path homeDir, String... arguments) {
        withSuppressedOutput(homeDir, homeDir.resolve("conf"), new DefaultFileSystemAbstraction(), ctx -> {
            final var cmd = new ImportCommand.Full(ctx);
            CommandLine.populateCommand(cmd, arguments);
            cmd.execute();
        });
    }

    static void assertExceptionContains(Throwable e, String message, Class<? extends Exception> type) {
        Throwable current = e;
        boolean found = false;
        while (current != null) {
            if (type.isInstance(current)
                    && current.getMessage() != null
                    && current.getMessage().contains(message)) {
                found = true;
                break;
            }
            current = current.getCause();
        }
        assertThat(found)
                .as("Expected exception chain to contain %s with message containing '%s'", type, message)
                .isTrue();
    }
}
