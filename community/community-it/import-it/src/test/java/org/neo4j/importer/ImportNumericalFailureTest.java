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

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.cli.CommandTestUtils.withSuppressedOutput;
import static org.neo4j.importer.ImportCommandTest.assertExceptionContains;

import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.params.ParameterizedTest;
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

    static List<String[]> parameters() {
        List<String[]> params = new ArrayList<>();

        for (String type : Arrays.asList("int", "long", "short", "byte", "float", "double")) {
            for (String val : Arrays.asList(
                    " 1 7 ", " -1 7 ", " - 1 ", "   ", "   -  ", "-", "1. 0", "1 .", ".", "1E 10", " . 1")) {
                // Only include decimals for floating point
                if (val.contains(".") && !(type.equals("float") || type.equals("double"))) {
                    continue;
                }

                final String error;
                if (type.equals("float") || type.equals("double")) {
                    error = "Not a number: \"" + val + "\"";
                } else {
                    error = "Not an integer: \"" + val + "\"";
                }

                String[] args = new String[3];
                args[0] = type;
                args[1] = val;
                args[2] = error;

                params.add(args);
            }
        }
        return params;
    }

    @ParameterizedTest
    @MethodSource(value = "parameters")
    void failImportOnInvalidData(String type, String val, String expectedError) throws Exception {

        Path data = file(databaseLayout, fileName("whitespace.csv"));
        try (PrintStream writer = new PrintStream(Files.newOutputStream(data))) {
            writer.println(":LABEL,adult:" + type);
            writer.println("PERSON," + val);
        }

        Exception exception = assertThrows(
                Exception.class,
                () -> runImport(
                        databaseLayout.databaseDirectory().toAbsolutePath(),
                        "--quote",
                        "'",
                        "--nodes",
                        data.toAbsolutePath().toString()));
        assertExceptionContains(exception, expectedError, InputException.class);
    }

    private static String fileName(String name) {
        return name;
    }

    private static Path file(DatabaseLayout databaseLayout, String localname) {
        return databaseLayout.file(localname);
    }

    private static void runImport(Path homeDir, String... arguments) {
        withSuppressedOutput(homeDir, homeDir.resolve("conf"), new DefaultFileSystemAbstraction(), ctx -> {
            final var cmd = new ImportCommand.Full(ctx);
            CommandLine.populateCommand(cmd, arguments);
            cmd.execute();
        });
    }
}
