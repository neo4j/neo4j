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
package org.neo4j.shell;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.function.Supplier;
import org.apache.commons.io.IOUtils;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.hamcrest.Matcher;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

public class BinaryNonInteractiveIntegrationTest {
    private static final String USER = "neo4j";
    private static final String PASSWORD = "neo";

    @TempDir
    Path tempDir;

    private File cypherShellBinary = cypherShellBinary();

    @Test
    void simpleReturn() {
        assertOutput(defaultArgsWith(), "return 1 as result;", equalTo("result\n1\n"));
    }

    @Test
    void simpleReturnVerbose() {
        assertOutput(
                defaultArgsWith("--format", "verbose"),
                "return 1 as result;",
                startsWith(
                        """
                +--------+
                | result |
                +--------+
                | 1      |
                +--------+

                1 row
                ready to start consuming query after"""));
    }

    private static MutableList<String> defaultArgsWith(String... args) {
        return Lists.mutable
                .with("-u", USER, "-p", PASSWORD, "--non-interactive")
                .withAll(Arrays.asList(args));
    }

    private void assertOutput(List<String> args, String input, Matcher<String> expectedOutput) {
        final var command =
                Lists.mutable.with(cypherShellBinary.getAbsolutePath()).withAll(args);
        final var result = execute(command, input);

        assertEquals(0, result.exitCode(), failureMessage(result));
        assertThat(result.out(), expectedOutput);
        assertEquals("", result.err());
    }

    private Supplier<String> failureMessage(ProcessExecution result) {
        return () -> "\nFailure executing: " + String.join(" ", result.command()) + "\n" + "Exit code:"
                + result.exitCode() + "\n" + "Output:\n"
                + result.out()
                + "\n\n" + "Error output:\n"
                + result.err()
                + "\n\n";
    }

    private ProcessExecution execute(List<String> command, String input) {
        Path inFile = tempDir.resolve("input.txt");

        Process process = null;
        try {
            Files.write(inFile, input.getBytes(), StandardOpenOption.CREATE_NEW);

            process = new ProcessBuilder(command).redirectInput(inFile.toFile()).start();

            final var out = IOUtils.toString(process.getInputStream(), Charset.defaultCharset());
            final var err = IOUtils.toString(process.getErrorStream(), Charset.defaultCharset());

            int exitCode = process.waitFor();

            return new ProcessExecution(command, out, err, exitCode);
        } catch (Exception e) {
            if (process != null) {
                process.destroy();
            }
            throw new RuntimeException("Failed to run: " + String.join(" ", command), e);
        }
    }

    public static File cypherShellBinary() {
        // TODO Test using the binary extracted from the zip distribution instead
        // TODO Also test native binaries
        if (System.getProperty("os.name").startsWith("Windows")) {
            return new File("../cypher-shell/target/assembly/bin/cypher-shell.bat");
        }
        return new File("../cypher-shell/target/assembly/bin/cypher-shell");
    }
}

record ProcessExecution(List<String> command, String out, String err, int exitCode) {}
