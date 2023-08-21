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

import static java.util.concurrent.TimeUnit.MINUTES;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.startup.Bootloader.EXIT_CODE_OK;
import static org.neo4j.test.assertion.Assert.assertEventually;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Optional;
import java.util.concurrent.Future;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.test.OtherThreadExecutor;
import picocli.CommandLine;

public class ServerConsoleCommandIT extends ServerProcessTestBase {

    @Override
    protected CommandLine createCommand(
            PrintStream out,
            PrintStream err,
            Function<String, String> envLookup,
            Function<String, String> propLookup,
            Runtime.Version version) {
        var environment = new Environment(out, err, envLookup, propLookup, version);
        return Neo4jCommand.asCommandLine(new Neo4jCommand(environment), environment);
    }

    @DisabledOnOs(OS.WINDOWS)
    @Test
    void consoleShouldWriteToBothFileAndSystemOut() throws Exception {
        // Since the "console" command will run in a different JVM, that will inherit the stdout and stderr from the
        // parent process, we can not use the SuppressOutputExtension
        ByteArrayOutputStream consoleOutput = new ByteArrayOutputStream();
        TestInFork testInFork = new TestInFork(consoleOutput, consoleOutput);
        if (testInFork.run(() -> {
            // We need to execute in a different thread in order to simulate the user hitting Ctrl+C
            try (OtherThreadExecutor other = new OtherThreadExecutor("test")) {
                Future<Integer> console = other.executeDontWait(() -> execute("console"));

                // Wait for the content to reach the log file
                assertEventually(this::getUserLogLines, s -> s.contains("Remote interface available at"), 5, MINUTES);

                // Interrupt and check that we exit in a graceful fashion
                Optional<ProcessHandle> process = getProcess();
                assertThat(process.isPresent()).isTrue();
                process.get().destroy();
                int exitCode = console.get();
                assertThat(exitCode).isEqualTo(EXIT_CODE_OK);
            }
        })) {
            assertThat(consoleOutput.toString()).contains("Remote interface available at");
        }
    }
}
