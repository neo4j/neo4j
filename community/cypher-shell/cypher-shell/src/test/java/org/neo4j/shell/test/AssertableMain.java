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
package org.neo4j.shell.test;

import static java.lang.System.lineSeparator;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.asList;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.allOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.shell.Main.EXIT_FAILURE;
import static org.neo4j.shell.Main.EXIT_SUCCESS;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.assertj.core.api.AbstractStringAssert;
import org.assertj.core.api.Condition;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.Environment;
import org.neo4j.shell.Main;
import org.neo4j.shell.ShellRunner;
import org.neo4j.shell.cli.CliArgHelper;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.terminal.TestSimplePrompt;

public class AssertableMain {
    private final int exitCode;
    private final ByteArrayOutputStream out;
    private final ByteArrayOutputStream err;
    private final CypherShell shell;

    public AssertableMain(int exitCode, ByteArrayOutputStream out, ByteArrayOutputStream err, CypherShell shell) {
        this.exitCode = exitCode;
        this.out = out;
        this.err = err;
        this.shell = shell;
    }

    private Supplier<String> failureSupplier(String description) {
        return () -> description + "\nError output:\n" + this.err.toString() + "\n" + "Output:\n" + this.out.toString()
                + "\n";
    }

    public AssertableMain assertOutputLines(String... expected) {
        final String expectedOutput =
                Arrays.stream(expected).map(line -> line + "\n").collect(joining());
        return assertThatOutput(new Condition<>(expectedOutput::equals, "Should equal expected"));
    }

    @SafeVarargs
    public final AssertableMain assertThatOutput(Condition<String>... conditions) {
        var output = out.toString(UTF_8).replace("\r\n", "\n");
        assertThat(output).satisfies(allOf(conditions));
        return this;
    }

    public AssertableMain assertSuccess(boolean isErrorOutputEmpty) {
        assertEquals(EXIT_SUCCESS, exitCode, failureSupplier("Unexpected exit code"));
        if (isErrorOutputEmpty) {
            final var errString = err.toString(UTF_8);
            if (!"".equals(errString)) {
                fail("Error output expected to be empty, but was:\n" + errString);
            }
        }
        return this;
    }

    public AssertableMain assertSuccessAndConnected(boolean isErrorOutputEmpty) {
        assertTrue(shell.isConnected(), "Shell is not connected");
        return assertSuccess(isErrorOutputEmpty);
    }

    public AssertableMain assertSuccessAndDisconnected(boolean isErrorOutputEmpty) {
        assertFalse(shell.isConnected(), "Shell is connected");
        return assertSuccess(isErrorOutputEmpty);
    }

    public AssertableMain assertSuccessAndConnected() {
        return assertSuccessAndConnected(true);
    }

    public AssertableMain assertSuccessAndDisconnected() {
        return assertSuccessAndDisconnected(true);
    }

    public AssertableMain assertSuccess() {
        return assertSuccess(true);
    }

    @SafeVarargs
    public final AssertableMain assertThatErrorOutput(Condition<String>... conditions) {
        var errorOutput = err.toString(UTF_8);
        assertThat(errorOutput).satisfies(allOf(conditions));
        return this;
    }

    public final AssertableMain assertThatErrorOutput(Consumer<AbstractStringAssert<?>> f) {
        var errorOutput = err.toString(UTF_8);
        f.accept(assertThat(errorOutput));
        return this;
    }

    public AssertableMain assertFailure(String... expectedErrorOutput) {
        assertEquals(EXIT_FAILURE, exitCode, failureSupplier("Unexpected exit code"));
        if (expectedErrorOutput.length > 0) {
            assertEquals(
                    stream(expectedErrorOutput).map(l -> l + lineSeparator()).collect(joining()),
                    err.toString(UTF_8),
                    "Unexpected error ouput");
        }
        return this;
    }

    public ByteArrayOutputStream getOutput() {
        return out;
    }

    public static class AssertableMainBuilder {
        public ByteArrayInputStream in = new ByteArrayInputStream(new byte[0]);
        public List<String> args = new ArrayList<>();
        public Boolean isOutputInteractive;
        public CypherShell shell;
        public ShellRunner.Factory runnerFactory;
        public final ByteArrayOutputStream out = new ByteArrayOutputStream();
        public final ByteArrayOutputStream err = new ByteArrayOutputStream();
        public Path historyFile;
        public ParameterService parameters;
        private final Map<String, String> envMap = new HashMap<>();
        private final Environment environment = new Environment(envMap);

        public AssertableMainBuilder shell(CypherShell shell) {
            this.shell = shell;
            return this;
        }

        public AssertableMainBuilder runnerFactory(ShellRunner.Factory factory) {
            this.runnerFactory = factory;
            return this;
        }

        public AssertableMainBuilder args(String whiteSpaceSeparatedArgs) {
            this.args = stream(whiteSpaceSeparatedArgs.split("\\s+")).collect(Collectors.toList());
            return this;
        }

        public AssertableMainBuilder addArgs(String... args) {
            this.args.addAll(asList(args));
            return this;
        }

        public AssertableMainBuilder outputInteractive(boolean isOutputInteractive) {
            this.isOutputInteractive = isOutputInteractive;
            return this;
        }

        public AssertableMainBuilder userInputLines(String... input) {
            this.in = new ByteArrayInputStream((stream(input).map(l -> l + "\n").collect(joining())).getBytes());
            return this;
        }

        public AssertableMainBuilder userInput(String input) {
            this.in = new ByteArrayInputStream(input.getBytes());
            return this;
        }

        public AssertableMainBuilder parameters(ParameterService parameters) {
            this.parameters = parameters;
            return this;
        }

        public AssertableMainBuilder addEnvVariable(String key, String value) {
            envMap.put(key, value);
            return this;
        }

        public AssertableMain run() throws ArgumentParserException, IOException {
            return run(false);
        }

        public AssertableMain run(boolean closeMain) throws ArgumentParserException, IOException {
            var outPrintStream = new PrintStream(out);
            var errPrintStream = new PrintStream(err);
            var args = parseArgs();
            var logger = new AnsiPrinter(Format.VERBOSE, outPrintStream, errPrintStream);

            var terminal = terminalBuilder()
                    .dumb()
                    .streams(in, outPrintStream)
                    .simplePromptSupplier(() -> new TestSimplePrompt(in, new PrintWriter(out)))
                    .interactive(!args.getNonInteractive())
                    .logger(logger)
                    .build();
            var main = new Main(args, logger, shell, parameters, isOutputInteractive, runnerFactory, terminal);
            var exitCode = main.startShell();

            if (closeMain) {
                main.close();
            }

            return new AssertableMain(exitCode, out, err, shell);
        }

        protected CliArgs parseArgs() throws ArgumentParserException {
            return new CliArgHelper(environment).parseAndThrow(args.toArray(String[]::new));
        }
    }
}
