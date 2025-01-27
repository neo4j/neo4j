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
package org.neo4j.shell;

import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.shell.ShellRunner.shouldBeInteractive;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.io.IOException;
import java.io.PrintStream;
import java.io.PrintWriter;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.shell.cli.AccessMode;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.completions.CompletionEngine;
import org.neo4j.shell.completions.DbInfo;
import org.neo4j.shell.completions.DbInfoImpl;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.CypherShellTerminal;
import org.neo4j.shell.terminal.TestSimplePrompt;
import org.neo4j.shell.test.AssertableMain;
import org.neo4j.shell.util.Version;
import org.neo4j.shell.util.Versions;

public class TestHarness {
    public static final String USER = "neo4j";
    public static final String PASSWORD = "neo";

    protected final Version serverVersion;
    protected final Version protocolVersion;

    TestHarness() {
        try {
            serverVersion = Versions.version(runInDbAndReturn("", CypherShell::getServerVersion));
            protocolVersion = Versions.version(runInDbAndReturn("", CypherShell::getProtocolVersion));
        } catch (Versions.FailedToParseException e) {
            throw new RuntimeException(e);
        }
    }

    AssertableMain.AssertableMainBuilder buildTest() {
        return new TestBuilder().outputInteractive(true);
    }

    static class TestBuilder extends AssertableMain.AssertableMainBuilder {
        public DbInfo dbInfo;
        public CypherShellTerminal terminal;
        private BoltStateHandler boltStateHandler;
        private ParameterService parameters;
        private Main main;
        private boolean hardcodeTerminalInput = true;

        public TestBuilder() {}

        // Use this test builder if you want to have more control on
        // some of the structures we prefill from the outside
        // (i.e. the db info for example). Also if you want the terminal
        // to not close, you can pass closeTerminal = false, run it
        // in a separate thread and use the public terminal variable
        public TestBuilder(
                ParameterService parameterService,
                BoltStateHandler boltStateHandler,
                DbInfoImpl dbInfo,
                boolean isOutputInteractive,
                boolean closeTerminal) {
            this.parameters = parameterService;
            this.boltStateHandler = boltStateHandler;
            this.dbInfo = dbInfo;
            this.isOutputInteractive = isOutputInteractive;
            this.hardcodeTerminalInput = closeTerminal;
        }

        public void closeMain() {
            this.main.close();
        }

        @Override
        public AssertableMain run(boolean closeMain) throws ArgumentParserException, IOException {
            assertNull(runnerFactory);
            assertNull(shell);
            var args = parseArgs();
            var outPrintStream = new PrintStream(out);
            var errPrintStream = new PrintStream(err);
            var logger = new AnsiPrinter(Format.VERBOSE, outPrintStream, errPrintStream);
            if (this.boltStateHandler == null) {
                this.boltStateHandler =
                        new BoltStateHandler(shouldBeInteractive(args, isOutputInteractive), args.getAccessMode());
            }
            if (this.parameters == null) {
                this.parameters = ParameterService.create(boltStateHandler);
            }
            var completionsEnabledByConfig = args.getEnableAutocompletions();
            if (this.dbInfo == null) {
                this.dbInfo = new DbInfoImpl(parameters, boltStateHandler, completionsEnabledByConfig);
            }
            var completionEngine = new CompletionEngine(dbInfo);

            var terminalBuilder = terminalBuilder()
                    .dumb()
                    .simplePromptSupplier(() -> new TestSimplePrompt(in, new PrintWriter(out)))
                    .interactive(!args.getNonInteractive())
                    .logger(logger);

            terminalBuilder = hardcodeTerminalInput ? terminalBuilder.streams(in, outPrintStream) : terminalBuilder;
            this.terminal = terminalBuilder.build(dbInfo, completionEngine);

            Logger.setupLogging(args);
            this.main = new Main(
                    args,
                    outPrintStream,
                    errPrintStream,
                    isOutputInteractive,
                    terminal,
                    boltStateHandler,
                    dbInfo,
                    parameters);
            var exitCode = main.startShell();

            if (closeMain) {
                closeMain();
            }

            return new AssertableMain(exitCode, out, err, main.getCypherShell());
        }
    }

    protected void assumeAtLeastVersion(String version) {
        try {
            assumeTrue(serverVersion.compareTo(Versions.version(version)) > 0);
        } catch (Versions.FailedToParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected void assumeVersionBefore(String version) {
        try {
            assumeTrue(serverVersion.compareTo(Versions.version(version)) < 0);
        } catch (Versions.FailedToParseException e) {
            throw new RuntimeException(e);
        }
    }

    protected void runInSystemDb(ThrowingConsumer<CypherShell, Exception> systemDbConsumer) {
        runInSystemDbAndReturn(shell -> {
            systemDbConsumer.accept(shell);
            return null;
        });
    }

    protected void runInDb(String database, ThrowingConsumer<CypherShell, Exception> consumer) {
        runInDbAndReturn(database, shell -> {
            consumer.accept(shell);
            return null;
        });
    }

    protected <T> T runInDbAndReturn(String database, ThrowingFunction<CypherShell, T, Exception> systemDbConsumer) {
        CypherShell shell = null;
        try {
            var boltHandler = new BoltStateHandler(false, AccessMode.WRITE);
            var printer = new PrettyPrinter(new PrettyConfig(Format.PLAIN, false, 100, false));
            var parameters = ParameterService.create(boltHandler);
            var dbInfo = new DbInfoImpl(parameters, boltHandler, true);
            shell = new CypherShell(new StringLinePrinter(), boltHandler, dbInfo, printer, parameters);
            shell.connect(testConnectionConfig("neo4j://localhost:7687")
                    .withUsernameAndPasswordAndDatabase(USER, PASSWORD, database));
            return systemDbConsumer.apply(shell);
        } catch (Exception e) {
            throw new RuntimeException("Failed to execute statements during test setup: " + e.getMessage(), e);
        } finally {
            if (shell != null) {
                shell.disconnect();
            }
        }
    }

    protected <T> T runInSystemDbAndReturn(ThrowingFunction<CypherShell, T, Exception> systemDbConsumer) {
        var systemDb = serverVersion.major() >= 4 ? "system" : ""; // Before version 4 we don't support multi databases
        return runInDbAndReturn(systemDb, systemDbConsumer);
    }
}
