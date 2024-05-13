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
package org.neo4j.shell.cli;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatCode;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.Main.EXIT_SUCCESS;
import static org.neo4j.shell.cli.InteractiveShellRunner.DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT;
import static org.neo4j.shell.terminal.CypherShellTerminalBuilder.terminalBuilder;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;
import org.fusesource.jansi.Ansi;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.shell.Connector;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.Historian;
import org.neo4j.shell.OfflineTestShell;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.UserMessagesHandler;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.CypherShellTerminal;

class InteractiveShellRunnerTest {
    @TempDir
    File temp;

    private Printer printer;
    private StatementExecuter cmdExecuter;
    private Path historyFile;
    private TransactionHandler txHandler;
    private DatabaseManager databaseManager;
    private ClientException badLineError;
    private Connector connector;
    private UserMessagesHandler userMessagesHandler;
    private ByteArrayOutputStream out;
    private ParameterService parameters;

    @BeforeEach
    void setup() throws Exception {
        Ansi.setEnabled(true);
        printer = mock(Printer.class);
        cmdExecuter = mock(StatementExecuter.class);
        txHandler = mock(TransactionHandler.class);
        databaseManager = mock(DatabaseManager.class);
        historyFile = new File(temp, "test").toPath();
        badLineError = new ClientException("Found a bad line");
        connector = mock(Connector.class);
        when(connector.isConnected()).thenReturn(true);
        when(connector.username()).thenReturn("myusername");
        when(connector.getProtocolVersion()).thenReturn("");
        final var connectionConfig = testConnectionConfig("neo4j://localhost:7687");
        when(connector.connectionConfig()).thenReturn(connectionConfig);
        userMessagesHandler = new UserMessagesHandler(connector);
        out = new ByteArrayOutputStream();
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn("mydb");
        parameters = mock(ParameterService.class);

        doThrow(badLineError).when(cmdExecuter).execute(statementContains("bad"));
    }

    @AfterEach
    void cleanup() {
        Ansi.setEnabled(Ansi.isDetected());
    }

    @Test
    void testSimple() throws Exception {
        var runner = runner(lines("good1;", "good2;"));
        runner.runUntilEnd();

        verify(cmdExecuter).execute(cypher("good1"));
        verify(cmdExecuter).execute(cypher("good2"));
        verify(cmdExecuter, times(3)).lastNeo4jErrorCode();
        verifyNoMoreInteractions(cmdExecuter);

        assertThat(out.toString())
                .isEqualTo("myusername@mydb> good1;\r\nmyusername@mydb> good2;\r\nmyusername@mydb> \r\n");
    }

    @Test
    void runUntilEndShouldKeepGoingOnErrors() throws CommandException {
        var runner = runner(lines("good1;", "bad1;", "good2;", "bad2;", "good3;"));
        int code = runner.runUntilEnd();

        assertThat(code).as("Wrong exit code").isEqualTo(EXIT_SUCCESS);

        verify(cmdExecuter).execute(cypher("good1"));
        verify(cmdExecuter).execute(cypher("bad1"));
        verify(cmdExecuter).execute(cypher("good2"));
        verify(cmdExecuter).execute(cypher("bad2"));
        verify(cmdExecuter).execute(cypher("good3"));
        verify(cmdExecuter, times(6)).lastNeo4jErrorCode();
        verifyNoMoreInteractions(cmdExecuter);

        verify(printer, times(2)).printError(badLineError);
    }

    @Test
    void runUntilEndShouldStopOnExitExceptionAndReturnCode() throws CommandException {
        var runner = runner(lines("good1;", "bad1;", "good2;", "exit;", "bad2;", "good3;"));
        final int expectedCode = 1234;

        doThrow(new ExitException(expectedCode)).when(cmdExecuter).execute(statementContains("exit"));

        int code = runner.runUntilEnd();

        assertThat(code).as("Wrong exit code").isEqualTo(expectedCode);

        verify(cmdExecuter).execute(cypher("good1"));
        verify(cmdExecuter).execute(cypher("bad1"));
        verify(cmdExecuter).execute(cypher("good2"));
        verify(cmdExecuter).execute(cypher("exit"));
        verify(cmdExecuter, times(4)).lastNeo4jErrorCode();
        verifyNoMoreInteractions(cmdExecuter);

        verify(printer).printError(badLineError);
    }

    @Test
    void historyIsRecorded() throws Exception {
        // given
        String[] commands = new String[] {":set var \"3\"", ":help exit"};
        var runner = runner(lines(commands));

        // when
        runner.runUntilEnd();

        // then
        Historian historian = runner.getHistorian();
        historian.flushHistory();

        List<String> history = Files.readAllLines(historyFile);

        assertThat(history).zipSatisfy(Arrays.asList(commands), (entry, cmd) -> assertThat(entry)
                .endsWith(":" + cmd));
        assertThat(historian.getHistory()).containsExactly(commands);
    }

    @Test
    void unescapedBangWorks() throws Exception {
        // Bangs need escaping in JLine by default, just like in bash, but we have disabled that
        var runner = runner(":set var \"String with !bang\"\n");

        // when
        var statements = runner.readUntilStatement();
        // then
        assertThat(statements)
                .contains(new CommandStatement(":set", List.of("var", "\"String", "with", "!bang\""), true, 0, 27));
    }

    @Test
    void escapedBangWorks() throws Exception {
        // Bangs need escaping in JLine by default, just like in bash, but we have disabled that
        var runner = runner(":set var \"String with \\!bang\"\n");

        // when
        var statements = runner.readUntilStatement();

        // then
        assertThat(statements)
                .contains(new CommandStatement(":set", List.of("var", "\"String", "with", "\\!bang\""), true, 0, 28));
    }

    @Test
    void justNewLineDoesNotThrowNoMoreInput() {
        // given
        var runner = runner("\n");

        // when
        assertThatCode(runner::readUntilStatement).doesNotThrowAnyException();
    }

    @Test
    void emptyStringThrowsNoMoreInput() {
        // given
        var runner = runner("");

        // when
        assertThatThrownBy(runner::readUntilStatement).isInstanceOf(NoMoreInputException.class);
    }

    @Test
    void emptyLineIsIgnored() throws Exception {
        // given
        var runner = runner("     \nCREATE (n:Person) RETURN n;\n");

        // when
        var statements1 = runner.readUntilStatement();
        var statements2 = runner.readUntilStatement();

        // then
        assertThat(statements1).isEmpty();
        assertThat(statements2).containsExactly(cypher("CREATE (n:Person) RETURN n"));
    }

    @Test
    void testPrompt() {
        // given
        var runner = runner(lines("    ", "   ", "bla bla;"));
        when(txHandler.isTransactionOpen()).thenReturn(false);
        runner.runUntilEnd();

        // when
        assertThat(out.toString())
                .isEqualTo(
                        "myusername@mydb>     \r\nmyusername@mydb>    \r\nmyusername@mydb> bla bla;\r\nmyusername@mydb> \r\n");
    }

    @Test
    void testDisconnectedPrompt() {
        // given
        var runner = runner(lines("bla bla;"));
        when(txHandler.isTransactionOpen()).thenReturn(false);
        when(connector.isConnected()).thenReturn(false);
        runner.runUntilEnd();

        // when
        assertThat(out.toString()).isEqualTo("Disconnected> bla bla;\r\nDisconnected> \r\n");
    }

    @Test
    void testPromptShowDatabaseAsSetByUserWhenServerReportNull() {
        // given
        var runner = runner("return 1;");

        // when
        when(txHandler.isTransactionOpen()).thenReturn(false);
        when(databaseManager.getActiveDatabaseAsSetByUser()).thenReturn("foo");
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn(null);
        runner.runUntilEnd();

        // then
        String wantedPrompt = "myusername@foo> return 1;\r\n";
        assertThat(out.toString()).isEqualTo(wantedPrompt);
    }

    @Test
    void testPromptShowDatabaseAsSetByUserWhenServerReportAbsent() {
        // given
        var runner = runner("return 1;");

        // when
        when(txHandler.isTransactionOpen()).thenReturn(false);
        when(databaseManager.getActiveDatabaseAsSetByUser()).thenReturn("foo");
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn(DatabaseManager.ABSENT_DB_NAME);
        runner.runUntilEnd();

        // then
        assertThat(out.toString()).isEqualTo("myusername@foo> return 1;\r\n");
    }

    @Test
    void testPromptShowUnresolvedDefaultDatabaseWhenServerReportNull() {
        // given
        var runner = runner("return 1;");

        // when
        when(txHandler.isTransactionOpen()).thenReturn(false);
        when(databaseManager.getActiveDatabaseAsSetByUser()).thenReturn(DatabaseManager.ABSENT_DB_NAME);
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn(null);
        runner.runUntilEnd();

        // then
        assertThat(out.toString()).isEqualTo("myusername@<default_database>> return 1;\r\n");
    }

    @Test
    void testPromptShowUnresolvedDefaultDatabaseWhenServerReportAbsent() {
        // given
        var runner = runner("return 1;");

        // when
        when(txHandler.isTransactionOpen()).thenReturn(false);
        when(databaseManager.getActiveDatabaseAsSetByUser()).thenReturn(DatabaseManager.ABSENT_DB_NAME);
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn(DatabaseManager.ABSENT_DB_NAME);
        runner.runUntilEnd();

        // then
        assertThat(out.toString()).isEqualTo("myusername@<default_database>> return 1;\r\n");
    }

    @Test
    void testLongPrompt() {
        // given
        String actualDbName = "TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome";
        when(databaseManager.getActualDatabaseAsReportedByServer()).thenReturn(actualDbName);
        var runner = runner(lines("match", "(n)", "where n.id = 1", "", ";", "return 1;"));

        // when
        when(txHandler.isTransactionOpen()).thenReturn(false);

        var exitCode = runner.runUntilEnd();

        assertThat(exitCode).isEqualTo(EXIT_SUCCESS);

        var expected = "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n" + "> match\n"
                + "  (n)\n"
                + "  where n.id = 1\n"
                + "  \n"
                + "  ;\n"
                + "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n"
                + "> return 1;\n"
                + "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n"
                + "> \n";
        assertThat(out.toString().replace("\r", "")).isEqualTo(expected);
    }

    @Test
    void testPromptInTx() {
        // given
        var runner = runner(lines("   ", "   ", "bla bla;"));
        when(txHandler.isTransactionOpen()).thenReturn(true);

        assertThat(runner.runUntilEnd()).isEqualTo(EXIT_SUCCESS);

        var expected = "myusername@mydb#    \r\n" + "myusername@mydb#    \r\n"
                + "myusername@mydb# bla bla;\r\n"
                + "myusername@mydb# \r\n";
        assertThat(out.toString()).isEqualTo(expected);
    }

    @Test
    void testImpersonationPrompt() {
        // given
        var runner = runner(lines("return 40;"));
        when(connector.impersonatedUser()).thenReturn(Optional.of("emil"));
        when(txHandler.isTransactionOpen()).thenReturn(false);
        runner.runUntilEnd();

        // when
        assertThat(out.toString()).isEqualTo("myusername(emil)@mydb> return 40;\r\nmyusername(emil)@mydb> \r\n");
    }

    @Test
    void multilineRequiresNewLineOrSemicolonToEnd() {
        // given
        var runner = runner("  \\   \nCREATE (n:Person) RETURN n\n");

        // when
        runner.runUntilEnd();

        // then
        verify(cmdExecuter).lastNeo4jErrorCode();
        verifyNoMoreInteractions(cmdExecuter);
    }

    @Test
    void printsWelcomeAndExitMessage() {
        // given
        var runner = runner("\nCREATE (n:Person) RETURN n\n;\n");

        // when
        runner.runUntilEnd();

        // then

        verify(printer)
                .printIfVerbose(
                        """
                            Connected to Neo4j at [1mneo4j://localhost:7687[22m as user [1mmyusername[22m.
                            Type [1m:help[22m for a list of available commands or [1m:exit[22m to exit the shell.
                            Note that Cypher queries must end with a [1msemicolon.[22;0m""");
        verify(printer).printIfVerbose("\nBye!");
    }

    @Test
    void multilineEndsOnSemicolonOnNewLine() throws Exception {
        // given
        var runner = runner("\nCREATE (n:Person) RETURN n\n;\n");

        // when
        runner.runUntilEnd();

        // then
        verify(cmdExecuter).execute(cypher("CREATE (n:Person) RETURN n\n"));
    }

    @Test
    void multilineEndsOnSemicolonOnSameLine() throws Exception {
        // given
        var runner = runner("\nCREATE (n:Person) RETURN n;\n");

        // when
        runner.runUntilEnd();

        // then
        verify(cmdExecuter).execute(cypher("CREATE (n:Person) RETURN n"));
    }

    @Test
    void testSignalHandleOutsideExecution() throws Exception {
        // given
        var reader = mock(CypherShellTerminal.Reader.class);
        when(reader.readStatement(any()))
                .thenThrow(new UserInterruptException(""))
                .thenReturn(new StatementParser.ParsedStatements(
                        List.of(new CommandStatement(":exit", List.of(), true, 0, 0))));
        var historian = mock(Historian.class);
        var terminal = mock(CypherShellTerminal.class);
        when(terminal.read()).thenReturn(reader);
        when(terminal.getHistory()).thenReturn(historian);
        doThrow(new ExitException(EXIT_SUCCESS)).when(cmdExecuter).execute(any(ParsedStatement.class));

        var runner = runner(terminal);

        // when
        runner.runUntilEnd();

        // then
        verify(cmdExecuter, times(2)).lastNeo4jErrorCode();
        verify(cmdExecuter).execute(new CommandStatement(":exit", List.of(), true, 0, 0));
        verifyNoMoreInteractions(cmdExecuter);
        var expectedError =
                "\u001B[91mInterrupted (Note that Cypher queries must end with a \u001B[1msemicolon\u001B[22m."
                        + " Type \u001B[1m:exit\u001B[22m to exit the shell.)\u001B[m";
        verify(printer).printError(expectedError);
    }

    @Test
    void testSignalHandleDuringExecution() throws Exception {
        // given
        BoltStateHandler boltStateHandler = mock(BoltStateHandler.class);
        var fakeShell = spy(new FakeInterruptableShell(printer, boltStateHandler));
        cmdExecuter = fakeShell;
        databaseManager = fakeShell;
        txHandler = fakeShell;
        var runner = runner(lines("RETURN 1;", ":exit"));

        // during
        Thread t = new Thread(runner::runUntilEnd);
        t.start();

        // wait until execution has begun
        while (fakeShell.executionThread.get() == null) {
            Thread.sleep(1000L);
        }

        // when
        runner.handleUserInterrupt();

        t.join();

        // then
        verify(fakeShell).execute(cypher("RETURN 1"));
        verify(fakeShell).execute(new CommandStatement(":exit", List.of(), true, 0, 4));
        verify(fakeShell).reset();
        verify(boltStateHandler).reset();
    }

    private TestInteractiveShellRunner setupInteractiveTestShellRunner(String input) {
        // NOTE: Tests using this will test a bit more of the stack using OfflineTestShell
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        BoltStateHandler mockedBoltStateHandler = mock(BoltStateHandler.class);
        when(mockedBoltStateHandler.getProtocolVersion()).thenReturn("");
        when(mockedBoltStateHandler.username()).thenReturn("myusername");
        when(mockedBoltStateHandler.isConnected()).thenReturn(true);

        final PrettyPrinter mockedPrettyPrinter = mock(PrettyPrinter.class);

        Printer printer = new AnsiPrinter(Format.VERBOSE, new PrintStream(output), new PrintStream(error));

        var in = new ByteArrayInputStream(input.getBytes(UTF_8));
        var terminal = terminalBuilder()
                .dumb()
                .streams(in, output)
                .interactive(true)
                .logger(printer)
                .build();
        OfflineTestShell offlineTestShell = new OfflineTestShell(printer, mockedBoltStateHandler, mockedPrettyPrinter);
        CommandHelper commandHelper =
                new CommandHelper(printer, Historian.empty, offlineTestShell, terminal, parameters);
        offlineTestShell.setCommandHelper(commandHelper);
        var runner = new InteractiveShellRunner(
                offlineTestShell,
                offlineTestShell,
                offlineTestShell,
                offlineTestShell,
                printer,
                terminal,
                userMessagesHandler,
                new CypherShellTerminal.FileHistory(historyFile));

        return new TestInteractiveShellRunner(runner, output, error, mockedBoltStateHandler);
    }

    @Test
    void testSwitchToUnavailableDatabase1() throws Exception {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner(input);

        // when
        when(sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer()).thenReturn("foo");
        doThrow(new TransientException(DatabaseManager.DATABASE_UNAVAILABLE_ERROR_CODE, "Not available"))
                .when(sr.mockedBoltStateHandler)
                .setActiveDatabase("foo");

        sr.runner.runUntilEnd();

        // then
        assertThat(sr.output.toString()).contains(format("myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT));
        assertThat(sr.error.toString()).contains("Not available");
    }

    @Test
    void testSwitchToUnavailableDatabase2() throws Exception {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner(input);

        // when
        when(sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer()).thenReturn("foo");
        doThrow(new ServiceUnavailableException("Not available"))
                .when(sr.mockedBoltStateHandler)
                .setActiveDatabase("foo");

        sr.runner.runUntilEnd();

        // then
        assertThat(sr.output.toString()).contains(format("myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT));
        assertThat(sr.error.toString()).contains("Not available");
    }

    @Test
    void testSwitchToUnavailableDatabase3() throws Exception {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner(input);

        // when
        when(sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer()).thenReturn("foo");
        doThrow(new DiscoveryException("Not available", null))
                .when(sr.mockedBoltStateHandler)
                .setActiveDatabase("foo");

        sr.runner.runUntilEnd();

        // then
        assertThat(sr.output.toString()).contains(format("myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT));
        assertThat(sr.error.toString()).contains("Not available");
    }

    @Test
    void testSwitchToNonExistingDatabase() throws Exception {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner(input);

        // when
        when(sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer()).thenReturn("mydb");
        doThrow(new ClientException("Non existing"))
                .when(sr.mockedBoltStateHandler)
                .setActiveDatabase("foo");

        sr.runner.runUntilEnd();

        // then
        assertThat(sr.output.toString()).contains("myusername@mydb> ");
        assertThat(sr.error.toString()).contains("Non existing");
    }

    private InteractiveShellRunner runner(String input) {
        return new InteractiveShellRunner(
                cmdExecuter,
                txHandler,
                databaseManager,
                connector,
                printer,
                testTerminal(input),
                userMessagesHandler,
                new CypherShellTerminal.FileHistory(historyFile));
    }

    private InteractiveShellRunner runner(CypherShellTerminal terminal) {
        return new InteractiveShellRunner(
                cmdExecuter,
                txHandler,
                databaseManager,
                connector,
                printer,
                terminal,
                userMessagesHandler,
                new CypherShellTerminal.FileHistory(historyFile));
    }

    private static String lines(String... lines) {
        return stream(lines).map(l -> l + "\n").collect(joining());
    }

    private CypherShellTerminal testTerminal(String input) {
        var in = new ByteArrayInputStream(input.getBytes(UTF_8));
        return terminalBuilder()
                .dumb()
                .streams(in, out)
                .interactive(true)
                .logger(printer)
                .build();
    }

    private static class FakeInterruptableShell extends CypherShell {
        protected final AtomicReference<Thread> executionThread = new AtomicReference<>();

        FakeInterruptableShell(Printer printer, BoltStateHandler boltStateHandler) {
            super(printer, boltStateHandler, mock(PrettyPrinter.class), null);
        }

        @Override
        public void execute(ParsedStatement statement) throws ExitException, CommandException {
            if (statement.statement().equals(":exit")) {
                throw new ExitException(EXIT_SUCCESS);
            }

            try {
                executionThread.set(Thread.currentThread());
                Thread.sleep(10_000L);
                System.out.println("Long done!");
            } catch (InterruptedException ignored) {
                throw new CommandException("execution interrupted");
            }
        }

        @Override
        public void reset() {
            // Do whatever usually happens
            super.reset();
            // But also simulate reset by interrupting the thread
            executionThread.get().interrupt();
        }

        @Override
        public String getActiveDatabaseAsSetByUser() {
            return ABSENT_DB_NAME;
        }

        @Override
        public String getActualDatabaseAsReportedByServer() {
            return DEFAULT_DEFAULT_DB_NAME;
        }
    }

    private static class TestInteractiveShellRunner {
        InteractiveShellRunner runner;
        ByteArrayOutputStream output;
        ByteArrayOutputStream error;
        BoltStateHandler mockedBoltStateHandler;

        TestInteractiveShellRunner(
                InteractiveShellRunner runner,
                ByteArrayOutputStream output,
                ByteArrayOutputStream error,
                BoltStateHandler mockedBoltStateHandler) {
            this.runner = runner;
            this.output = output;
            this.error = error;
            this.mockedBoltStateHandler = mockedBoltStateHandler;
        }
    }

    private static ParsedStatement statementContains(String contains) {
        return argThat(a -> a.statement().contains(contains));
    }

    private CypherStatement cypher(String cypher) {
        return new CypherStatement(cypher, true, 0, cypher.length() - 1);
    }
}
