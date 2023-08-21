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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;
import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.Historian;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;
import org.neo4j.shell.printer.Printer;

class NonInteractiveShellRunnerTest {
    private final Printer printer = mock(Printer.class);
    private final StatementExecuter cmdExecuter = mock(StatementExecuter.class);
    private StatementParser statementParser;
    private ClientException badLineError;

    @BeforeEach
    void setup() throws CommandException {
        statementParser = new ShellStatementParser();
        badLineError = new ClientException("Found a bad line");
        doThrow(badLineError).when(cmdExecuter).execute(argThat((ArgumentMatcher<ParsedStatement>)
                statement -> statement.statement().contains("bad")));
    }

    @Test
    void testSimple() {
        String input = """
                good1;
                good2;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));
        int code = runner.runUntilEnd();

        assertEquals(0, code, "Exit code incorrect");
        verify(printer, times(0)).printError(anyString());
    }

    @Test
    void testFailFast() {
        String input =
                """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));

        int code = runner.runUntilEnd();

        assertEquals(1, code, "Exit code incorrect");
        verify(printer).printError(badLineError);
    }

    @Test
    void testFailAtEnd() {
        String input =
                """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));

        int code = runner.runUntilEnd();

        assertEquals(1, code, "Exit code incorrect");
        verify(printer, times(2)).printError(badLineError);
    }

    @Test
    void runUntilEndExitsImmediatelyOnParseError() throws IOException {
        // given
        StatementParser statementParser = mock(StatementParser.class);
        RuntimeException boom = new RuntimeException("BOOM");
        doThrow(boom).when(statementParser).parse(any(Reader.class));

        String input =
                """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));

        // when
        int code = runner.runUntilEnd();

        // then
        assertEquals(1, code);
        verify(printer).printError(boom);
    }

    @Test
    void runUntilEndExitsImmediatelyOnExitCommand() throws Exception {
        // given
        String input =
                """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));

        // when
        doThrow(new ExitException(99)).when(cmdExecuter).execute(any(ParsedStatement.class));

        int code = runner.runUntilEnd();

        // then
        assertEquals(99, code);
        verify(cmdExecuter).execute(new CypherStatement("good1", true, 0, 4));
        verifyNoMoreInteractions(cmdExecuter);
    }

    @Test
    void nonInteractiveHasNoHistory() {
        // given
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream("".getBytes()));

        // when then
        assertEquals(Historian.empty, runner.getHistorian());
    }

    @Test
    void shouldTryToExecuteIncompleteStatements() throws CommandException {
        String input = "good1;\nno semicolon here\n// A comment at end";
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST,
                cmdExecuter,
                printer,
                statementParser,
                new ByteArrayInputStream(input.getBytes()));
        int code = runner.runUntilEnd();

        assertEquals(0, code, "Exit code incorrect");
        verify(printer, times(0)).printError(anyString());
        verify(cmdExecuter).execute(new CypherStatement("good1", true, 0, 4));
        verify(cmdExecuter).execute(new CypherStatement("no semicolon here\n// A comment at end", false, 7, 43));
        verifyNoMoreInteractions(cmdExecuter);
    }
}
