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
package org.neo4j.shell.cli;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatcher;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.Reader;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.shell.Historian;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.parser.ShellStatementParser;
import org.neo4j.shell.parser.StatementParser;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.parser.StatementParser.IncompleteStatement;
import org.neo4j.shell.parser.StatementParser.ParsedStatement;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.anyString;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

class NonInteractiveShellRunnerTest
{
    private final Logger logger = mock( Logger.class );
    private final StatementExecuter cmdExecuter = mock( StatementExecuter.class );
    private StatementParser statementParser;
    private ClientException badLineError;

    @BeforeEach
    void setup() throws CommandException
    {
        statementParser = new ShellStatementParser();
        badLineError = new ClientException( "Found a bad line" );
        doThrow( badLineError ).when( cmdExecuter )
                .execute( argThat( (ArgumentMatcher<ParsedStatement>) statement -> statement.statement().contains( "bad" ) ) );
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    void testSimple()
    {
        String input = """
                good1;
                good2;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST,
                cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );
        int code = runner.runUntilEnd();

        assertEquals( 0, code, "Exit code incorrect" );
        verify( logger, times( 0 ) ).printError( anyString() );
    }

    @Test
    void testFailFast()
    {
        String input = """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST, cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );

        int code = runner.runUntilEnd();

        assertEquals( 1, code, "Exit code incorrect" );
        verify( logger ).printError( badLineError );
    }

    @Test
    void testFailAtEnd()
    {
        String input = """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END, cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );

        int code = runner.runUntilEnd();

        assertEquals( 1, code, "Exit code incorrect" );
        verify( logger, times( 2 ) ).printError( badLineError );
    }

    @Test
    void runUntilEndExitsImmediatelyOnParseError() throws IOException
    {
        // given
        StatementParser statementParser = mock( StatementParser.class );
        RuntimeException boom = new RuntimeException( "BOOM" );
        doThrow( boom ).when( statementParser ).parse( any( Reader.class ) );

        String input = """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END, cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );

        // when
        int code = runner.runUntilEnd();

        // then
        assertEquals( 1, code );
        verify( logger ).printError( boom );
    }

    @Test
    void runUntilEndExitsImmediatelyOnExitCommand() throws Exception
    {
        // given
        String input = """
                good1;
                bad;
                good2;
                bad;
                """;
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END, cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );

        // when
        doThrow( new ExitException( 99 ) ).when( cmdExecuter ).execute( any( ParsedStatement.class ) );

        int code = runner.runUntilEnd();

        // then
        assertEquals( 99, code );
        verify( cmdExecuter ).execute( new CypherStatement( "good1;" ) );
        verifyNoMoreInteractions( cmdExecuter );
    }

    @Test
    void nonInteractiveHasNoHistory()
    {
        // given
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_AT_END, cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( "".getBytes() ) );

        // when then
        assertEquals( Historian.empty, runner.getHistorian() );
    }

    @Test
    void shouldTryToExecuteIncompleteStatements() throws CommandException
    {
        String input = "good1;\nno semicolon here\n// A comment at end";
        NonInteractiveShellRunner runner = new NonInteractiveShellRunner(
                FailBehavior.FAIL_FAST,
                cmdExecuter,
                logger, statementParser,
                new ByteArrayInputStream( input.getBytes() ) );
        int code = runner.runUntilEnd();

        assertEquals( 0, code, "Exit code incorrect" );
        verify( logger, times( 0 ) ).printError( anyString() );
        verify( cmdExecuter ).execute( new CypherStatement( "good1;" ) );
        verify( cmdExecuter ).execute( new IncompleteStatement( "no semicolon here\n// A comment at end" ) );
        verifyNoMoreInteractions( cmdExecuter );
    }
}
