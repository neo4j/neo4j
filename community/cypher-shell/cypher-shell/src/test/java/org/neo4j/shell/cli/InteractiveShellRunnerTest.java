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
import org.junit.jupiter.api.io.TempDir;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintStream;
import java.nio.file.Files;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.DiscoveryException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.shell.ConnectionConfig;
import org.neo4j.shell.CypherShell;
import org.neo4j.shell.DatabaseManager;
import org.neo4j.shell.Historian;
import org.neo4j.shell.OfflineTestShell;
import org.neo4j.shell.ShellParameterMap;
import org.neo4j.shell.StatementExecuter;
import org.neo4j.shell.TransactionHandler;
import org.neo4j.shell.UserMessagesHandler;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.exception.NoMoreInputException;
import org.neo4j.shell.exception.UserInterruptException;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.terminal.CypherShellTerminal;

import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Arrays.stream;
import static java.util.stream.Collectors.joining;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.endsWith;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.doReturn;
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

class InteractiveShellRunnerTest
{
    @TempDir
    File temp;

    private Logger logger;
    private StatementExecuter cmdExecuter;
    private File historyFile;
    private TransactionHandler txHandler;
    private DatabaseManager databaseManager;
    private ClientException badLineError;
    private UserMessagesHandler userMessagesHandler;
    private ConnectionConfig connectionConfig;
    private ByteArrayOutputStream out;

    @BeforeEach
    void setup() throws Exception
    {
        logger = mock( Logger.class );
        cmdExecuter = mock( StatementExecuter.class );
        txHandler = mock( TransactionHandler.class );
        databaseManager = mock( DatabaseManager.class );
        connectionConfig = mock( ConnectionConfig.class );
        historyFile = new File( temp, "test" );
        badLineError = new ClientException( "Found a bad line" );
        userMessagesHandler = mock( UserMessagesHandler.class );
        out = new ByteArrayOutputStream();
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( "mydb" );
        when( userMessagesHandler.getWelcomeMessage() ).thenReturn( "Welcome to cypher-shell!" );
        when( connectionConfig.username() ).thenReturn( "myusername" );

        doThrow( badLineError ).when( cmdExecuter ).execute( contains( "bad" ) );
        doReturn( System.out ).when( logger ).getOutputStream();
    }

    @Test
    void testSimple() throws Exception
    {
        var runner = runner( lines( "good1;", "good2;" ) );
        runner.runUntilEnd();

        verify( cmdExecuter ).execute( "good1;" );
        verify( cmdExecuter ).execute( "good2;" );
        verify( cmdExecuter, times( 3 ) ).lastNeo4jErrorCode();
        verifyNoMoreInteractions( cmdExecuter );

        assertEquals( "myusername@mydb> good1;\r\nmyusername@mydb> good2;\r\nmyusername@mydb> \r\n", out.toString() );
    }

    @Test
    void runUntilEndShouldKeepGoingOnErrors() throws CommandException
    {
        var runner = runner( lines( "good1;", "bad1;", "good2;", "bad2;", "good3;" ) );
        int code = runner.runUntilEnd();

        assertEquals( 0, code, "Wrong exit code" );

        verify( cmdExecuter ).execute( "good1;" );
        verify( cmdExecuter ).execute( "bad1;" );
        verify( cmdExecuter ).execute( "good2;" );
        verify( cmdExecuter ).execute( "bad2;" );
        verify( cmdExecuter ).execute( "good3;" );
        verify( cmdExecuter, times( 6 ) ).lastNeo4jErrorCode();
        verifyNoMoreInteractions( cmdExecuter );

        verify( logger, times( 2 ) ).printError( badLineError );
    }

    @Test
    void runUntilEndShouldStopOnExitExceptionAndReturnCode() throws CommandException
    {
        var runner = runner( lines( "good1;", "bad1;", "good2;", "exit;", "bad2;", "good3;" ) );

        doThrow( new ExitException( 1234 ) ).when( cmdExecuter ).execute( contains( "exit;" ) );

        int code = runner.runUntilEnd();

        assertEquals( 1234, code, "Wrong exit code" );

        verify( cmdExecuter ).execute( "good1;" );
        verify( cmdExecuter ).execute( "bad1;" );
        verify( cmdExecuter ).execute( "good2;" );
        verify( cmdExecuter ).execute( "exit;" );
        verify( cmdExecuter, times( 4 ) ).lastNeo4jErrorCode();
        verifyNoMoreInteractions( cmdExecuter );

        verify( logger ).printError( badLineError );
    }

    @Test
    void historyIsRecorded() throws Exception
    {
        // given

        String cmd1 = ":set var \"3\"";
        String cmd2 = ":help exit";
        var runner = runner( lines( cmd1, cmd2 ) );

        // when
        runner.runUntilEnd();

        // then
        Historian historian = runner.getHistorian();
        historian.flushHistory();

        List<String> history = Files.readAllLines( historyFile.toPath() );

        assertEquals( 2, history.size() );
        assertThat( history.get( 0 ), endsWith( ":" + cmd1 ) );
        assertThat( history.get( 1 ), endsWith( ":" + cmd2 ) );

        history = historian.getHistory();
        assertEquals( 2, history.size() );
        assertEquals( cmd1, history.get( 0 ) );
        assertEquals( cmd2, history.get( 1 ) );
    }

    @Test
    void unescapedBangWorks() throws Exception
    {
        // given
        PrintStream mockedErr = mock( PrintStream.class );
        when( logger.getErrorStream() ).thenReturn( mockedErr );

        // Bangs need escaping in JLine by default, just like in bash, but we have disabled that
        var runner = runner( ":set var \"String with !bang\"\n" );

        // when
        List<String> statements = runner.readUntilStatement();
        // then
        assertThat( statements.get( 0 ), equalTo( ":set var \"String with !bang\"" ) );
    }

    @Test
    void escapedBangWorks() throws Exception
    {
        // given
        PrintStream mockedErr = mock( PrintStream.class );
        when( logger.getErrorStream() ).thenReturn( mockedErr );

        // Bangs need escaping in JLine by default, just like in bash, but we have disabled that
        var runner = runner( ":set var \"String with \\!bang\"\n" );

        // when
        List<String> statements = runner.readUntilStatement();

        // then
        assertEquals( ":set var \"String with \\!bang\"", statements.get( 0 ) );
    }

    @Test
    void justNewLineThrowsNoMoreInput()
    {
        // given
        var runner = runner( "\n" );

        // when
        assertThrows( NoMoreInputException.class, runner::readUntilStatement );
    }

    @Test
    void emptyStringThrowsNoMoreInput()
    {
        // given
        var runner = runner( "" );

        // when
        assertThrows( NoMoreInputException.class, runner::readUntilStatement );
    }

    @Test
    void emptyLineIsIgnored() throws Exception
    {
        // given
        var runner = runner( "     \nCREATE (n:Person) RETURN n;\n" );

        // when
        List<String> statements = runner.readUntilStatement();

        // then
        assertEquals( 1, statements.size() );
        assertThat( statements.get( 0 ), is( "CREATE (n:Person) RETURN n;" ) );
    }

    @Test
    void testPrompt()
    {
        // given
        var runner = runner( lines( "  \t ", "   ", "bla bla;" ) );
        when( txHandler.isTransactionOpen() ).thenReturn( false );
        runner.runUntilEnd();

        // when
        assertThat( out.toString(), equalTo( "myusername@mydb>    \r\n                    \r\n                 bla bla;\r\nmyusername@mydb> \r\n" ) );
    }

    @Test
    void testPromptShowDatabaseAsSetByUserWhenServerReportNull()
    {
        // given
        var runner = runner( "return 1;" );

        // when
        when( txHandler.isTransactionOpen() ).thenReturn( false );
        when( databaseManager.getActiveDatabaseAsSetByUser() ).thenReturn( "foo" );
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( null );
        runner.runUntilEnd();

        // then
        String wantedPrompt = "myusername@foo> return 1;\r\n";
        assertThat( out.toString(), equalTo( wantedPrompt ) );
    }

    @Test
    void testPromptShowDatabaseAsSetByUserWhenServerReportAbsent()
    {
        // given
        var runner = runner( "return 1;" );

        // when
        when( txHandler.isTransactionOpen() ).thenReturn( false );
        when( databaseManager.getActiveDatabaseAsSetByUser() ).thenReturn( "foo" );
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( DatabaseManager.ABSENT_DB_NAME );
        runner.runUntilEnd();

        // then
        assertThat( out.toString(), equalTo( "myusername@foo> return 1;\r\n" ) );
    }

    @Test
    void testPromptShowUnresolvedDefaultDatabaseWhenServerReportNull()
    {
        // given
        var runner = runner( "return 1;" );

        // when
        when( txHandler.isTransactionOpen() ).thenReturn( false );
        when( databaseManager.getActiveDatabaseAsSetByUser() ).thenReturn( DatabaseManager.ABSENT_DB_NAME );
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( null );
        runner.runUntilEnd();

        // then
        assertThat( out.toString(), equalTo( "myusername@<default_database>> return 1;\r\n" ) );
    }

    @Test
    void testPromptShowUnresolvedDefaultDatabaseWhenServerReportAbsent()
    {
        // given
        var runner = runner( "return 1;" );

        // when
        when( txHandler.isTransactionOpen() ).thenReturn( false );
        when( databaseManager.getActiveDatabaseAsSetByUser() ).thenReturn( DatabaseManager.ABSENT_DB_NAME );
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( DatabaseManager.ABSENT_DB_NAME );
        runner.runUntilEnd();

        // then
        assertThat( out.toString(), equalTo( "myusername@<default_database>> return 1;\r\n" ) );
    }

    @Test
    void testLongPrompt()
    {
        // given
        String actualDbName = "TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome";
        when( databaseManager.getActualDatabaseAsReportedByServer() ).thenReturn( actualDbName );
        var runner = runner( lines( "match", "(n)", "where n.id = 1", "", ";", "return 1;" ) );

        // when
        when( txHandler.isTransactionOpen() ).thenReturn( false );

        var exitCode = runner.runUntilEnd();

        assertEquals( EXIT_SUCCESS, exitCode );

        var expected =
                "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n" +
                "> match\n" +
                "  (n)\n" +
                "  where n.id = 1\n" +
                "  \n" +
                "  ;\n" +
                "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n" +
                "> return 1;\n" +
                "myusername@TheLongestDbNameEverCreatedInAllOfHistoryAndTheUniversePlusSome\n" +
                "> \n";
        assertThat( out.toString().replace( "\r", "" ), equalTo( expected ) );
    }

    @Test
    void testPromptInTx()
    {
        // given
        var runner = runner( lines( "   ", "   ", "bla bla;" ) );
        when( txHandler.isTransactionOpen() ).thenReturn( true );

        assertEquals( EXIT_SUCCESS, runner.runUntilEnd() );

        var expected =
                "myusername@mydb#    \r\n" +
                "                    \r\n" +
                "                 bla bla;\r\n" +
                "myusername@mydb# \r\n";
        assertThat( out.toString(), equalTo( expected ) );
    }

    @Test
    void multilineRequiresNewLineOrSemicolonToEnd()
    {
        // given
        var runner = runner( "  \\   \nCREATE (n:Person) RETURN n\n" );

        // when
        runner.runUntilEnd();

        // then
        verify( cmdExecuter ).lastNeo4jErrorCode();
        verifyNoMoreInteractions( cmdExecuter );
    }

    @Test
    void printsWelcomeAndExitMessage()
    {
        // given
        var runner = runner( "\nCREATE (n:Person) RETURN n\n;\n" );

        // when
        runner.runUntilEnd();

        // then
        verify( logger ).printIfVerbose( "Welcome to cypher-shell!" );
        verify( logger ).printIfVerbose( "\nBye!" );
    }

    @Test
    void multilineEndsOnSemicolonOnNewLine() throws Exception
    {
        // given
        var runner = runner( "\nCREATE (n:Person) RETURN n\n;\n" );

        // when
        runner.runUntilEnd();

        // then
        verify( cmdExecuter ).execute( "CREATE (n:Person) RETURN n\n;" );
    }

    @Test
    void multilineEndsOnSemicolonOnSameLine() throws Exception
    {
        // given
        var runner = runner( "\nCREATE (n:Person) RETURN n;\n" );

        // when
        runner.runUntilEnd();

        // then
        verify( cmdExecuter ).execute( "CREATE (n:Person) RETURN n;" );
    }

    @Test
    void testSignalHandleOutsideExecution() throws Exception
    {
        // given
        var reader = mock( CypherShellTerminal.Reader.class );
        when( reader.readStatement( any() ) )
            .thenThrow( new UserInterruptException( "" ) )
            .thenReturn( new CypherShellTerminal.ParsedStatement()
            {
                @Override
                public String unparsed()
                {
                    return ":exit";
                }

                @Override
                public List<String> parsed()
                {
                    return Collections.singletonList( ":exit" );
                }
            } );
        var historian = mock( Historian.class );
        var terminal = mock( CypherShellTerminal.class );
        when( terminal.read() ).thenReturn( reader );
        when( terminal.getHistory() ).thenReturn( historian );
        doThrow( new ExitException( EXIT_SUCCESS ) ).when( cmdExecuter ).execute( any() );

        var runner = runner( terminal );

        // when
        runner.runUntilEnd();

        // then
        verify( cmdExecuter, times( 2 ) ).lastNeo4jErrorCode();
        verify( cmdExecuter ).execute( ":exit" );
        verifyNoMoreInteractions( cmdExecuter );
        var expectedError = "@|RED Interrupted (Note that Cypher queries must end with a |@@|RED,BOLD semicolon|@@|RED . " +
                            "Type |@@|RED,BOLD :exit|@@|RED  to exit the shell.)|@";
        verify( logger ).printError( expectedError );
    }

    @Test
    void testSignalHandleDuringExecution() throws Exception
    {
        // given
        BoltStateHandler boltStateHandler = mock( BoltStateHandler.class );
        var fakeShell = spy( new FakeInterruptableShell( logger, boltStateHandler ) );
        cmdExecuter = fakeShell;
        databaseManager = fakeShell;
        txHandler = fakeShell;
        var runner = runner( lines( "RETURN 1;", ":exit" ) );

        // during
        Thread t = new Thread( runner::runUntilEnd );
        t.start();

        // wait until execution has begun
        while ( fakeShell.executionThread.get() == null )
        {
            Thread.sleep( 1000L );
        }

        // when
        runner.handleUserInterrupt();

        t.join();

        // then
        verify( fakeShell ).execute( "RETURN 1;" );
        verify( fakeShell ).execute( ":exit" );
        verify( fakeShell ).reset();
        verify( boltStateHandler ).reset();
    }

    private TestInteractiveShellRunner setupInteractiveTestShellRunner( String input )
    {
        // NOTE: Tests using this will test a bit more of the stack using OfflineTestShell
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        ByteArrayOutputStream error = new ByteArrayOutputStream();

        BoltStateHandler mockedBoltStateHandler = mock( BoltStateHandler.class );
        when( mockedBoltStateHandler.getProtocolVersion() ).thenReturn( "" );

        final PrettyPrinter mockedPrettyPrinter = mock( PrettyPrinter.class );

        Logger logger = new AnsiLogger( false, Format.VERBOSE, new PrintStream( output ), new PrintStream( error ) );

        OfflineTestShell offlineTestShell = new OfflineTestShell( logger, mockedBoltStateHandler, mockedPrettyPrinter );
        CommandHelper commandHelper = new CommandHelper( logger, Historian.empty, offlineTestShell );
        offlineTestShell.setCommandHelper( commandHelper );

        var in = new ByteArrayInputStream( input.getBytes( UTF_8 ) );
        var terminal = terminalBuilder().dumb().streams( in, output ).interactive( true ).logger( logger ).build();
        var runner = new InteractiveShellRunner( offlineTestShell, offlineTestShell, offlineTestShell, logger,
                                                 terminal, userMessagesHandler, connectionConfig, historyFile );

        return new TestInteractiveShellRunner( runner, output, error, mockedBoltStateHandler );
    }

    @Test
    void testSwitchToUnavailableDatabase1() throws Exception
    {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner( input );

        // when
        when( sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer() ).thenReturn( "foo" );
        doThrow( new TransientException( DatabaseManager.DATABASE_UNAVAILABLE_ERROR_CODE, "Not available" ) )
                .when( sr.mockedBoltStateHandler ).setActiveDatabase( "foo" );

        sr.runner.runUntilEnd();

        // then
        assertThat( sr.output.toString(), containsString( format( "myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT ) ) );
        assertThat( sr.error.toString(), containsString( "Not available" ) );
    }

    @Test
    void testSwitchToUnavailableDatabase2() throws Exception
    {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner( input );

        // when
        when( sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer() ).thenReturn( "foo" );
        doThrow( new ServiceUnavailableException( "Not available" ) ).when( sr.mockedBoltStateHandler ).setActiveDatabase( "foo" );

        sr.runner.runUntilEnd();

        // then
        assertThat( sr.output.toString(), containsString( format( "myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT ) ) );
        assertThat( sr.error.toString(), containsString( "Not available" ) );
    }

    @Test
    void testSwitchToUnavailableDatabase3() throws Exception
    {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner( input );

        // when
        when( sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer() ).thenReturn( "foo" );
        doThrow( new DiscoveryException( "Not available", null ) ).when( sr.mockedBoltStateHandler ).setActiveDatabase( "foo" );

        sr.runner.runUntilEnd();

        // then
        assertThat( sr.output.toString(), containsString( format( "myusername@foo%s> ", DATABASE_UNAVAILABLE_ERROR_PROMPT_TEXT ) ) );
        assertThat( sr.error.toString(), containsString( "Not available" ) );
    }

    @Test
    void testSwitchToNonExistingDatabase() throws Exception
    {
        // given
        String input = ":use foo;\n";
        TestInteractiveShellRunner sr = setupInteractiveTestShellRunner( input );

        // when
        when( sr.mockedBoltStateHandler.getActualDatabaseAsReportedByServer() ).thenReturn( "mydb" );
        doThrow( new ClientException( "Non existing" ) ).when( sr.mockedBoltStateHandler ).setActiveDatabase( "foo" );

        sr.runner.runUntilEnd();

        // then
        assertThat( sr.output.toString(), containsString( "myusername@mydb> " ) );
        assertThat( sr.error.toString(), containsString( "Non existing" ) );
    }

    private InteractiveShellRunner runner( String input )
    {
        return new InteractiveShellRunner( cmdExecuter, txHandler, databaseManager, logger,
                                           testTerminal( input ), userMessagesHandler, connectionConfig, historyFile );
    }

    private InteractiveShellRunner runner( CypherShellTerminal terminal )
    {
        return new InteractiveShellRunner( cmdExecuter, txHandler, databaseManager, logger,
                                           terminal, userMessagesHandler, connectionConfig, historyFile );
    }

    private static String lines( String... lines )
    {
        return stream( lines ).map( l -> l + "\n" ).collect( joining() );
    }

    private CypherShellTerminal testTerminal( String input )
    {
        var in = new ByteArrayInputStream( input.getBytes( UTF_8 ) );
        return terminalBuilder().dumb().streams( in, out ).interactive( true ).logger( logger ).build();

    }

    private static class FakeInterruptableShell extends CypherShell
    {
        protected final AtomicReference<Thread> executionThread = new AtomicReference<>();

        FakeInterruptableShell( Logger logger,
                                BoltStateHandler boltStateHandler )
        {
            super( logger, boltStateHandler, mock( PrettyPrinter.class ), new ShellParameterMap() );
        }

        @Override
        public void execute( String statement ) throws ExitException, CommandException
        {
            if ( statement.equals( ":exit" ) )
            {
                throw new ExitException( EXIT_SUCCESS );
            }

            try
            {
                executionThread.set( Thread.currentThread() );
                Thread.sleep( 10_000L );
                System.out.println("Long done!");
            }
            catch ( InterruptedException ignored )
            {
                throw new CommandException( "execution interrupted" );
            }
        }

        @Override
        public void reset()
        {
            // Do whatever usually happens
            super.reset();
            // But also simulate reset by interrupting the thread
            executionThread.get().interrupt();
        }

        @Override
        public String getActiveDatabaseAsSetByUser()
        {
            return ABSENT_DB_NAME;
        }

        @Override
        public String getActualDatabaseAsReportedByServer()
        {
            return DEFAULT_DEFAULT_DB_NAME;
        }
    }

    private static class TestInteractiveShellRunner
    {
        InteractiveShellRunner runner;
        ByteArrayOutputStream output;
        ByteArrayOutputStream error;
        BoltStateHandler mockedBoltStateHandler;

        TestInteractiveShellRunner( InteractiveShellRunner runner, ByteArrayOutputStream output,
                                    ByteArrayOutputStream error, BoltStateHandler mockedBoltStateHandler )
        {
            this.runner = runner;
            this.output = output;
            this.error = error;
            this.mockedBoltStateHandler = mockedBoltStateHandler;
        }
    }
}
