/*
 * Copyright (c) 2002-2020 "Neo4j,"
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

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;

import org.neo4j.driver.exceptions.ClientException;
import org.neo4j.driver.exceptions.ServiceUnavailableException;
import org.neo4j.driver.exceptions.TransientException;
import org.neo4j.shell.cli.CliArgs;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ExitException;
import org.neo4j.shell.log.AnsiLogger;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyConfig;
import org.neo4j.shell.prettyprint.ToStringLinePrinter;

import static java.lang.String.format;
import static org.hamcrest.CoreMatchers.isA;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.junit.Assume.assumeTrue;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.shell.DatabaseManager.DEFAULT_DEFAULT_DB_NAME;
import static org.neo4j.shell.DatabaseManager.SYSTEM_DB_NAME;
import static org.neo4j.shell.Main.EXIT_FAILURE;
import static org.neo4j.shell.Main.EXIT_SUCCESS;
import static org.neo4j.shell.util.Versions.majorVersion;

public class MainIntegrationTest
{
    private static String USER = "neo4j";
    private static String PASSWORD = "neo";
    @Rule
    public final ExpectedException exception = ExpectedException.none();
    private String inputString = String.format( "%s%n%s%n", USER, PASSWORD );
    private ByteArrayOutputStream baos;
    private ConnectionConfig connectionConfig;
    private CliArgs cliArgs;
    private CypherShell shell;
    private Main main;
    private PrintStream printStream;
    private InputStream inputStream;
    private ByteBuffer inputBuffer;

    @Before
    public void setup()
    {
        // given
        inputBuffer = ByteBuffer.allocate( 256 );
        inputBuffer.put( inputString.getBytes() );
        inputStream = new ByteArrayInputStream( inputBuffer.array() );

        baos = new ByteArrayOutputStream();
        printStream = new PrintStream( baos );

        main = new Main( inputStream, printStream );

        cliArgs = new CliArgs();
        cliArgs.setUsername( "", "" );
        cliArgs.setPassword( "", "" );

        ShellAndConnection sac = getShell( cliArgs );
        shell = sac.shell;
        connectionConfig = sac.connectionConfig;
    }

    private void ensureUser() throws Exception
    {
        if ( majorVersion( shell.getServerVersion() ) >= 4 )
        {
            shell.execute( ":use " + SYSTEM_DB_NAME );
            shell.execute( "CREATE OR REPLACE USER foo SET PASSWORD 'pass';" );
            shell.execute( "GRANT ROLE reader TO foo;" );
            shell.execute( ":use" );
        }
        else
        {
            try
            {
                shell.execute( "CALL dbms.security.createUser('foo', 'pass', true)" );
            }
            catch ( ClientException e )
            {
                if ( e.code().equalsIgnoreCase( "Neo.ClientError.General.InvalidArguments" ) && e.getMessage().contains( "already exists" ) )
                {
                    shell.execute( "CALL dbms.security.deleteUser('foo')" );
                    shell.execute( "CALL dbms.security.createUser('foo', 'pass', true)" );
                }
            }
        }
    }

    private void ensureDefaultDatabaseStarted() throws Exception
    {
        CliArgs cliArgs = new CliArgs();
        cliArgs.setUsername( "neo4j", "" );
        cliArgs.setPassword( "neo", "" );
        cliArgs.setDatabase( "system" );
        ShellAndConnection sac = getShell( cliArgs );
        main.connectMaybeInteractively( sac.shell, sac.connectionConfig, true, false, true );
        sac.shell.execute( "START DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );
    }

    @Test
    public void promptsOnWrongAuthenticationIfInteractive() throws Exception
    {
        // when
        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        assertEquals( format( "username: neo4j%npassword: ***%n" ), baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );
    }

    @Test
    public void promptsOnPasswordChangeRequired() throws Exception
    {
        int majorVersion = getVersionAndCreateUserWithPasswordChangeRequired();

        connectionConfig = getConnectionConfig( cliArgs );
        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // when
        inputBuffer.put( String.format( "foo%npass%nnewpass%n" ).getBytes() );
        baos.reset();
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // then
        assertTrue( shell.isConnected() );
        if ( majorVersion >= 4 )
        {
            // should have prompted to change the password
            String expectedChangePasswordOutput = format( "username: foo%npassword: ****%nPassword change required%nnew password: *******%n" );
            assertEquals( expectedChangePasswordOutput, baos.toString() );
            assertEquals( "foo", connectionConfig.username() );
            assertEquals( "newpass", connectionConfig.password() );
            assertNull( connectionConfig.newPassword() );

            // Should be able to execute read query
            shell.execute( "MATCH (n) RETURN count(n)" );
        }
        else
        {
            // in 3.x we do not get credentials expired exception on connection, but when we try to access data
            String expectedChangePasswordOutput = format( "username: foo%npassword: ****%n" );
            assertEquals( expectedChangePasswordOutput, baos.toString() );
            assertEquals( "foo", connectionConfig.username() );
            assertEquals( "pass", connectionConfig.password() );

            // Should get exception with instructions on how to change password using procedure
            exception.expect( ClientException.class );
            exception.expectMessage( "CALL dbms.changePassword" );
            shell.execute( "MATCH (n) RETURN count(n)" );
        }
    }

    @Test
    public void allowUserToUpdateExpiredPasswordInteractivelyWithoutBeingPrompted() throws Exception
    {
        //given a user that require a password change
        int majorVersion = getVersionAndCreateUserWithPasswordChangeRequired();

        //when the user attempts a non-interactive password update
        assumeTrue( majorVersion >= 4 );
        baos.reset();
        assertEquals( EXIT_SUCCESS, main.runShell( args( SYSTEM_DB_NAME, "foo", "pass",
                                                         "ALTER CURRENT USER SET PASSWORD from \"pass\" to \"pass2\";" ), shell, mock( Logger.class ) ) );
        //we shouldn't ask for a new password
        assertEquals( "", baos.toString() );

        //then the new user should be able to successfully connect, and run a command
        assertEquals( format( "n%n42%n" ),
                      executeNonInteractively( args( DEFAULT_DEFAULT_DB_NAME,
                                                     "foo", "pass2", "RETURN 42 AS n" ) ) );
    }

    @Test
    public void shouldFailIfNonInteractivelySettingPasswordOnNonSystemDb() throws Exception
    {
        //given a user that require a password change
        int majorVersion = getVersionAndCreateUserWithPasswordChangeRequired();

        //when
        assumeTrue( majorVersion >= 4 );

        //then
        assertEquals( EXIT_FAILURE, main.runShell( args( DEFAULT_DEFAULT_DB_NAME, "foo", "pass",
                                                         "ALTER CURRENT USER SET PASSWORD from \"pass\" to \"pass2\";" ), shell, mock( Logger.class ) ) );
    }

    @Test
    public void shouldBePromptedIfRunningNonInteractiveCypherThatDoesntUpdatePassword() throws Exception
    {
        //given a user that require a password change
        int majorVersion = getVersionAndCreateUserWithPasswordChangeRequired();

        //when
        assumeTrue( majorVersion >= 4 );

        //when interactively asked for a password use this
        inputBuffer.put( String.format( "pass2%n" ).getBytes() );
        baos.reset();
        assertEquals( EXIT_SUCCESS, main.runShell( args( DEFAULT_DEFAULT_DB_NAME, "foo", "pass",
                                                         "MATCH (n) RETURN n" ), shell, mock( Logger.class ) ) );

        //then should ask for a new password
        assertEquals( format( "Password change required%nnew password: *****%n" ), baos.toString() );

        //then the new user should be able to successfully connect, and run a command
        assertEquals( format( "n%n42%n" ),
                      executeNonInteractively( args( DEFAULT_DEFAULT_DB_NAME,
                                                     "foo", "pass2", "RETURN 42 AS n" ) ) );
    }

    @Test
    public void shouldNotBePromptedIfRunningWithExplicitNonInteractiveCypherThatDoesntUpdatePassword() throws Exception
    {
        //given a user that require a password change
        int majorVersion = getVersionAndCreateUserWithPasswordChangeRequired();

        //when
        assumeTrue( majorVersion >= 4 );

        //when interactively asked for a password use this
        inputBuffer.put( String.format( "pass2%n" ).getBytes() );
        baos.reset();
        CliArgs args = args( DEFAULT_DEFAULT_DB_NAME, "foo", "pass",
                             "MATCH (n) RETURN n" );
        args.setNonInteractive( true );
        assertEquals( EXIT_FAILURE, main.runShell( args, shell, mock( Logger.class ) ) );
    }

    @Test
    public void doesNotPromptToStdOutOnWrongAuthenticationIfOutputRedirected() throws Exception
    {
        // when
        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // Redirect System.in and System.out
        InputStream stdIn = System.in;
        PrintStream stdOut = System.out;
        System.setIn( inputStream );
        System.setOut( printStream );

        // Create a Main with the standard in and out
        try
        {
            Main realMain = new Main();
            realMain.connectMaybeInteractively( shell, connectionConfig, true, false, true );

            // then
            // should be connected
            assertTrue( shell.isConnected() );
            // should have prompted silently and set the username and password
            assertEquals( "neo4j", connectionConfig.username() );
            assertEquals( "neo", connectionConfig.password() );

            String out = baos.toString();
            assertEquals( "", out );
        }
        finally
        {
            // Restore in and out
            System.setIn( stdIn );
            System.setOut( stdOut );
        }
    }

    @Test
    public void wrongPortWithBolt() throws Exception
    {
        // given
        CliArgs cliArgs = new CliArgs();
        cliArgs.setScheme( "bolt", "" );
        cliArgs.setPort( 1234 );

        ShellAndConnection sac = getShell( cliArgs );
        CypherShell shell = sac.shell;
        ConnectionConfig connectionConfig = sac.connectionConfig;

        exception.expect( ServiceUnavailableException.class );
        exception.expectMessage( "Unable to connect to localhost:1234, ensure the database is running and that there is a working network connection to it" );
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );
    }

    @Test
    public void wrongPortWithNeo4j() throws Exception
    {
        // given
        CliArgs cliArgs = new CliArgs();
        cliArgs.setScheme( "neo4j", "" );
        cliArgs.setPort( 1234 );

        ShellAndConnection sac = getShell( cliArgs );
        CypherShell shell = sac.shell;
        ConnectionConfig connectionConfig = sac.connectionConfig;

        exception.expect( ServiceUnavailableException.class );
        // The error message here may be subject to change and is not stable across versions so let us not assert on it
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );
    }

    @Test
    public void shouldAskForCredentialsWhenConnectingWithAFile() throws Exception
    {
        //given

        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        //when
        CliArgs cliArgs = new CliArgs();
        cliArgs.setInputFilename( fileFromResource( "single.cypher" ) );
        ShellAndConnection sac = getShell( cliArgs );
        CypherShell shell = sac.shell;
        ConnectionConfig connectionConfig = sac.connectionConfig;
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // then we should have prompted and set the username and password
        assertEquals( format( "username: neo4j%npassword: ***%n" ), baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );
    }

    @Test
    public void shouldReadSingleCypherStatementsFromFile() throws Exception
    {
        assertEquals( format( "result%n42%n" ), executeFileNonInteractively( fileFromResource( "single.cypher" ) ) );
    }

    @Test
    public void shouldReadEmptyCypherStatementsFile() throws Exception
    {
        assertEquals( "", executeFileNonInteractively( fileFromResource( "empty.cypher" ) ) );
    }

    @Test
    public void shouldReadMultipleCypherStatementsFromFile() throws Exception
    {
        assertEquals( format( "result%n42%n" +
                              "result%n1337%n" +
                              "result%n\"done\"%n" ), executeFileNonInteractively( fileFromResource( "multiple.cypher" ) ) );
    }

    @Test
    public void shouldFailIfInputFileDoesntExist() throws Exception
    {
        //given
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        Logger logger = new AnsiLogger( false, Format.VERBOSE, new PrintStream( out ), new PrintStream( out ) );

        //when
        executeFileNonInteractively( "what.cypher", logger );

        //then
        assertEquals( format( "what.cypher (No such file or directory)%n" ), out.toString() );
    }

    @Test
    public void shouldHandleInvalidCypherFromFile() throws Exception
    {
        //given
        Logger logger = mock( Logger.class );

        // when
        String actual = executeFileNonInteractively( fileFromResource( "invalid.cypher" ), logger );

        //then we print the first valid row
        assertEquals( format( "result%n42%n" ), actual );
        //and print errors to the error log
        verify( logger ).printError( any( ClientException.class ) );
        verifyNoMoreInteractions( logger );
    }

    @Test
    public void shouldReadSingleCypherStatementsFromFileInteractively() throws Exception
    {
        // given
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        CypherShell shell = interactiveShell( linePrinter );

        // when
        shell.execute( ":source " + fileFromResource( "single.cypher" ) );
        exit( shell );

        // then
        assertEquals( format( "result%n42%n" ), linePrinter.result() );
    }

    @Test
    public void shouldReadMultipleCypherStatementsFromFileInteractively() throws Exception
    {
        // given
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        CypherShell shell = interactiveShell( linePrinter );

        // when
        shell.execute( ":source " + fileFromResource( "multiple.cypher" ) );
        exit( shell );

        // then
        assertEquals( format( "result%n42%n" +
                              "result%n1337%n" +
                              "result%n\"done\"%n" ), linePrinter.result() );
    }

    @Test
    public void shouldReadEmptyCypherStatementsFromFileInteractively() throws Exception
    {
        // given
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        CypherShell shell = interactiveShell( linePrinter );

        // when
        shell.execute( ":source " + fileFromResource( "empty.cypher" ) );
        exit( shell );

        // then
        assertEquals( "", linePrinter.result() );
    }

    @Test
    public void shouldHandleInvalidCypherStatementsFromFileInteractively() throws Exception
    {
        // given
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        CypherShell shell = interactiveShell( linePrinter );

        // then
        exception.expect( ClientException.class );
        exception.expectMessage( "Invalid input 'T" );
        shell.execute( ":source " + fileFromResource( "invalid.cypher" ) );
    }

    @Test
    public void shouldFailIfInputFileDoesntExistInteractively() throws Exception
    {
        // given
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        CypherShell shell = interactiveShell( linePrinter );

        // expect
        exception.expect( CommandException.class );
        exception.expectMessage( "Cannot find file: 'what.cypher'" );
        exception.expectCause( isA( FileNotFoundException.class ) );
        shell.execute( ":source what.cypher" );
    }

    @Test
    public void doesNotStartWhenDefaultDatabaseUnavailableIfInteractive() throws Exception
    {
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );
        inputBuffer.put( String.format( "neo4j%nneo%n" ).getBytes() );

        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // when
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // Multiple databases are only available from 4.0
        assumeTrue( majorVersion( shell.getServerVersion() ) >= 4 );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        String expectedLoginOutput = format( "username: neo4j%npassword: ***%n" );
        assertEquals( expectedLoginOutput, baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );

        // Stop the default database
        shell.execute( ":use " + SYSTEM_DB_NAME );
        shell.execute( "STOP DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );

        try
        {
            shell.disconnect();

            // Should get exception that database is unavailable when trying to connect
            exception.expect( TransientException.class );
            exception.expectMessage( "Database 'neo4j' is unavailable" );
            main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

            // then
            assertFalse( shell.isConnected() );
        }
        finally
        {
            // Start the default database again
            ensureDefaultDatabaseStarted();
        }
    }

    @Test
    public void startsAgainstSystemDatabaseWhenDefaultDatabaseUnavailableIfInteractive() throws Exception
    {
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );

        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // when
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // Multiple databases are only available from 4.0
        assumeTrue( majorVersion( shell.getServerVersion() ) >= 4 );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        String expectedLoginOutput = format( "username: neo4j%npassword: ***%n" );
        assertEquals( expectedLoginOutput, baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );

        // Stop the default database
        shell.execute( ":use " + SYSTEM_DB_NAME );
        shell.execute( "STOP DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );

        try
        {
            shell.disconnect();

            // Connect to system database
            CliArgs cliArgs = new CliArgs();
            cliArgs.setUsername( "neo4j", "" );
            cliArgs.setPassword( "neo", "" );
            cliArgs.setDatabase( "system" );
            ShellAndConnection sac = getShell( cliArgs );
            // Use the new shell and connection config from here on
            shell = sac.shell;
            connectionConfig = sac.connectionConfig;
            main.connectMaybeInteractively( shell, connectionConfig, true, false, true );

            // then
            assertTrue( shell.isConnected() );
        }
        finally
        {
            // Start the default database again
            ensureDefaultDatabaseStarted();
        }
    }

    @Test
    public void switchingToUnavailableDatabaseIfInteractive() throws Exception
    {
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );
        inputBuffer.put( String.format( "neo4j%nneo%n" ).getBytes() );

        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // when
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // Multiple databases are only available from 4.0
        assumeTrue( majorVersion( shell.getServerVersion() ) >= 4 );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        String expectedLoginOutput = format( "username: neo4j%npassword: ***%n" );
        assertEquals( expectedLoginOutput, baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );

        // Stop the default database
        shell.execute( ":use " + SYSTEM_DB_NAME );
        shell.execute( "STOP DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );

        try
        {
            // Should get exception that database is unavailable when trying to connect
            exception.expect( TransientException.class );
            exception.expectMessage( "Database 'neo4j' is unavailable" );
            shell.execute( ":use " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );
        }
        finally
        {
            // Start the default database again
            ensureDefaultDatabaseStarted();
        }
    }

    @Test
    public void switchingToUnavailableDefaultDatabaseIfInteractive() throws Exception
    {
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );
        inputBuffer.put( String.format( "neo4j%nneo%n" ).getBytes() );

        assertEquals( "", connectionConfig.username() );
        assertEquals( "", connectionConfig.password() );

        // when
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );

        // Multiple databases are only available from 4.0
        assumeTrue( majorVersion( shell.getServerVersion() ) >= 4 );

        // then
        // should be connected
        assertTrue( shell.isConnected() );
        // should have prompted and set the username and password
        String expectedLoginOutput = format( "username: neo4j%npassword: ***%n" );
        assertEquals( expectedLoginOutput, baos.toString() );
        assertEquals( "neo4j", connectionConfig.username() );
        assertEquals( "neo", connectionConfig.password() );

        // Stop the default database
        shell.execute( ":use " + SYSTEM_DB_NAME );
        shell.execute( "STOP DATABASE " + DatabaseManager.DEFAULT_DEFAULT_DB_NAME );

        try
        {
            // Should get exception that database is unavailable when trying to connect
            exception.expect( TransientException.class );
            exception.expectMessage( "Database 'neo4j' is unavailable" );
            shell.execute( ":use" );
        }
        finally
        {
            // Start the default database again
            ensureDefaultDatabaseStarted();
        }
    }

    private String executeFileNonInteractively( String filename )
    {
        return executeFileNonInteractively( filename, mock( Logger.class ) );
    }

    private String executeFileNonInteractively( String filename, Logger logger )
    {
        CliArgs cliArgs = new CliArgs();
        cliArgs.setUsername( USER, "" );
        cliArgs.setPassword( PASSWORD, "" );
        cliArgs.setInputFilename( filename );

        return executeNonInteractively( cliArgs, logger );
    }

    private String executeNonInteractively( CliArgs cliArgs )
    {
        return executeNonInteractively( cliArgs, mock( Logger.class ) );
    }

    private String executeNonInteractively( CliArgs cliArgs, Logger logger )
    {
        ToStringLinePrinter linePrinter = new ToStringLinePrinter();
        ShellAndConnection sac = getShell( cliArgs, linePrinter );
        CypherShell shell = sac.shell;
        main.runShell( cliArgs, shell, logger );
        return linePrinter.result();
    }

    private String fileFromResource( String filename )
    {
        return getClass().getClassLoader().getResource( filename ).getFile();
    }

    private CypherShell interactiveShell( LinePrinter linePrinter ) throws Exception
    {
        PrettyConfig prettyConfig = new PrettyConfig( new CliArgs() );
        CypherShell shell = new CypherShell( linePrinter, prettyConfig, true, new ShellParameterMap() );
        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );
        return shell;
    }

    private ShellAndConnection getShell( CliArgs cliArgs )
    {
        Logger logger = new AnsiLogger( cliArgs.getDebugMode() );
        return getShell( cliArgs, logger );
    }

    private ShellAndConnection getShell( CliArgs cliArgs, LinePrinter linePrinter )
    {
        PrettyConfig prettyConfig = new PrettyConfig( cliArgs );
        ConnectionConfig connectionConfig = getConnectionConfig( cliArgs );

        return new ShellAndConnection( new CypherShell( linePrinter, prettyConfig, true, new ShellParameterMap() ), connectionConfig );
    }

    private ConnectionConfig getConnectionConfig( CliArgs cliArgs )
    {
        return new ConnectionConfig(
                cliArgs.getScheme(),
                cliArgs.getHost(),
                cliArgs.getPort(),
                cliArgs.getUsername(),
                cliArgs.getPassword(),
                cliArgs.getEncryption(),
                cliArgs.getDatabase() );
    }

    private void exit( CypherShell shell ) throws CommandException
    {
        try
        {
            shell.execute( ":exit" );
            fail( "Should have exited" );
        }
        catch ( ExitException e )
        {
            //do nothing
        }
    }

    private CliArgs args( String db, String user, String pass, String cypher )
    {
        CliArgs cliArgs = new CliArgs();
        cliArgs.setUsername( user, "" );
        cliArgs.setPassword( pass, "" );
        cliArgs.setDatabase( db );
        cliArgs.setCypher( cypher );
        return cliArgs;
    }

    private int getVersionAndCreateUserWithPasswordChangeRequired() throws Exception
    {
        shell.setCommandHelper( new CommandHelper( mock( Logger.class ), Historian.empty, shell ) );

        main.connectMaybeInteractively( shell, connectionConfig, true, true, true );
        String expectedLoginOutput = format( "username: neo4j%npassword: ***%n" );
        assertEquals( expectedLoginOutput, baos.toString() );
        ensureUser();
        int majorVersion = majorVersion( shell.getServerVersion() );
        shell.disconnect();
        return majorVersion;
    }

    private static class ShellAndConnection
    {
        CypherShell shell;
        ConnectionConfig connectionConfig;

        ShellAndConnection( CypherShell shell, ConnectionConfig connectionConfig )
        {
            this.shell = shell;
            this.connectionConfig = connectionConfig;
        }
    }
}
