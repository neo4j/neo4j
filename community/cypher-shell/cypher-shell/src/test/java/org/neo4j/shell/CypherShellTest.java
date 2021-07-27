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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Record;
import org.neo4j.driver.Session;
import org.neo4j.driver.Value;
import org.neo4j.driver.summary.ResultSummary;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.commands.CommandExecutable;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.exception.ParameterException;
import org.neo4j.shell.log.Logger;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.state.ListBoltResult;

import static java.util.Arrays.asList;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

@SuppressWarnings( "OptionalGetWithoutIsPresent" )
class CypherShellTest
{
    private final PrettyPrinter mockedPrettyPrinter = mock( PrettyPrinter.class );
    private final BoltStateHandler mockedBoltStateHandler = mock( BoltStateHandler.class );
    private final Logger logger = mock( Logger.class );
    private OfflineTestShell offlineTestShell;

    @BeforeEach
    void setup()
    {
        when( mockedBoltStateHandler.getProtocolVersion() ).thenReturn( "" );

        doReturn( System.out ).when( logger ).getOutputStream();
        offlineTestShell = new OfflineTestShell( logger, mockedBoltStateHandler, mockedPrettyPrinter );

        CommandHelper commandHelper = new CommandHelper( logger, Historian.empty, offlineTestShell );

        offlineTestShell.setCommandHelper( commandHelper );
    }

    @Test
    void verifyDelegationOfConnectionMethods() throws CommandException
    {
        ConnectionConfig cc = new ConnectionConfig( "bolt", "", 1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME );
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        shell.connect( cc );
        verify( mockedBoltStateHandler ).connect( cc, null );

        shell.isConnected();
        verify( mockedBoltStateHandler ).isConnected();
    }

    @Test
    void verifyDelegationOfResetMethod()
    {
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        shell.reset();
        verify( mockedBoltStateHandler ).reset();
    }

    @Test
    void verifyDelegationOfGetProtocolVersionMethod()
    {
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        shell.getProtocolVersion();
        verify( mockedBoltStateHandler ).getProtocolVersion();
    }

    @Test
    void verifyDelegationOfIsTransactionOpenMethod()
    {
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        shell.isTransactionOpen();
        verify( mockedBoltStateHandler ).isTransactionOpen();
    }

    @Test
    void verifyDelegationOfTransactionMethods() throws CommandException
    {
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        shell.beginTransaction();
        verify( mockedBoltStateHandler ).beginTransaction();

        shell.commitTransaction();
        verify( mockedBoltStateHandler ).commitTransaction();

        shell.rollbackTransaction();
        verify( mockedBoltStateHandler ).rollbackTransaction();
    }

    @Test
    void setWhenOfflineShouldWork() throws ParameterException, CommandException
    {
        CypherShell shell = new OfflineTestShell( logger, mockedBoltStateHandler, mockedPrettyPrinter );
        when( mockedBoltStateHandler.isConnected() ).thenReturn( false );
        when( mockedBoltStateHandler.runCypher( anyString(), anyMap() ) ).thenThrow( new CommandException( "not connected" ) );

        Object result = shell.getParameterMap().setParameter( "bob", "99" );
        assertEquals( 99L, result );
    }

    @Test
    void executeOfflineThrows()
    {
        OfflineTestShell shell = new OfflineTestShell( logger, mockedBoltStateHandler, mockedPrettyPrinter );
        when( mockedBoltStateHandler.isConnected() ).thenReturn( false );

        CommandException exception = assertThrows( CommandException.class, () -> shell.execute( "RETURN 999" ) );
        assertThat( exception.getMessage(), containsString( "Not connected to Neo4j" ) );
    }

    @Test
    void setParamShouldAddParamWithSpecialCharactersAndValue() throws ParameterException, CommandException
    {
        Value value = mock( Value.class );
        Record recordMock = mock( Record.class );
        BoltResult boltResult = new ListBoltResult( asList( recordMock ), mock( ResultSummary.class ) );

        when( mockedBoltStateHandler.runCypher( anyString(), anyMap() ) ).thenReturn( Optional.of( boltResult ) );
        when( recordMock.get( "bo`b" ) ).thenReturn( value );
        when( value.asObject() ).thenReturn( "99" );

        assertTrue( offlineTestShell.getParameterMap().allParameterValues().isEmpty() );

        Object result = offlineTestShell.getParameterMap().setParameter( "`bo``b`", "99" );
        assertEquals( 99L, result );
        assertEquals( 99L, offlineTestShell.getParameterMap().allParameterValues().get( "bo`b" ) );
    }

    @Test
    void setParamShouldAddParam() throws ParameterException, CommandException
    {
        Value value = mock( Value.class );
        Record recordMock = mock( Record.class );
        BoltResult boltResult = mock( ListBoltResult.class );

        when( mockedBoltStateHandler.runCypher( anyString(), anyMap() ) ).thenReturn( Optional.of( boltResult ) );
        when( boltResult.getRecords() ).thenReturn( asList( recordMock ) );
        when( recordMock.get( "bob" ) ).thenReturn( value );
        when( value.asObject() ).thenReturn( "99" );

        assertTrue( offlineTestShell.getParameterMap().allParameterValues().isEmpty() );

        Object result = offlineTestShell.getParameterMap().setParameter( "`bob`", "99" );
        assertEquals( 99L, result );
        assertEquals( 99L, offlineTestShell.getParameterMap().allParameterValues().get( "bob" ) );
    }

    @Test
    void executeShouldPrintResult() throws CommandException
    {
        Driver mockedDriver = mock( Driver.class );
        Session session = mock( Session.class );
        BoltResult result = mock( ListBoltResult.class );

        BoltStateHandler boltStateHandler = mock( BoltStateHandler.class );

        when( boltStateHandler.isConnected() ).thenReturn( true );
        when( boltStateHandler.runCypher( anyString(), anyMap() ) ).thenReturn( Optional.of( result ) );
        doAnswer( a ->
                  {
                      ((LinePrinter) a.getArguments()[1]).printOut( "999" );
                      return null;
                  } ).when( mockedPrettyPrinter ).format( any( BoltResult.class ), any() );
        when( mockedDriver.session() ).thenReturn( session );

        OfflineTestShell shell = new OfflineTestShell( logger, boltStateHandler, mockedPrettyPrinter );
        shell.execute( "RETURN 999" );
        verify( logger ).printOut( contains( "999" ) );
    }

    @Test
    void shouldStripEndingSemicolonsFromCommand() throws Exception
    {
        // Should not throw
        offlineTestShell.getCommandExecutable( ":help;;" ).get().execute();
        verify( logger ).printOut( contains( "Available commands:" ) );
    }

    @Test
    void shouldStripEndingSemicolonsFromCommandArgs() throws Exception
    {
        // Should not throw
        offlineTestShell.getCommandExecutable( ":help param;;" ).get().execute();
        verify( logger ).printOut( contains( "usage: " ) );
    }

    @Test
    void testStripSemicolons()
    {
        assertEquals( "", CypherShell.stripTrailingSemicolons( "" ) );
        assertEquals( "nothing", CypherShell.stripTrailingSemicolons( "nothing" ) );
        assertEquals( "", CypherShell.stripTrailingSemicolons( ";;;;;" ) );
        assertEquals( "hej", CypherShell.stripTrailingSemicolons( "hej;" ) );
        assertEquals( ";bob", CypherShell.stripTrailingSemicolons( ";bob;;" ) );
    }

    @Test
    void shouldParseCommandsAndArgs()
    {
        assertTrue( offlineTestShell.getCommandExecutable( ":help" ).isPresent() );
        assertTrue( offlineTestShell.getCommandExecutable( ":help :param" ).isPresent() );
        assertTrue( offlineTestShell.getCommandExecutable( ":param \"A piece of string\"" ).isPresent() );
        assertTrue( offlineTestShell.getCommandExecutable( ":params" ).isPresent() );
    }

    @Test
    void commandNameShouldBeParsed()
    {
        assertTrue( offlineTestShell.getCommandExecutable( "   :help    " ).isPresent() );
        assertTrue( offlineTestShell.getCommandExecutable( "   :help    \n" ).isPresent() );
        assertTrue( offlineTestShell.getCommandExecutable( "   :help   arg1 arg2 " ).isPresent() );
    }

    @Test
    void incorrectCommandsThrowException()
    {
        Optional<CommandExecutable> exe = offlineTestShell.getCommandExecutable( "   :help   arg1 arg2 " );
        CommandException exception = assertThrows( CommandException.class, () -> CypherShell.executeCmd( exe.get() ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }

    @Test
    void shouldReturnNothingOnStrangeCommand()
    {
        Optional<CommandExecutable> exe = offlineTestShell.getCommandExecutable( "   :aklxjde   arg1 arg2 " );

        assertFalse( exe.isPresent() );
    }

    @Test
    void setParameterDoesNotTriggerByBoltError() throws ParameterException, CommandException
    {
        // given
        when( mockedBoltStateHandler.runCypher( anyString(), anyMap() ) ).thenReturn( Optional.empty() );
        CypherShell shell = new CypherShell( logger, mockedBoltStateHandler, mockedPrettyPrinter, new ShellParameterMap() );

        // when
        Object result = shell.getParameterMap().setParameter( "bob", "99" );
        assertEquals( 99L, result );
    }

    @Test
    void ignoreEmptyStatement() throws CommandException
    {
        when( mockedBoltStateHandler.runCypher( anyString(), anyMap() ) ).thenThrow( new IllegalStateException( "Test failed" ) );
        OfflineTestShell shell = new OfflineTestShell( logger, mockedBoltStateHandler, mockedPrettyPrinter );

        shell.execute( "  \t;" );
    }
}
