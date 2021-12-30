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

import java.util.List;
import java.util.Optional;

import org.neo4j.driver.Driver;
import org.neo4j.driver.Session;
import org.neo4j.shell.cli.Encryption;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.state.ListBoltResult;
import org.neo4j.shell.terminal.CypherShellTerminal;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.ConnectionConfig.connectionConfig;
import static org.neo4j.shell.DatabaseManager.ABSENT_DB_NAME;

class CypherShellTest
{
    private final PrettyPrinter mockedPrettyPrinter = mock( PrettyPrinter.class );
    private final BoltStateHandler mockedBoltStateHandler = mock( BoltStateHandler.class );
    private final ParameterService mockedParameterService = mock( ParameterService.class );
    private final Printer printer = mock( Printer.class );
    private OfflineTestShell offlineTestShell;

    @BeforeEach
    void setup()
    {
        when( mockedBoltStateHandler.getProtocolVersion() ).thenReturn( "" );

        offlineTestShell = new OfflineTestShell( printer, mockedBoltStateHandler, mockedPrettyPrinter );

        CommandHelper commandHelper = new CommandHelper(
                printer,
                Historian.empty,
                offlineTestShell,
                mock( CypherShellTerminal.class ),
                mock( ParameterService.class )
        );

        offlineTestShell.setCommandHelper( commandHelper );
    }

    @Test
    void verifyDelegationOfConnectionMethods() throws CommandException
    {
        ConnectionConfig cc = connectionConfig( "bolt", "", 1, "", "", Encryption.DEFAULT, ABSENT_DB_NAME, new Environment() );
        CypherShell shell = new CypherShell( printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService );

        shell.connect( cc );
        verify( mockedBoltStateHandler ).connect( cc );

        shell.isConnected();
        verify( mockedBoltStateHandler ).isConnected();
    }

    @Test
    void verifyDelegationOfResetMethod()
    {
        CypherShell shell = new CypherShell( printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService );

        shell.reset();
        verify( mockedBoltStateHandler ).reset();
    }

    @Test
    void verifyDelegationOfGetProtocolVersionMethod()
    {
        CypherShell shell = new CypherShell( printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService );

        shell.getProtocolVersion();
        verify( mockedBoltStateHandler ).getProtocolVersion();
    }

    @Test
    void verifyDelegationOfIsTransactionOpenMethod()
    {
        CypherShell shell = new CypherShell( printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService );

        shell.isTransactionOpen();
        verify( mockedBoltStateHandler ).isTransactionOpen();
    }

    @Test
    void verifyDelegationOfTransactionMethods() throws CommandException
    {
        CypherShell shell = new CypherShell( printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService );

        shell.beginTransaction();
        verify( mockedBoltStateHandler ).beginTransaction();

        shell.commitTransaction();
        verify( mockedBoltStateHandler ).commitTransaction();

        shell.rollbackTransaction();
        verify( mockedBoltStateHandler ).rollbackTransaction();
    }

    @Test
    void executeOfflineThrows()
    {
        OfflineTestShell shell = new OfflineTestShell( printer, mockedBoltStateHandler, mockedPrettyPrinter );
        when( mockedBoltStateHandler.isConnected() ).thenReturn( false );

        CommandException exception = assertThrows( CommandException.class, () -> shell.execute( new CypherStatement( "RETURN 999;", true, 0, 0 ) ) );
        assertThat( exception.getMessage(), containsString( "Not connected to Neo4j" ) );
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

        OfflineTestShell shell = new OfflineTestShell( printer, boltStateHandler, mockedPrettyPrinter );
        shell.execute( new CypherStatement( "RETURN 999;", true, 0, 0 ) );
        verify( printer ).printOut( contains( "999" ) );
    }

    @Test
    void incorrectCommandsThrowException()
    {
        var statement = new CommandStatement( ":help", List.of( "arg1", "arg2" ), true, 0, 0 );
        CommandException exception = assertThrows( CommandException.class, () -> offlineTestShell.execute( statement ) );
        assertThat( exception.getMessage(), containsString( "Incorrect number of arguments" ) );
    }
}
