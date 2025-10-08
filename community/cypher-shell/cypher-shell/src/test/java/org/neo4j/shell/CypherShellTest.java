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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.contains;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.completions.DbInfo;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.Printer;
import org.neo4j.shell.state.BoltResult;
import org.neo4j.shell.state.BoltStateHandler;
import org.neo4j.shell.state.LicenseDetails;
import org.neo4j.shell.state.ListBoltResult;
import org.neo4j.shell.terminal.CypherShellTerminal;

class CypherShellTest {
    private final PrettyPrinter mockedPrettyPrinter = mock(PrettyPrinter.class);
    private final ParameterService mockedParameterService = mock(ParameterService.class);
    private BoltStateHandler mockedBoltStateHandler;
    private DbInfo mockedDbInfo;
    private Printer printer;
    private OfflineTestShell offlineTestShell;

    @BeforeEach
    void setup() {
        mockedBoltStateHandler = mock(BoltStateHandler.class);
        mockedDbInfo = mock(DbInfo.class);

        printer = mock(Printer.class);
        when(mockedBoltStateHandler.getProtocolVersion()).thenReturn("");

        offlineTestShell = new OfflineTestShell(printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter);

        CommandHelper commandHelper = new CommandHelper(
                printer,
                Historian.empty,
                offlineTestShell,
                mock(CypherShellTerminal.class),
                mock(ParameterService.class));

        offlineTestShell.setCommandHelper(commandHelper);
    }

    @Test
    void verifyDelegationOfConnectionMethods() throws CommandException {
        ConnectionConfig cc = testConnectionConfig("bolt://localhost:1");
        CypherShell shell = new CypherShell(
                printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter, mockedParameterService);

        shell.connect(cc);
        verify(mockedBoltStateHandler).connect(cc);

        shell.isConnected();
        verify(mockedBoltStateHandler).isConnected();
    }

    @Test
    void verifyDelegationOfResetMethod() {
        CypherShell shell = new CypherShell(
                printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter, mockedParameterService);

        shell.reset();
        verify(mockedBoltStateHandler).reset();
    }

    @Test
    void verifyDelegationOfGetProtocolVersionMethod() {
        CypherShell shell = new CypherShell(
                printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter, mockedParameterService);

        shell.getProtocolVersion();
        verify(mockedBoltStateHandler).getProtocolVersion();
    }

    @Test
    void verifyDelegationOfIsTransactionOpenMethod() {
        CypherShell shell = new CypherShell(
                printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter, mockedParameterService);

        shell.isTransactionOpen();
        verify(mockedBoltStateHandler).isTransactionOpen();
    }

    @Test
    void verifyDelegationOfTransactionMethods() throws CommandException {
        CypherShell shell = new CypherShell(
                printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter, mockedParameterService);

        shell.beginTransaction();
        verify(mockedBoltStateHandler).beginTransaction();

        shell.commitTransaction();
        verify(mockedBoltStateHandler).commitTransaction();

        shell.rollbackTransaction();
        verify(mockedBoltStateHandler).rollbackTransaction();
    }

    @Test
    void executeOfflineThrows() {
        OfflineTestShell shell =
                new OfflineTestShell(printer, mockedBoltStateHandler, mockedDbInfo, mockedPrettyPrinter);
        when(mockedBoltStateHandler.isConnected()).thenReturn(false);

        CommandException exception = assertThrows(
                CommandException.class, () -> shell.execute(new CypherStatement("RETURN 999;", true, 0, 0)));
        assertThat(exception).hasMessageContaining("Not connected to Neo4j");
    }

    @Test
    void executeShouldPrintResult() throws CommandException {
        BoltResult result = mock(ListBoltResult.class);

        BoltStateHandler boltStateHandler = mock(BoltStateHandler.class);

        when(boltStateHandler.isConnected()).thenReturn(true);
        when(boltStateHandler.runUserCypher(anyString(), anyMap())).thenReturn(Optional.of(result));
        doAnswer(a -> {
                    ((LinePrinter) a.getArguments()[1]).printOut("999");
                    return null;
                })
                .when(mockedPrettyPrinter)
                .format(any(BoltResult.class), any());

        OfflineTestShell shell = new OfflineTestShell(printer, boltStateHandler, mockedDbInfo, mockedPrettyPrinter);
        shell.execute(new CypherStatement("RETURN 999;", true, 0, 0));
        verify(printer).printOut(contains("999"));
    }

    @Test
    void incorrectCommandsThrowException() {
        var statement = new CommandStatement(":help", List.of("arg1", "arg2"), true, 0, 0);
        CommandException exception = assertThrows(CommandException.class, () -> offlineTestShell.execute(statement));
        assertThat(exception).hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void printLicenseExpired() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("expired", -1, 120));
        offlineTestShell.printLicenseWarnings();
        verify(printer).printOut(contains("This is a time limited trial, and the\n120 days have expired"));
        verify(printer, times(0)).printIfVerbose(anyString());
    }

    @Test
    void printLicenseAccepted() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("yes", 0, 0));
        offlineTestShell.printLicenseWarnings();
        verify(printer, times(0)).printOut(anyString());
        verify(printer, times(0)).printIfVerbose(anyString());
    }

    @Test
    void printLicenseNotAccepted() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("no", -1, 120));
        offlineTestShell.printLicenseWarnings();
        verify(printer).printOut(contains("A Neo4j license has not been accepted"));
        verify(printer, times(0)).printIfVerbose(anyString());
    }

    @Test
    void printLicenseDaysLeft() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("eval", 2, 30));
        offlineTestShell.printLicenseWarnings();
        verify(printer).printOut(contains("This is a time limited trial.\nYou have 2 days remaining out of 30 days."));
        verify(printer, times(0)).printIfVerbose(anyString());
    }

    @Test
    void printFallbackWarningScheme() {
        final var oldConnection = testConnectionConfig("neo4j://hello.hi:1");
        final var newConnection = testConnectionConfig("bolt://hello.hi:1");
        when(mockedBoltStateHandler.connectionConfig()).thenReturn(newConnection);
        offlineTestShell.printFallbackWarning(oldConnection.uri());
        verify(printer)
                .printIfVerbose(contains("Failed to connect to neo4j://hello.hi:1, fallback to bolt://hello.hi:1"));
        verify(printer, times(0)).printOut(anyString());
    }

    @Test
    void doNotPrintFallbackWarningScheme() {
        final var connection = testConnectionConfig("neo4j://hello.hi:1");
        when(mockedBoltStateHandler.connectionConfig()).thenReturn(connection);
        offlineTestShell.printFallbackWarning(connection.uri());
        verify(printer, times(0)).printIfVerbose(anyString());
        verify(printer, times(0)).printOut(anyString());
    }
}
