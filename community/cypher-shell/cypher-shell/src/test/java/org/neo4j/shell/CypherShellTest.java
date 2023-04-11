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

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.shell.test.Util.testConnectionConfig;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.List;
import java.util.Optional;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.shell.cli.Format;
import org.neo4j.shell.commands.CommandHelper;
import org.neo4j.shell.exception.CommandException;
import org.neo4j.shell.parameter.ParameterService;
import org.neo4j.shell.parser.StatementParser.CommandStatement;
import org.neo4j.shell.parser.StatementParser.CypherStatement;
import org.neo4j.shell.prettyprint.LinePrinter;
import org.neo4j.shell.prettyprint.PrettyPrinter;
import org.neo4j.shell.printer.AnsiPrinter;
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
    private ByteArrayOutputStream out = new ByteArrayOutputStream();
    private ByteArrayOutputStream err = new ByteArrayOutputStream();
    private Printer printer;
    private OfflineTestShell offlineTestShell;

    @BeforeEach
    void setup() {
        out.reset();
        err.reset();
        mockedBoltStateHandler = mock(BoltStateHandler.class);
        printer = new AnsiPrinter(Format.VERBOSE, new PrintStream(out), new PrintStream(err), true);
        when(mockedBoltStateHandler.getProtocolVersion()).thenReturn("");

        offlineTestShell = new OfflineTestShell(printer, mockedBoltStateHandler, mockedPrettyPrinter);

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
        CypherShell shell =
                new CypherShell(printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService);

        shell.connect(cc);
        verify(mockedBoltStateHandler).connect(cc);

        shell.isConnected();
        verify(mockedBoltStateHandler).isConnected();
    }

    @Test
    void verifyDelegationOfResetMethod() {
        CypherShell shell =
                new CypherShell(printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService);

        shell.reset();
        verify(mockedBoltStateHandler).reset();
    }

    @Test
    void verifyDelegationOfGetProtocolVersionMethod() {
        CypherShell shell =
                new CypherShell(printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService);

        shell.getProtocolVersion();
        verify(mockedBoltStateHandler).getProtocolVersion();
    }

    @Test
    void verifyDelegationOfIsTransactionOpenMethod() {
        CypherShell shell =
                new CypherShell(printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService);

        shell.isTransactionOpen();
        verify(mockedBoltStateHandler).isTransactionOpen();
    }

    @Test
    void verifyDelegationOfTransactionMethods() throws CommandException {
        CypherShell shell =
                new CypherShell(printer, mockedBoltStateHandler, mockedPrettyPrinter, mockedParameterService);

        shell.beginTransaction();
        verify(mockedBoltStateHandler).beginTransaction();

        shell.commitTransaction();
        verify(mockedBoltStateHandler).commitTransaction();

        shell.rollbackTransaction();
        verify(mockedBoltStateHandler).rollbackTransaction();
    }

    @Test
    void executeOfflineThrows() {
        OfflineTestShell shell = new OfflineTestShell(printer, mockedBoltStateHandler, mockedPrettyPrinter);
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

        OfflineTestShell shell = new OfflineTestShell(printer, boltStateHandler, mockedPrettyPrinter);
        shell.execute(new CypherStatement("RETURN 999;", true, 0, 0));
        assertOutput("999\n");
    }

    @Test
    void incorrectCommandsThrowException() {
        var statement = new CommandStatement(":help", List.of("arg1", "arg2"), true, 0, 0);
        CommandException exception = assertThrows(CommandException.class, () -> offlineTestShell.execute(statement));
        assertThat(exception).hasMessageContaining("Incorrect number of arguments");
    }

    @Test
    void printLicenseExpired() {
        printer.setFormat(Format.PLAIN);
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("expired", -1, 120));
        offlineTestShell.printLicenseWarnings();
        assertOutput(
                """
                        [33mThank you for installing Neo4j. This is a time limited trial, and the
                        120 days have expired. Please contact https://neo4j.com/contact-us/
                        to continue using the software. Use of this Software without
                        a proper commercial or evaluation license with Neo4j, Inc. or
                        its affiliates is prohibited.
                        [m
                        """);
    }

    @Test
    void printLicenseAccepted() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("yes", 0, 0));
        offlineTestShell.printLicenseWarnings();
        assertOutput("");
    }

    @Test
    void printLicenseNotAccepted() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("no", -1, 120));
        offlineTestShell.printLicenseWarnings();
        assertOutput(
                """
                        [33mA Neo4j license has not been accepted. To accept the commercial license agreement, run
                            neo4j-admin server license --accept-commercial.
                        To accept the terms of the evaluation agreement, run
                            neo4j-admin server license --accept-evaluation.

                        (c) Neo4j Sweden AB. All Rights Reserved.
                        Use of this Software without a proper commercial license, or evaluation license
                        with Neo4j, Inc. or its affiliates is prohibited.
                        Neo4j has the right to terminate your usage if you are not compliant.

                        Please contact us about licensing via https://neo4j.com/contact-us/
                        [m
                        """);
    }

    @Test
    void printLicenseDaysLeft() {
        when(mockedBoltStateHandler.licenseDetails()).thenReturn(LicenseDetails.parse("eval", 2, 30));
        offlineTestShell.printLicenseWarnings();
        assertOutput(
                """
                        Thank you for installing Neo4j. This is a time limited trial.
                        You have 2 days remaining out of 30 days. Please
                        contact https://neo4j.com/contact-us/ if you require more time.

                        """);
    }

    @Test
    void printFallbackWarningScheme() {
        final var oldConnection = testConnectionConfig("neo4j://hello.hi:1");
        final var newConnection = testConnectionConfig("bolt://hello.hi:1");
        when(mockedBoltStateHandler.connectionConfig()).thenReturn(newConnection);
        offlineTestShell.printFallbackWarning(oldConnection.uri());
        assertOutput(
                """
                        [33mFailed to connect to neo4j://hello.hi:1, fallback to bolt://hello.hi:1[m
                        """);
    }

    @Test
    void doNotPrintFallbackWarningScheme() {
        final var connection = testConnectionConfig("neo4j://hello.hi:1");
        when(mockedBoltStateHandler.connectionConfig()).thenReturn(connection);
        offlineTestShell.printFallbackWarning(connection.uri());
        assertOutput("");
    }

    private void assertOutput(String expected) {
        assertOutput(expected, "");
    }

    private void assertOutput(String expectedOut, String expectedErr) {
        assertEquals(expectedOut, out.toString(UTF_8).replace("\r", ""));
        assertEquals(expectedErr, err.toString());
    }
}
