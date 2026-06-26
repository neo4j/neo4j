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

package org.neo4j.bolt;

import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.Negotiated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.setup.preset.EnableTelemetry;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.testing.messages.factory.TelemetryMessageBuilder;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@EnableTelemetry
public class TelemetryIT {

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 3))
    void shouldFailToProcessTelemetryWhenOldBoltVersion(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.telemetry(TelemetryMessageBuilder::withExecute));

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldProcessTelemetry(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection
                .send(wire.telemetry(TelemetryMessageBuilder::withExecute))
                .send(wire.telemetry(TelemetryMessageBuilder::withUnmanagedTransactions))
                .send(wire.telemetry(TelemetryMessageBuilder::withManagedTransactionFunctions))
                .send(wire.telemetry(TelemetryMessageBuilder::withImplicitTransactions));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(4);
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldNotProcessTelemetryWhenFailed(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.run("✨✨✨ FIRE ✨✨✨"));
        connection
                .send(wire.telemetry(TelemetryMessageBuilder::withExecute))
                .send(wire.telemetry(TelemetryMessageBuilder::withUnmanagedTransactions))
                .send(wire.telemetry(TelemetryMessageBuilder::withManagedTransactionFunctions))
                .send(wire.telemetry(TelemetryMessageBuilder::withImplicitTransactions))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesFailure().receivesIgnored(5);
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldFailWhenTelemetryIsReceivedPriorToNegotiation(
            @VersionSelected BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.telemetry(TelemetryMessageBuilder::withExecute));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Message of type TelemetryMessage cannot be handled by a session in the NEGOTIATION state.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString("TelemetryMessage")
                                                .withString("NEGOTIATION"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message TelemetryMessage cannot be handled by session in the 'NEGOTIATION' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldFailWhenTelemetryIsReceivedPriorToAuthentication(
            @Negotiated BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.telemetry(TelemetryMessageBuilder::withExecute));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Message of type TelemetryMessage cannot be handled by a session in the AUTHENTICATION state.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString("TelemetryMessage")
                                                .withString("AUTHENTICATION"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message TelemetryMessage cannot be handled by session in the 'AUTHENTICATION' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7))
    void shouldFailWhenTelemetryIsInTxReady(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.begin());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.telemetry(TelemetryMessageBuilder::withExecute));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Message of type TelemetryMessage cannot be handled by a session in the IN_TRANSACTION state.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString("TelemetryMessage")
                                                .withString("IN_TRANSACTION"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message TelemetryMessage cannot be handled by session in the 'IN_TRANSACTION' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldFailWhenTelemetryIsReceivedAfterLogoff(@Authenticated BoltTestConnection connection, BoltWire wire) {
        connection
                .send(wire.telemetry(TelemetryMessageBuilder::withExecute))
                .send(wire.logoff())
                .send(wire.telemetry(TelemetryMessageBuilder::withExecute));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(2)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Message of type TelemetryMessage cannot be handled by a session in the AUTHENTICATION state.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString("TelemetryMessage")
                                                .withString("AUTHENTICATION"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message TelemetryMessage cannot be handled by session in the 'AUTHENTICATION' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 4))
    void shouldCloseConnectionWhenANonSupportedApiTypeIsSent(
            @Authenticated BoltTestConnection connection, BoltWire wire) {
        connection.send(wire.telemetry(TelemetryMessageBuilder::withANonValidAPIType));

        // FIXME: This error has been misclassified
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Database.General.UnknownError)
                        .hasLegacyMessageFuzzy("Unknown driver interface type")
                        .hasStatus(
                                GqlStatusInfoCodes.STATUS_50N00,
                                GqlMessageParameters.create()
                                        .withString("DecoderException")
                                        // FIXME: Leaking implementation detail?
                                        .withString(
                                                "org.neo4j.packstream.error.reader.PackstreamReaderException: 22N00: The provided value is unsupported and cannot be processed."))
                        .hasDescriptionFuzzy("general processing exception - internal error")
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_50N09,
                                        GqlMessageParameters.create().withString("uncaught error"))
                                .hasDescriptionFuzzy("general processing exception - invalid server state transition")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.DATABASE_ERROR))));
    }
}
