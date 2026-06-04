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
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.test.TransportTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.boltmessages.request.transaction.RunMessage;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

/**
 * Evaluates whether Bolt correctly handles invalid messages.
 */
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class MalformedMessageIT {

    @TransportTest
    void shouldHandleIncorrectFraming(BoltWire wire, @Connected BoltTestConnection connection) {
        // Given I have a message that gets truncated in the chunking, so part of it is missing
        var msg = wire.run("UNWIND [1,2,3] AS a RETURN a, a * a AS a_squared");
        var truncated = msg.readSlice(msg.readableBytes() - 12);

        // When
        connection.connect().sendDefaultProtocolVersion().send(truncated);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .negotiatesDefaultVersion()
                .isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldHandleMessagesWithIncorrectFields(@VersionSelected BoltTestConnection connection) {
        // Given I send a message with the wrong types in its fields
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeString("RETURN 1")
                .writeMapHeader(0)
                .writeInt(42);

        // When
        connection.send(msg);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"metadata\": Unexpected type: Expected MAP but got INT")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasDescription("error: data exception - invalid value type")
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N01,
                                                GqlMessageParameters.create()
                                                        .withInt(0)
                                                        .withString("MAP")
                                                        .withString("INT"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 0 to be of type MAP, but was of type INT.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldHandleUnknownMarkerBytesV40(@VersionSelected BoltTestConnection connection) {
        // Given I send a message with an invalid type
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeMarkerByte(0xC7)
                .writeMapHeader(0)
                .writeMapHeader(0);

        // When
        connection.send(msg);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Unexpected type: RESERVED")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasDescription("error: data exception - invalid value type")
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N01,
                                                GqlMessageParameters.create()
                                                        .withString("0")
                                                        .withList("MAP")
                                                        .withString("INT"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 0 to be of type MAP, but was of type INT.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7))
    void shouldHandleUnknownMarkerBytesV5x7(@VersionSelected BoltTestConnection connection) {
        // Given I send a message with an invalid type
        var msg = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, RunMessage.SIGNATURE))
                .writeMarkerByte(0xC7)
                .writeMapHeader(0)
                .writeMapHeader(0);

        // When
        connection.send(msg);

        // Then
        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Unexpected type: RESERVED")
                        .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                        .hasDescription("error: data exception - invalid value type")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N01,
                                        GqlMessageParameters.create()
                                                .withString("RESERVED")
                                                .withList(
                                                        "BYTES", "BOOLEAN", "FLOAT", "INT", "LIST", "MAP", "STRING",
                                                        "UUID", "STRUCT")
                                                .withString("RESERVED"))
                                .hasDescriptionFuzzy(
                                        "error: data exception - invalid type. Expected the value RESERVED to be of type")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @TransportTest
    void shouldCloseConnectionOnInvalidHandshake(@Connected BoltTestConnection connection) {

        // GIVEN
        connection.sendRaw(new byte[] {
            (byte) 0xDE, (byte) 0xAD, (byte) 0xB0, (byte) 0x17, 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
        });

        // THEN
        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }
}
