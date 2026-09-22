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
package org.neo4j.bolt.tx;

import java.util.List;
import java.util.Map;
import org.neo4j.bolt.protocol.io.StructType;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.BoltV40Wire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class TransactionInitiatingMessageIT {
    private static final short MESSAGE_TAG_BEGIN = (short) 0x11;

    @ProtocolTest
    void shouldFailWhenAccessModeIsInvalid(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(Map.of("mode", "x"))
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"metadata\": Illegal value for field \"mode\": Expecting access mode value to be 'r' or 'w'")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasDescription("error: connection exception - protocol error. "
                                        + "General network protocol error.")
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N04,
                                                GqlMessageParameters.create()
                                                        .withString("x")
                                                        .withString("access mode value")
                                                        .withList("r", "w"))
                                        .hasDescription("error: data exception - invalid input value. Invalid input "
                                                + "'x' for access mode value. Expected 'r' or 'w'.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenBookmarkIsNotAString(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(1)
                .writeString("bookmarks")
                .writeListHeader(1)
                .writeInt(42)
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"bookmarks\": Expected list of strings")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                .hasDescription("error: data exception - invalid value type")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N01,
                                                GqlMessageParameters.create()
                                                        .withString("")
                                                        .withList("LIST<STRING>")
                                                        .withString("Long"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value  to be of type LIST<STRING>, but was of type Long.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenTransactionMetadataContainsNode(BoltWire wire, @Authenticated BoltTestConnection connection) {
        var metadata = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(1)
                .writeString("tx_metadata")
                .writeMapHeader(1)
                .writeString("node");

        wire.nodeValue(metadata, "42", 42, List.of("Node"));

        connection.send(metadata.raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"metadata\": Unexpected struct tag: 0x4E")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22N00)
                                .hasDescription(
                                        "error: data exception - unsupported value. The provided value is unsupported and cannot be processed.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N97,
                                                GqlMessageParameters.create().withString("0x4E"))
                                        .hasDescription(
                                                "error: data exception - unexpected struct tag. Unexpected struct tag: 0x4E.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenTransactionMetadataContainsRelationship(
            BoltWire wire, @Authenticated BoltTestConnection connection) {
        var metadata = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(1)
                .writeString("tx_metadata")
                .writeMapHeader(1)
                .writeString("relationship");

        wire.relationshipValue(metadata, "42", 42, "1", 1, "2", 2, "REL");

        connection.send(metadata.raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"metadata\": Unexpected struct tag: 0x52")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22N00)
                                .hasDescription(
                                        "error: data exception - unsupported value. The provided value is unsupported and cannot be processed.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N97,
                                                GqlMessageParameters.create().withString("0x52"))
                                        .hasDescription(
                                                "error: data exception - unexpected struct tag. Unexpected struct tag: 0x52.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenTransactionMetadataContainsPath(BoltWire wire, @Authenticated BoltTestConnection connection) {
        var metadata = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(1)
                .writeString("tx_metadata")
                .writeMapHeader(1)
                .writeString("path")
                .writeStructHeader(new StructHeader(3, StructType.PATH.getTag()));

        metadata.writeListHeader(2);
        wire.nodeValue(metadata, "1", 1, List.of("Start"));
        wire.nodeValue(metadata, "2", 2, List.of("End"));

        metadata.writeListHeader(1);
        wire.unboundRelationshipValue(metadata, "3", 3, "REL");

        metadata.writeListHeader(2);
        metadata.writeInt(1);
        metadata.writeInt(1);

        connection.send(metadata.raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"metadata\": Unexpected struct tag: 0x50")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22N00)
                                .hasDescription(
                                        "error: data exception - unsupported value. The provided value is unsupported and cannot be processed.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(
                                                GqlStatusInfoCodes.STATUS_22N97,
                                                GqlMessageParameters.create().withString("0x50"))
                                        .hasDescription(
                                                "error: data exception - unexpected struct tag. Unexpected struct tag: 0x50.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenTimeoutIsWrongType(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMapHeader(1)
                .writeString("tx_timeout")
                .writeString("not-a-timeout")
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessageFuzzy(
                                "Illegal value for field \"metadata\": Illegal value for field \"tx_timeout\": Expected long")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasDescription(
                                        "error: connection exception - protocol error. General network protocol error.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                        .hasDescription("error: data exception - invalid value type")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR))
                                        .hasCause(FailureCauseAssertions.create()
                                                .hasStatus(
                                                        GqlStatusInfoCodes.STATUS_22N01,
                                                        GqlMessageParameters.create()
                                                                .withString("String(\"not-a-timeout\")")
                                                                .withString("LONG")
                                                                .withString("String"))
                                                .hasDescription(
                                                        "error: data exception - invalid type. Expected the value String(\"not-a-timeout\") to be of type LONG, but was of type String.")
                                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                        .hasClassification(ErrorClassification.CLIENT_ERROR))))));
    }

    @ProtocolTest
    void shouldFailWhenTransactionMetadataIsWrongType(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, MESSAGE_TAG_BEGIN))
                .writeMap(Map.of("tx_metadata", "not-metadata"))
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"metadata\": Illegal value for field \"tx_metadata\": Expected dictionary")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasDescription(
                                        "error: connection exception - protocol error. General network protocol error.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                        .hasDescription("error: data exception - invalid value type")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR))
                                        .hasCause(FailureCauseAssertions.create()
                                                .hasStatus(
                                                        GqlStatusInfoCodes.STATUS_22N01,
                                                        GqlMessageParameters.create()
                                                                .withString("String(\"not-metadata\")")
                                                                .withString("MAP")
                                                                .withString("String"))
                                                .hasDescription(
                                                        "error: data exception - invalid type. Expected the value String(\"not-metadata\") to be of type MAP, but was of type String.")
                                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                        .hasClassification(ErrorClassification.CLIENT_ERROR))))));
    }

    @ProtocolTest
    void shouldFailWhenMetadataMapCannotBeDecoded(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_BEGIN))
                .writeString("not-a-map")
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        FailureMetadataAssertions.create()
                                .hasLegacyStatus(Status.Request.Invalid)
                                .hasLegacyMessage(
                                        "Illegal value for field \"metadata\": Unexpected type: Expected MAP but got STRING")
                                .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                                .hasDescription(
                                        "error: connection exception - protocol error. General network protocol error.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(
                                        FailureCauseAssertions.create()
                                                .hasStatus(GqlStatusInfoCodes.STATUS_22G03)
                                                .hasDescription("error: data exception - invalid value type")
                                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                                .hasCause(
                                                        FailureCauseAssertions.create()
                                                                .hasStatus(
                                                                        GqlStatusInfoCodes.STATUS_22N01,
                                                                        GqlMessageParameters.create()
                                                                                .withInt(128)
                                                                                .withString("MAP")
                                                                                .withString("STRING"))
                                                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                                        .hasClassification(
                                                                                ErrorClassification.CLIENT_ERROR))
                                                                .hasDescription(
                                                                        "error: data exception - invalid type. Expected the value 128 to be of type MAP, but was of type STRING."))));
    }

    @ProtocolTest
    void shouldAcceptValidTransactionMetadata(BoltWire wire, @Authenticated BoltTestConnection connection) {
        var txMetadata = Map.of("useful_meta", "critical_tx");
        connection.send(wire.begin(beginMessageBuilder -> beginMessageBuilder.withTxMetadata(txMetadata)));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }
}
