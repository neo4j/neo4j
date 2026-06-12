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

import static org.neo4j.values.storable.Values.longValue;
import static org.neo4j.values.storable.Values.stringValue;

import java.util.List;
import java.util.function.Consumer;
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
import org.neo4j.values.virtual.MapValueBuilder;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
public class ProtocolViolationIT {

    private static void sendRun(BoltTestConnection connection, Consumer<PackstreamBuf> packer) {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("x");

        packer.accept(buf);

        connection.send(buf.writeMapHeader(0) // extra
                .raw());
    }

    @ProtocolTest
    void shouldFailWhenNullKeyIsSent(@Authenticated BoltTestConnection connection) {
        sendRun(connection, buf -> buf.writeMapHeader(1).writeNull().writeString("foo"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"params\": Unexpected type: Expected STRING but got NONE")
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
                                                        .withInt(192)
                                                        .withList("STRING")
                                                        .withString("NONE"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 192 to be of type STRING, but was of type NONE.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenDuplicateKeyIsSent(@Authenticated BoltTestConnection connection) {
        sendRun(
                connection,
                buf -> buf.writeMapHeader(2)
                        .writeString("foo")
                        .writeString("bar")
                        .writeString("foo")
                        .writeString("changed_my_mind"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"params\": Duplicate map key: \"foo\"")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N54,
                                        GqlMessageParameters.create().withString("foo"))
                                .hasDescription(
                                        "error: data exception - invalid map. Multiple conflicting entries specified for 'foo'.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @ProtocolTest
    void shouldFailWhenNodeIsSentWithRun(BoltWire wire, @Authenticated BoltTestConnection connection) {
        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        sendRun(connection, buf -> wire.nodeValue(buf, "42", 42, List.of("Broken", "Dreams")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"params\": Unexpected struct tag: 0x4E")
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
    void shouldFailWhenRelationshipIsSentWithRun(BoltWire wire, @Authenticated BoltTestConnection connection) {
        var properties = new MapValueBuilder();
        properties.add("the_answer", longValue(42));
        properties.add("one_does_not_simply", stringValue("break_decoding"));

        sendRun(connection, buf -> wire.relationshipValue(buf, "42", 42, "21", 21, "84", 84, "RUINS_EXPECTATIONS"));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"params\": Unexpected struct tag: 0x52")
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
    void shouldFailWhenPathIsSentWithRun(BoltWire wire, @Authenticated BoltTestConnection connection) {
        sendRun(connection, buf -> {
            buf.writeStructHeader(new StructHeader(3, StructType.PATH.getTag()));

            buf.writeListHeader(2);
            wire.nodeValue(buf, "42", 42, List.of("Computer"));
            wire.nodeValue(buf, "84", 84, List.of("Vendor"));

            buf.writeListHeader(1);
            wire.unboundRelationshipValue(buf, "13", 13, "MAKES");

            buf.writeListHeader(2);
            buf.writeInt(1);
            buf.writeInt(1);
        });

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"params\": Unexpected struct tag: 0x50")
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

        connection.send(wire.reset());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldTerminateConnectionWhenUnknownMessageIsSent(@Authenticated BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, (short) 0x42))
                .writeListHeader(1)
                .writeInt(42));

        BoltConnectionAssertions.assertThat(connection).isEventuallyTerminated();
    }

    @ProtocolTest
    void shouldFailWhenTryToStartTransactionInFailedState(@Authenticated BoltTestConnection connection) {
        var buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(3, BoltV40Wire.MESSAGE_TAG_RUN))
                .writeString("RETURN $x") // statement
                .writeMapHeader(1) // parameters
                .writeString("foo")
                .writeString("bar");
        connection.send(buf.writeMapHeader(0) // extra
                .raw());

        BoltConnectionAssertions.assertThat(connection).receivesFailure();

        buf = PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, BoltV40Wire.MESSAGE_TAG_BEGIN))
                .writeMapHeader(0); // metadata
        connection.send(buf.writeMapHeader(0) // extra
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessageFuzzy("cannot be handled by session in the READY state")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString(
                                                        "BeginMessage{bookmarks=[], txTimeout=null, accessMode=WRITE, txMetadata=null, databaseName='null', impersonatedUser='null', notificationsConfig=Default, type=EXPLICIT}")
                                                .withString("READY"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message BeginMessage{bookmarks=[], txTimeout=null, accessMode=WRITE, txMetadata=null, databaseName='null', impersonatedUser='null', notificationsConfig=Default, type=EXPLICIT} cannot be handled by session in the 'READY' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }
}
