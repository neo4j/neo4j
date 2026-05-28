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
package org.neo4j.bolt.authentication;

import java.util.Map;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.messages.AbstractBoltWire;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.packstream.io.PackstreamBuf;
import org.neo4j.packstream.struct.StructHeader;
import org.neo4j.test.extension.OtherThreadExtension;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExtendWith(OtherThreadExtension.class)
@ExcludeWire(until = @Version(major = 5, minor = 2))
public class BoltAgentIT {

    @ProtocolTest
    void shouldSucceedWhenAddingExtraValues(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withBoltAgent(Map.of("product", "test-agent", "extra", "included"))));
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentIsOmitted(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none", "user_agent", "ignore"))
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.")
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
                                                        .withString("null")
                                                        .withList("MAP<STRING, STRING>")
                                                        .withString("null"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value null to be of type MAP<STRING, STRING>, but was of type null.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenInvalidBoltAgentIsGiven(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent("42L")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.")
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
                                                        .withString("42L")
                                                        .withList("MAP<STRING, STRING>")
                                                        .withString("String"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 42L to be of type MAP<STRING, STRING>, but was of type String.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentInvalidValues(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent(Map.of("product", 1))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"bolt_agent\": Must be a map with string keys and string values.")
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
                                                        .withString("product=1")
                                                        .withList("MAP<STRING, STRING>")
                                                        .withString("class java.util.HashMap$Node"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value product=1 to be of type MAP<STRING, STRING>, but was of type class java.util.HashMap$Node.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    void shouldFailWhenBoltAgentMissingProduct(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withScheme("none").withBadBoltAgent(Map.of("invalid", "value"))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Illegal value for field \"bolt_agent\": Expected map to contain key: 'product'.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N55,
                                        GqlMessageParameters.create()
                                                .withString("product")
                                                .withString("bolt_agent"))
                                .hasDescription(
                                        "error: data exception - required key missing from map. Map requires key 'product' but was missing from field `bolt_agent`.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }
}
