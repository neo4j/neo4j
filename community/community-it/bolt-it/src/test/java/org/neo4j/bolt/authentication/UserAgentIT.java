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
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
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
public class UserAgentIT {

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 8))
    void shouldFailWhenUserAgentIsOmittedV5x7(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none"))
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"user_agent\": Expected value to be non-null")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N05,
                                        GqlMessageParameters.create()
                                                .withString("null")
                                                .withString("field 'user_agent'"))
                                .hasDescription(
                                        "error: data exception - input failed validation. Invalid input 'null' for field 'user_agent'.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(GqlStatusInfoCodes.STATUS_22004)
                                        .hasDescription("error: data exception - null value not allowed")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailWhenUserAgentIsOmitted(@VersionSelected BoltTestConnection connection) {
        connection.send(PackstreamBuf.allocUnpooled()
                .writeStructHeader(new StructHeader(1, AbstractBoltWire.MESSAGE_TAG_HELLO))
                .writeMap(Map.of("scheme", "none"))
                .raw());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"user_agent\": Expected value to be non-null")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_22N05,
                                        GqlMessageParameters.create()
                                                .withString("null")
                                                .withString("field 'user_agent'"))
                                .hasDescription(
                                        "error: data exception - input failed validation. Invalid input 'null' for field 'user_agent'.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))
                                .hasCause(FailureCauseAssertions.create()
                                        .hasStatus(GqlStatusInfoCodes.STATUS_22004)
                                        .hasDescription("error: data exception - null value not allowed")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 8))
    void shouldFailWhenInvalidUserAgentIsGivenV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withScheme("none").withUserAgent(42L)));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"user_agent\": Expected string")
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
                                                        .withInt(42)
                                                        .withList("STRING")
                                                        .withString("Long"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 42 to be of type STRING, but was of type Long.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailWhenInvalidUserAgentIsGiven(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(x -> x.withScheme("none").withUserAgent(42L)));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Illegal value for field \"user_agent\": Expected string")
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
                                                        .withInt(42)
                                                        .withList("STRING")
                                                        .withString("Long"))
                                        .hasDescription(
                                                "error: data exception - invalid type. Expected the value 42 to be of type STRING, but was of type Long.")
                                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                .hasClassification(ErrorClassification.CLIENT_ERROR)))));
    }
}
