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

import java.io.IOException;
import java.util.Map;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Authenticated;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.preset.DisableUnixUserDatabaseAccess;
import org.neo4j.bolt.test.annotation.test.ProtocolTest;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.boltmessages.request.connection.RoutingContext;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@DisableUnixUserDatabaseAccess
@IncludeTransport(TransportType.UNIX)
@DisabledOnOs(OS.WINDOWS)
public class UnixDomainSocketIT {

    /**
     * Evaluates whether clients will receive an error when they indicate that routing should be
     * enabled (e.g., a neo4j:// URI is used instead of bolt:// to connect to the connector).
     */
    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7))
    void shouldForceDisableRouting(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(msg -> msg.withoutAuth().withRoutingContext(new RoutingContext(true, Map.of()))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Routing is not supported on this connector")
                        .hasStatus(GqlStatusInfoCodes.STATUS_51N78)
                        .hasDescriptionFuzzy(
                                "Routing is not permitted via this connector. Switch the connection URI scheme to bolt:// or connect to a connector with routing support.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)));
    }

    @ProtocolTest // v4.0 did not support routing contexts
    @IncludeWire(since = @Version(major = 4, minor = 1), until = @Version(major = 5, minor = 6))
    void shouldForceDisableRoutingV50(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello(msg -> msg.withoutAuth().withRoutingContext(new RoutingContext(true, Map.of()))));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage("Routing is not supported on this connector")
                        .hasStatus(GqlStatusInfoCodes.STATUS_51N78)
                        .hasDescription("Routing is not supported on this connector"));
    }

    /**
     * Evaluates whether clients will receive an error when they indicate that they wish to interact
     * with a user database through implicit transactions.
     */
    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldPreventUserDatabaseAccessViaImplicitTransactionV40(
            BoltWire wire, @Authenticated BoltTestConnection connection) throws IOException {
        connection.send(wire.run("RETURN 1", msg -> msg.withDatabase("neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Cannot access database \"neo4j\": Only system database access is permitted via UNIX domain sockets")
                        .hasStatus(GqlStatusInfoCodes.STATUS_51N79)
                        .hasDescription("Only system database access is permitted"));
    }

    @ProtocolTest
    @IncludeWire(since = @Version(major = 5, minor = 7))
    void shouldPreventUserDatabaseAccessViaImplicitTransaction(
            BoltWire wire, @Authenticated BoltTestConnection connection) throws IOException {
        connection.send(wire.run("RETURN 1", msg -> msg.withDatabase("neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Cannot access database \"neo4j\": Only system database access is permitted via UNIX domain sockets")
                        .hasStatus(
                                GqlStatusInfoCodes.STATUS_51N79,
                                GqlMessageParameters.create().withString("neo4j"))
                        .hasDescription(
                                "error: system configuration or operation exception - database unavailable. Access to database `$db` is not permitted via this connector.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)));
    }

    /**
     * Evaluates whether clients will receive an error when they indicate that they wish to interact
     * with a user database through explicit transactions.
     */
    @ProtocolTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldPreventUserDatabaseAccessViaExplicitTransaction(
            BoltWire wire, @Authenticated BoltTestConnection connection) throws IOException {
        connection.send(wire.begin(msg -> msg.withDatabase("neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Cannot access database \"neo4j\": Only system database access is permitted via UNIX domain sockets")
                        .hasStatus(GqlStatusInfoCodes.STATUS_51N79)
                        .hasDescription("Only system database access is permitted"));
    }
}
