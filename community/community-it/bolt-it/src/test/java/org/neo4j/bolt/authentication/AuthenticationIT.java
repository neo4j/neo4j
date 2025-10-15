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

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.neo4j.bolt.test.util.ErrorUtil.useNewMessage;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.TestInstance;
import org.neo4j.bolt.protocol.common.connector.connection.AtomicSchedulingConnection;
import org.neo4j.bolt.test.annotation.BoltTestExtension;
import org.neo4j.bolt.test.annotation.connection.initializer.Connected;
import org.neo4j.bolt.test.annotation.connection.initializer.VersionSelected;
import org.neo4j.bolt.test.annotation.connection.transport.ExcludeTransport;
import org.neo4j.bolt.test.annotation.connection.transport.IncludeTransport;
import org.neo4j.bolt.test.annotation.setup.FactoryFunction;
import org.neo4j.bolt.test.annotation.setup.SettingsFunction;
import org.neo4j.bolt.test.annotation.test.BoltTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.assertion.Assert;
import org.neo4j.test.extension.SkipOnSpd;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
@BoltTestExtension
@ExcludeWire(until = @Version(major = 5, minor = 0))
@IncludeTransport({TransportType.TCP, TransportType.UNIX, TransportType.LOCAL})
public class AuthenticationIT {

    protected final AssertableLogProvider userLogProvider = new AssertableLogProvider();

    @FactoryFunction
    protected void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setUserLogProvider(this.userLogProvider);
    }

    @SettingsFunction
    protected void customizeSettings(Map<Setting<?>, Object> settings) {
        settings.put(GraphDatabaseSettings.auth_enabled, true);
        settings.put(BoltConnector.enable_unix_socket_auth, true);
        settings.put(BoltConnectorInternalSettings.enable_unix_socket_user_database_access, true);
        settings.put(BoltConnector.advertised_address, new SocketAddress("my-server.neo4j.io", 7688));
    }

    @AfterEach
    void cleanup() {
        this.userLogProvider.clear();
    }

    private static MapValue singletonMap(String key, Object value) {
        return VirtualValues.map(new String[] {key}, new AnyValue[] {ValueUtils.of(value)});
    }

    @BoltTest
    void shouldRespondWithCredentialsExpiredOnFirstUse(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailIfWrongCredentialsV40(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "wrong")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                .isEventuallyTerminated();

        Assert.assertEventually(
                () -> "Matching log call not found in\n" + this.userLogProvider.serialize(),
                () -> {
                    try {
                        LogAssertions.assertThat(this.userLogProvider)
                                .forClass(AtomicSchedulingConnection.class)
                                .forLevel(WARN)
                                .containsMessages(useNewMessage("Access denied, see the security logs for details.")
                                        .whenLegacyFallbackTo(
                                                "The client is unauthorized due to authentication failure."));
                        return true;
                    } catch (AssertionError e) {
                        return false;
                    }
                },
                TRUE,
                30,
                SECONDS);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailIfWrongCredentialsV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "wrong")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Security.Unauthorized,
                        "The client is unauthorized due to authentication failure.",
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();

        Assert.assertEventually(
                () -> "Matching log call not found in\n" + this.userLogProvider.serialize(),
                () -> {
                    try {
                        LogAssertions.assertThat(this.userLogProvider)
                                .forClass(AtomicSchedulingConnection.class)
                                .forLevel(WARN)
                                .containsMessages(useNewMessage("Access denied, see the security logs for details.")
                                        .whenLegacyFallbackTo(
                                                "The client is unauthorized due to authentication failure."));
                        return true;
                    } catch (AssertionError e) {
                        return false;
                    }
                },
                TRUE,
                30,
                SECONDS);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailIfWrongCredentials(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "wrong")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Security.Unauthorized,
                        useNewMessage("42NFF: Access denied, see the security logs for details.")
                                .whenLegacyFallbackTo("The client is unauthorized due to authentication failure."),
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();

        Assert.assertEventually(
                () -> "Matching log call not found in\n" + this.userLogProvider.serialize(),
                () -> {
                    try {
                        LogAssertions.assertThat(this.userLogProvider)
                                .forClass(AtomicSchedulingConnection.class)
                                .forLevel(WARN)
                                .containsMessages(useNewMessage("Access denied, see the security logs for details.")
                                        .whenLegacyFallbackTo(
                                                "The client is unauthorized due to authentication failure."));
                        return true;
                    } catch (AssertionError e) {
                        return false;
                    }
                },
                TRUE,
                30,
                SECONDS);
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailIfWrongCredentialsFollowingSuccessfulLoginV40(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {

        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                        singletonMap("password", "secretPassword"))
                .withDatabase(SYSTEM_DATABASE_NAME)));
        connection.send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);

        // attempt to authenticate again with the new password
        connection.reconnect();
        wire.negotiate(connection);

        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "secretPassword")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // attempt to authenticate again with the old password
        connection.reconnect();
        wire.negotiate(connection);

        connection.send(wire.hello());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailIfWrongCredentialsFollowingSuccessfulLoginV5x7(
            BoltWire wire, @VersionSelected ConnectionProvider connectionProvider) {
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            // ensure that the server returns the expected set of metadata
            BoltConnectionAssertions.assertThat(connection)
                    .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

            // authenticate normally using the preset credentials and update the password to a new value
            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                            singletonMap("password", "secretPassword"))
                    .withDatabase(SYSTEM_DATABASE_NAME)));
            connection.send(wire.pull());

            BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);
        }

        // attempt to authenticate again with the new password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "secretPassword")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        }

        // attempt to authenticate again with the old password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j")));

            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailure(
                            Status.Security.Unauthorized,
                            "The client is unauthorized due to authentication failure.",
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                    .isEventuallyTerminated();
        }
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(
            BoltWire wire, @VersionSelected ConnectionProvider connectionProvider) {
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            // ensure that the server returns the expected set of metadata
            BoltConnectionAssertions.assertThat(connection)
                    .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

            // authenticate normally using the preset credentials and update the password to a new value
            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                            singletonMap("password", "secretPassword"))
                    .withDatabase(SYSTEM_DATABASE_NAME)));
            connection.send(wire.pull());

            BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);
        }

        // attempt to authenticate again with the new password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "secretPassword")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();
        }

        // attempt to authenticate again with the old password
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j")));

            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailure(
                            Status.Security.Unauthorized,
                            useNewMessage("42NFF: Access denied, see the security logs for details.")
                                    .whenLegacyFallbackTo("The client is unauthorized due to authentication failure."),
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                    .isEventuallyTerminated();
        }
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailIfMalformedAuthTokenWrongTypeV40(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", List.of("neo4j"),
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList")
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailIfMalformedAuthTokenWrongTypeV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", List.of("neo4j"),
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList",
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailIfMalformedAuthTokenWrongType(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", List.of("neo4j"),
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        useNewMessage("42NFF: Access denied, see the security logs for details.")
                                .whenLegacyFallbackTo(
                                        "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList"),
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailIfMalformedAuthTokenMissingKeyV40(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "this-should-have-been-credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `credentials`")
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailIfMalformedAuthTokenMissingKeyV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "this-should-have-been-credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, missing key `credentials`",
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailIfMalformedAuthTokenMissingKey(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "this-should-have-been-credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        useNewMessage("42NFF: Access denied, see the security logs for details.")
                                .whenLegacyFallbackTo("Unsupported authentication token, missing key `credentials`"),
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailIfMalformedAuthTokenMissingSchemeV40(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `scheme`")
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailIfMalformedAuthTokenMissingSchemeV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, missing key `scheme`",
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailIfMalformedAuthTokenMissingScheme(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        useNewMessage("42NFF: Access denied, see the security logs for details.")
                                .whenLegacyFallbackTo("Unsupported authentication token, missing key `scheme`"),
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    @SkipOnSpd(reason = "Message for unsupported authentication token is different in enterprise and spd")
    protected void shouldFailIfMalformedAuthTokenUnknownSchemeV40(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "unknown",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, scheme 'unknown' is not supported.")
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    @SkipOnSpd(reason = "Message for unsupported authentication token is different in enterprise and spd")
    protected void shouldFailIfMalformedAuthTokenUnknownSchemeV5x7(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws InterruptedException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "unknown",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, scheme 'unknown' is not supported.",
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    @SkipOnSpd(reason = "Message for unsupported authentication token is different in enterprise and spd")
    protected void shouldFailIfMalformedAuthTokenUnknownScheme(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws InterruptedException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "unknown",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Security.Unauthorized,
                        useNewMessage("42NFF: Access denied, see the security logs for details.")
                                .whenLegacyFallbackTo(
                                        "Unsupported authentication token, scheme 'unknown' is not supported."),
                        GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .isEventuallyTerminated();
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailDifferentlyIfTooManyFailedAuthAttemptsV40(
            BoltWire wire, @Connected ConnectionProvider connectionProvider) {
        awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                connection.reconnect();
                wire.negotiate(connection);

                connection.send(wire.hello());
                // ensure that the server returns the expected set of metadata
                BoltConnectionAssertions.assertThat(connection)
                        .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

                connection.send(wire.logon(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));

                BoltConnectionAssertions.assertThat(connection)
                        .receivesFailureV40(
                                Status.Security.AuthenticationRateLimit,
                                "The client has provided incorrect authentication details too many times in a row.")
                        .isEventuallyTerminated();
            }
        });
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailDifferentlyIfTooManyFailedAuthAttemptsV5x7(
            BoltWire wire, @Connected ConnectionProvider connectionProvider) {
        awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                wire.negotiate(connection);

                connection.send(wire.hello());
                // ensure that the server returns the expected set of metadata
                BoltConnectionAssertions.assertThat(connection)
                        .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

                connection.send(wire.logon(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));

                BoltConnectionAssertions.assertThat(connection)
                        .receivesFailure(
                                Status.Security.AuthenticationRateLimit,
                                "The client has provided incorrect authentication details too many times in a row.",
                                GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                        .isEventuallyTerminated();
            }
        });
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailDifferentlyIfTooManyFailedAuthAttempts(
            BoltWire wire, @Connected ConnectionProvider connectionProvider) {
        awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                wire.negotiate(connection);

                connection.send(wire.hello());
                // ensure that the server returns the expected set of metadata
                BoltConnectionAssertions.assertThat(connection)
                        .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

                connection.send(wire.logon(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));

                BoltConnectionAssertions.assertThat(connection)
                        .receivesFailure(
                                Status.Security.AuthenticationRateLimit,
                                useNewMessage("42NFF: Access denied, see the security logs for details.")
                                        .whenLegacyFallbackTo(
                                                "The client has provided incorrect authentication details too many times in a row."),
                                GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                        .isEventuallyTerminated();
            }
        });
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailWhenReusingTheSamePasswordV40(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        connection
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Statement.ArgumentError, "Old password and new password cannot be the same.")
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailWhenReusingTheSamePasswordV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        connection
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyWithCause(
                        Status.Statement.ArgumentError,
                        "Old password and new password cannot be the same.",
                        GqlStatusInfoCodes.STATUS_22N05.getGqlStatus(),
                        "error: data exception - input failed validation. Invalid input '***' for neo4j password.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCause(
                                "2N89: Expected the new password to be different from the old password.",
                                GqlStatusInfoCodes.STATUS_22N89.getGqlStatus(),
                                "error: data exception - new password cannot be the same as the old password. Expected the new password to be different from the old password.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR")))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailWhenReusingTheSamePassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        connection
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "password"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyWithCause(
                        Status.Statement.ArgumentError,
                        useNewMessage("22N05: Invalid input '***' for neo4j password.")
                                .whenLegacyFallbackTo(
                                        "User 'neo4j' failed to alter their own password: Old password and new password cannot be the same."),
                        GqlStatusInfoCodes.STATUS_22N05.getGqlStatus(),
                        "error: data exception - input failed validation. Invalid input '***' for neo4j password.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCause(
                                "2N89: Expected the new password to be different from the old password.",
                                GqlStatusInfoCodes.STATUS_22N89.getGqlStatus(),
                                "error: data exception - new password cannot be the same as the old password. Expected the new password to be different from the old password.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR")))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldFailWhenSubmittingEmptyPasswordV40(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureV40(Status.Statement.ArgumentError, "A password cannot be empty.")
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldFailWhenSubmittingEmptyPasswordV5x7(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Statement.ArgumentError,
                        "A password cannot be empty.",
                        GqlStatusInfoCodes.STATUS_22NB6.getGqlStatus(),
                        "error: data exception - input empty. Invalid input. Password is not allowed to be an empty string.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldFailWhenSubmittingEmptyPassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(
                        Status.Statement.ArgumentError,
                        useNewMessage("22NB6: Invalid input. Password is not allowed to be an empty string.")
                                .whenLegacyFallbackTo("A password cannot be empty."),
                        GqlStatusInfoCodes.STATUS_22NB6.getGqlStatus(),
                        "error: data exception - input empty. Invalid input. Password is not allowed to be an empty string.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
                                singletonMap("password", "abcdefgh"))
                        .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldNotBeAbleToReadWhenPasswordChangeRequiredV40(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate with the default (expired) credentials
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        // attempt to execute a query
        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

        // which should fail with one of two possible errors
        try {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzyV40(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        } catch (AssertionError ignore) {
            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN
            // message will
            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzyV40(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        }
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldNotBeAbleToReadWhenPasswordChangeRequiredV5x7(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate with the default (expired) credentials
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        // attempt to execute a query
        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

        // which should fail with one of two possible errors
        try {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.",
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                            BoltConnectionAssertions.assertErrorCause(
                                    "42NFD: Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    GqlStatusInfoCodes.STATUS_42NFD.getGqlStatus(),
                                    "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR")));
        } catch (StackOverflowError ignore) {
            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN
            // message will
            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.",
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                            BoltConnectionAssertions.assertErrorCause(
                                    "42NFD: Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    GqlStatusInfoCodes.STATUS_42NFD.getGqlStatus(),
                                    "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR")));
        }
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldNotBeAbleToReadWhenPasswordChangeRequired(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate with the default (expired) credentials
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));

        // attempt to execute a query
        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

        // which should fail with one of two possible errors
        try {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            useNewMessage("42NFF: Access denied, see the security logs for details.")
                                    .whenLegacyFallbackTo(
                                            "The credentials you provided were valid, but must be changed before you can use this instance."),
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                            BoltConnectionAssertions.assertErrorCause(
                                    "42NFD: Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    GqlStatusInfoCodes.STATUS_42NFD.getGqlStatus(),
                                    "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR")));
        } catch (StackOverflowError ignore) {
            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN
            // message will
            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            useNewMessage("42NFF: Access denied, see the security logs for details.")
                                    .whenLegacyFallbackTo(
                                            "The credentials you provided were valid, but must be changed before you can use this instance."),
                            GqlStatusInfoCodes.STATUS_42NFF.getGqlStatus(),
                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.",
                            BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                            BoltConnectionAssertions.assertErrorCause(
                                    "42NFD: Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    GqlStatusInfoCodes.STATUS_42NFD.getGqlStatus(),
                                    "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.",
                                    BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord(
                                            "CLIENT_ERROR")));
        }
    }

    @BoltTest
    void shouldBeAbleToLogoffAfterBeingAuthenticatedThenLogBackOn(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        connection.send(wire.logoff());
        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Should be back in authentication state so should be able to log back on
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    }

    @BoltTest
    @IncludeWire(until = @Version(major = 5, minor = 6))
    void shouldNotBeAbleToAuthenticateOnHelloMessageV40(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Attempt to start a transaction this will fail because you are in authentication state and not authenticated
        connection.send(wire.begin());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureFuzzyV40(
                        Status.Request.Invalid, "cannot be handled by a session in the AUTHENTICATION state.");
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 7), until = @Version(major = 5, minor = 8))
    void shouldNotBeAbleToAuthenticateOnHelloMessageV5x7(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Attempt to start a transaction this will fail because you are in authentication state and not authenticated
        connection.send(wire.begin());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureWithCause(
                        Status.Request.Invalid,
                        "Message of type BeginMessage cannot be handled by a session in the AUTHENTICATION state.",
                        GqlStatusInfoCodes.STATUS_08N06.getGqlStatus(),
                        "error: connection exception - protocol error. General network protocol error.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCause(
                                "08N10: Message BeginMessage cannot be handled by session in the 'AUTHENTICATION' state.",
                                GqlStatusInfoCodes.STATUS_08N10.getGqlStatus(),
                                "error: connection exception - invalid server state. Message BeginMessage cannot be handled by session in the 'AUTHENTICATION' state.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR")));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 6, minor = 0))
    void shouldNotBeAbleToAuthenticateOnHelloMessage(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Attempt to start a transaction this will fail because you are in authentication state and not authenticated
        connection.send(wire.begin());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailureWithCause(
                        Status.Request.Invalid,
                        useNewMessage("08N06: General network protocol error.")
                                .whenLegacyFallbackTo(
                                        "Message of type BeginMessage cannot be handled by a session in the AUTHENTICATION state."),
                        GqlStatusInfoCodes.STATUS_08N06.getGqlStatus(),
                        "error: connection exception - protocol error. General network protocol error.",
                        BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR"),
                        BoltConnectionAssertions.assertErrorCause(
                                "08N10: Message BeginMessage cannot be handled by session in the 'AUTHENTICATION' state.",
                                GqlStatusInfoCodes.STATUS_08N10.getGqlStatus(),
                                "error: connection exception - invalid server state. Message BeginMessage cannot be handled by session in the 'AUTHENTICATION' state.",
                                BoltConnectionAssertions.assertErrorClassificationOnDiagnosticRecord("CLIENT_ERROR")));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 1), until = @Version(major = 5, minor = 7))
    void shouldNotReturnAdvertiseAddressOnLogonMessageSuccess(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).doesNotContainKeys("advertised_address"));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 8))
    @IncludeTransport({TransportType.LOCAL, TransportType.UNIX})
    void shouldNotReturnAdvertiseAddressOnLogonMessageSuccessWhenRoutingIsUnavailable(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).doesNotContainKeys("advertised_address"));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 8))
    // address advertisement is not available on LOCAL and UNIX connectors
    @ExcludeTransport({TransportType.LOCAL, TransportType.UNIX})
    void shouldReturnAdvertiseAddressOnLogonMessageSuccess(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection).receivesSuccess(meta -> Assertions.assertThat(meta)
                .containsEntry("advertised_address", "my-server.neo4j.io:7688"));
    }
}
