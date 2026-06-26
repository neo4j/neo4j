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
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.test.util.ErrorUtil.useNewMessage;
import static org.neo4j.collection.Dependencies.dependenciesOf;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
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
import org.neo4j.bolt.test.annotation.setup.preset.EnableAuthentication;
import org.neo4j.bolt.test.annotation.test.BoltTest;
import org.neo4j.bolt.test.annotation.wire.selector.ExcludeWire;
import org.neo4j.bolt.test.annotation.wire.selector.IncludeWire;
import org.neo4j.bolt.test.connection.setup.SettingBuilder;
import org.neo4j.bolt.test.provider.ConnectionProvider;
import org.neo4j.bolt.testing.annotation.Version;
import org.neo4j.bolt.testing.assertions.BoltConnectionAssertions;
import org.neo4j.bolt.testing.assertions.DiagnosticRecordAssertions;
import org.neo4j.bolt.testing.assertions.FailureCauseAssertions;
import org.neo4j.bolt.testing.assertions.FailureMetadataAssertions;
import org.neo4j.bolt.testing.assertions.GqlMessageParameters;
import org.neo4j.bolt.testing.client.BoltTestConnection;
import org.neo4j.bolt.testing.client.TransportType;
import org.neo4j.bolt.testing.messages.BoltWire;
import org.neo4j.bolt.transport.Neo4jWithSocketExtension;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.gqlstatus.ErrorClassification;
import org.neo4j.gqlstatus.GqlStatusInfoCodes;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.internal.LogService;
import org.neo4j.logging.internal.SimpleLogService;
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
@EnableAuthentication
@ExcludeWire(until = @Version(major = 5, minor = 0))
@IncludeTransport({TransportType.TCP, TransportType.UNIX, TransportType.LOCAL})
class AuthenticationIT {

    protected final AssertableLogProvider securityLogProvider = new AssertableLogProvider();
    protected final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    protected final LogService logService = new SimpleLogService(userLogProvider, securityLogProvider);

    @FactoryFunction
    protected void customizeDatabase(TestDatabaseManagementServiceBuilder factory) {
        factory.setUserLogProvider(this.userLogProvider);

        factory.setExternalDependencies(dependenciesOf(logService));
    }

    @SettingsFunction
    protected void customizeSettings(SettingBuilder settings) {
        settings.set(BoltConnector.advertised_address, new SocketAddress("my-server.neo4j.io", 7688));
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
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsEntry("credentials_expired", true));
    }

    @BoltTest
    void shouldFailIfWrongCredentials(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "wrong")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Security.Unauthorized)
                        .hasLegacyMessage("The client is unauthorized due to authentication failure.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                        .hasDescription(
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .isEventuallyTerminated();

        LogAssertions.assertThat(this.securityLogProvider)
                .containsMessages("The client is unauthorized due to authentication failure.");

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
    void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(
            BoltWire wire, @VersionSelected ConnectionProvider connectionProvider) {
        try (var connection = connectionProvider.create()) {
            connection.send(wire.hello());
            // ensure that the server returns the expected set of metadata
            BoltConnectionAssertions.assertThat(connection)
                    .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

            // authenticate normally using the preset credentials and update the password to a new value
            connection.send(wire.logon(Map.of(
                    "scheme", "basic",
                    "principal", "neo4j",
                    "credentials", "neo4j")));

            BoltConnectionAssertions.assertThat(connection).receivesSuccess();

            connection.send(wire.run(
                    "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                    x -> x.withParameters(singletonMap("password", "secretPassword"))
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
                    .receivesFailure(FailureMetadataAssertions.create()
                            .hasLegacyStatus(Status.Security.Unauthorized)
                            .hasLegacyMessage("The client is unauthorized due to authentication failure.")
                            .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                            .hasDescription(
                                    "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                            .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                    .hasClassification(ErrorClassification.CLIENT_ERROR)))
                    .isEventuallyTerminated();

            LogAssertions.assertThat(this.securityLogProvider)
                    .containsMessages("The client is unauthorized due to authentication failure.");
        }
    }

    @BoltTest
    void shouldFailIfMalformedAuthTokenWrongType(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", List.of("neo4j"),
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Security.Unauthorized)
                        .hasLegacyMessage(
                                "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList")
                        .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                        .hasDescription(
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .isEventuallyTerminated();

        LogAssertions.assertThat(this.securityLogProvider)
                .containsMessages("The client is unauthorized due to authentication failure.");
    }

    @BoltTest
    void shouldFailIfMalformedAuthTokenMissingKey(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "this-should-have-been-credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Security.Unauthorized)
                        .hasLegacyMessage("Unsupported authentication token, missing key `credentials`")
                        .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                        .hasDescription(
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .isEventuallyTerminated();
    }

    @BoltTest
    void shouldFailIfMalformedAuthTokenMissingScheme(BoltWire wire, @VersionSelected BoltTestConnection connection)
            throws IOException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Security.Unauthorized)
                        .hasLegacyMessage("Unsupported authentication token, missing key `scheme`")
                        .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                        .hasDescription(
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .isEventuallyTerminated();

        LogAssertions.assertThat(this.securityLogProvider)
                .containsMessages("The client is unauthorized due to authentication failure.");
    }

    @BoltTest
    @SkipOnSpd(reason = "Message for unsupported authentication token is different in enterprise and spd")
    protected void shouldFailIfMalformedAuthTokenUnknownScheme(
            BoltWire wire, @VersionSelected BoltTestConnection connection) throws InterruptedException {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "unknown",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Security.Unauthorized)
                        .hasLegacyMessage("Unsupported authentication token, scheme 'unknown' is not supported.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                        .hasDescription(
                                "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .isEventuallyTerminated();
    }

    @BoltTest
    void shouldFailDifferentlyIfTooManyFailedAuthAttempts(
            BoltWire wire, @Connected ConnectionProvider connectionProvider) {
        awaitUntilAsserted(() -> {
            try (var connection = connectionProvider.create()) {
                wire.negotiate(connection);

                connection.send(wire.hello());
                // ensure that the server returns the expected set of metadata
                BoltConnectionAssertions.assertThat(connection)
                        .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

                connection.send(wire.logon(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));

                BoltConnectionAssertions.assertThat(connection)
                        .receivesFailure(FailureMetadataAssertions.create()
                                .hasLegacyStatus(Status.Security.AuthenticationRateLimit)
                                .hasLegacyMessage(
                                        "The client has provided incorrect authentication details too many times in a row.")
                                .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                                .hasDescription(
                                        "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR)))
                        .isEventuallyTerminated();

                LogAssertions.assertThat(this.securityLogProvider)
                        .containsMessages("The client is unauthorized due to authentication failure.");
            }
        });
    }

    @BoltTest
    void shouldFailWhenReusingTheSamePassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.reset())
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "password"))
                                .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password",
                        x -> x.withParameters(singletonMap("password", "password"))
                                .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Statement.ArgumentError)
                        .hasLegacyMessage(
                                "User 'neo4j' failed to alter their own password: Old password and new password cannot be the same.")
                        .hasStatus(
                                GqlStatusInfoCodes.STATUS_22N05,
                                GqlMessageParameters.create().withString("***").withString("neo4j password"))
                        .hasDescription(
                                "error: data exception - input failed validation. Invalid input '***' for neo4j password.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(GqlStatusInfoCodes.STATUS_22N89)
                                .hasDescription(
                                        "error: data exception - new password cannot be the same as the old password. Expected the new password to be different from the old password.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password",
                        x -> x.withParameters(singletonMap("password", "abcdefgh"))
                                .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    void shouldFailWhenSubmittingEmptyPassword(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsEntry("credentials_expired", true));

        connection
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Statement.ArgumentError)
                        .hasLegacyMessage("A password cannot be empty.")
                        .hasStatus(
                                GqlStatusInfoCodes.STATUS_22NB6,
                                GqlMessageParameters.create().withString("Password"))
                        .hasDescription(
                                "error: data exception - input empty. Invalid input. Password is not allowed to be an empty string.")
                        .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                .hasClassification(ErrorClassification.CLIENT_ERROR)))
                .receivesIgnored();

        connection
                .send(wire.reset())
                .send(wire.run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        x -> x.withParameters(singletonMap("password", "abcdefgh"))
                                .withDatabase(SYSTEM_DATABASE_NAME)))
                .send(wire.pull());

        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    }

    @BoltTest
    void shouldNotBeAbleToReadWhenPasswordChangeRequired(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        // authenticate with the default (expired) credentials
        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsEntry("credentials_expired", true));

        // attempt to execute a query
        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());

        try {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailure(FailureMetadataAssertions.create()
                            .hasLegacyStatus(Status.Security.CredentialsExpired)
                            .hasLegacyMessageFuzzy(
                                    "The credentials you provided were valid, but must be changed before you can use this instance.")
                            .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                            .hasDescription(
                                    "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                            .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                    .hasClassification(ErrorClassification.CLIENT_ERROR))
                            .hasCause(FailureCauseAssertions.create()
                                    .hasStatus(GqlStatusInfoCodes.STATUS_42NFD)
                                    .hasDescription(
                                            "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.")
                                    .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                            .hasClassification(ErrorClassification.CLIENT_ERROR))));
        } catch (StackOverflowError e) {
            BoltConnectionAssertions.assertThat(connection)
                    .receivesFailure(
                            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message,
                            // which
                            // means the RUN
                            // message will
                            // give a Success response. This should not matter much since RUN + PULL_N are always sent
                            // together.
                            FailureMetadataAssertions.create()
                                    .hasLegacyStatus(Status.Security.CredentialsExpired)
                                    .hasLegacyMessageFuzzy(
                                            "The credentials you provided were valid, but must be changed before you can use this instance.")
                                    .hasStatus(GqlStatusInfoCodes.STATUS_42NFF)
                                    .hasDescription(
                                            "error: syntax error or access rule violation - permission/access denied. Access denied, see the security logs for details.")
                                    .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                            .hasClassification(ErrorClassification.CLIENT_ERROR))
                                    .hasCause(FailureCauseAssertions.create()
                                            .hasStatus(GqlStatusInfoCodes.STATUS_42NFD)
                                            .hasDescription(
                                                    "error: syntax error or access rule violation - credentials expired. Permission denied. The credentials you provided were valid, but must be changed before you can use this instance.")
                                            .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                                    .hasClassification(ErrorClassification.CLIENT_ERROR))));
        }

        LogAssertions.assertThat(this.securityLogProvider)
                .containsMessages(
                        "The credentials you provided were valid, but must be changed before you can use this instance.");
    }

    @BoltTest
    void shouldBeAbleToLogoffAfterBeingAuthenticatedThenLogBackOn(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

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
    void shouldNotBeAbleToAuthenticateOnHelloMessage(BoltWire wire, @VersionSelected BoltTestConnection connection) {
        // authenticate normally using the preset credentials and update the password to a new value
        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));

        BoltConnectionAssertions.assertThat(connection).receivesSuccess();

        // Attempt to start a transaction this will fail because you are in authentication state and not authenticated
        connection.send(wire.begin());

        BoltConnectionAssertions.assertThat(connection)
                .receivesFailure(FailureMetadataAssertions.create()
                        .hasLegacyStatus(Status.Request.Invalid)
                        .hasLegacyMessage(
                                "Message of type BeginMessage cannot be handled by a session in the AUTHENTICATION state.")
                        .hasStatus(GqlStatusInfoCodes.STATUS_08N06)
                        .hasDescription("error: connection exception - protocol error. General network protocol error.")
                        .hasDiagnosticRecord(
                                DiagnosticRecordAssertions.create().hasClassification(ErrorClassification.CLIENT_ERROR))
                        .hasCause(FailureCauseAssertions.create()
                                .hasStatus(
                                        GqlStatusInfoCodes.STATUS_08N10,
                                        GqlMessageParameters.create()
                                                .withString("BeginMessage")
                                                .withString("AUTHENTICATION"))
                                .hasDescription(
                                        "error: connection exception - invalid server state. Message BeginMessage cannot be handled by session in the 'AUTHENTICATION' state.")
                                .hasDiagnosticRecord(DiagnosticRecordAssertions.create()
                                        .hasClassification(ErrorClassification.CLIENT_ERROR))));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 1), until = @Version(major = 5, minor = 7))
    void shouldNotReturnAdvertiseAddressOnLogonMessageSuccess(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());
        // ensure that the server returns the expected set of metadata
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).doesNotContainKeys("advertised_address"));
    }

    @BoltTest
    @IncludeWire(since = @Version(major = 5, minor = 8))
    @IncludeTransport({TransportType.LOCAL, TransportType.UNIX})
    void shouldNotReturnAdvertiseAddressOnLogonMessageSuccessWhenRoutingIsUnavailable(
            BoltWire wire, @VersionSelected BoltTestConnection connection) {
        connection.send(wire.hello());

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(meta -> assertThat(meta).doesNotContainKeys("advertised_address"));
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
                .receivesSuccess(meta -> assertThat(meta).containsKeys("server", "connection_id"));

        connection.send(wire.logon(Map.of(
                "scheme", "basic",
                "principal", "neo4j",
                "credentials", "neo4j")));

        // ensure that the server returns the expected set of metadata as well as a marker indicating that the used
        // credentials have expired and will need to be changed
        BoltConnectionAssertions.assertThat(connection)
                .receivesSuccess(
                        meta -> assertThat(meta).containsEntry("advertised_address", "my-server.neo4j.io:7688"));
    }
}
