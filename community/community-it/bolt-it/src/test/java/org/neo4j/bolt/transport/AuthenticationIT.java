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
package org.neo4j.bolt.transport;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.bolt.testing.assertions.BoltConnectionAssertions.assertThat;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.hello;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.pull;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.reset;
import static org.neo4j.bolt.testing.messages.BoltDefaultWire.run;
import static org.neo4j.configuration.GraphDatabaseSettings.SYSTEM_DATABASE_NAME;
import static org.neo4j.logging.AssertableLogProvider.Level.WARN;
import static org.neo4j.logging.LogAssertions.assertThat;
import static org.neo4j.test.assertion.Assert.assertEventually;
import static org.neo4j.test.assertion.Assert.awaitUntilAsserted;
import static org.neo4j.test.conditions.Conditions.TRUE;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.TestInfo;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.bolt.AbstractBoltTransportsTest;
import org.neo4j.bolt.protocol.common.connection.DefaultBoltConnection;
import org.neo4j.bolt.testing.client.TransportConnection;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.graphdb.config.Setting;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.util.ValueUtils;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.testdirectory.EphemeralTestDirectoryExtension;
import org.neo4j.values.AnyValue;
import org.neo4j.values.virtual.MapValue;
import org.neo4j.values.virtual.VirtualValues;

@EphemeralTestDirectoryExtension
@Neo4jWithSocketExtension
public class AuthenticationIT extends AbstractBoltTransportsTest {
    protected final AssertableLogProvider logProvider = new AssertableLogProvider();

    @Inject
    private Neo4jWithSocket server;

    protected TestDatabaseManagementServiceBuilder getTestGraphDatabaseFactory() {
        return new TestDatabaseManagementServiceBuilder().setUserLogProvider(logProvider);
    }

    @BeforeEach
    void setup(TestInfo testInfo) throws IOException {
        server.setGraphDatabaseFactory(getTestGraphDatabaseFactory());
        server.setConfigure(getSettingsFunction());
        server.init(testInfo);
        address = server.lookupDefaultConnector();
    }

    @Override
    protected Consumer<Map<Setting<?>, Object>> getSettingsFunction() {
        return settings -> {
            super.getSettingsFunction().accept(settings);
            settings.put(GraphDatabaseSettings.auth_enabled, true);
        };
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldRespondWithCredentialsExpiredOnFirstUse(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess(meta -> assertThat(meta)
                .containsKeys("server", "connection_id")
                .containsEntry("credentials_expired", true));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailIfWrongCredentials(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "wrong")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailure(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                .isEventuallyTerminated();

        assertEventually(
                () -> "Matching log call not found in\n" + logProvider.serialize(),
                this::authFailureLoggedToUserLog,
                TRUE,
                30,
                SECONDS);
    }

    private boolean authFailureLoggedToUserLog() {
        try {
            assertThat(logProvider)
                    .forClass(DefaultBoltConnection.class)
                    .forLevel(WARN)
                    .containsMessages("The client is unauthorized due to authentication failure.");
            return true;
        } catch (AssertionError e) {
            return false;
        }
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion();
        assertThat(connection).receivesSuccess();

        // change password
        connection.send(run(
                "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                singletonMap("password", "secret"),
                singletonMap("db", SYSTEM_DATABASE_NAME)));
        connection.send(pull());

        assertThat(connection).receivesSuccess();
        // Then
        assertThat(connection).receivesSuccess();

        // When login again with the new password
        connection
                .reconnect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "secret")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess();

        // When login again with the wrong password
        connection
                .reconnect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailure(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailIfMalformedAuthTokenWrongType(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", List.of("neo4j"),
                        "credentials", "neo4j")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, the value associated with the key `principal` must be a String but was: ArrayList")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailIfMalformedAuthTokenMissingKey(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "this-should-have-been-credentials", "neo4j")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `credentials`")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailIfMalformedAuthTokenMissingScheme(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailureFuzzy(
                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `scheme`")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    public void shouldFailIfMalformedAuthTokenUnknownScheme(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "unknown",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailure(
                        Status.Security.Unauthorized,
                        "Unsupported authentication token, scheme 'unknown' is not supported.")
                .isEventuallyTerminated();
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailDifferentlyIfTooManyFailedAuthAttempts(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);
        AtomicInteger counter = new AtomicInteger();

        awaitUntilAsserted(() -> {
            System.out.println(counter.getAndIncrement());
            connection
                    .reconnect() // ensure that this works beyond connection boundaries
                    .sendDefaultProtocolVersion()
                    .send(hello(Map.of(
                            "scheme", "basic",
                            "principal", "neo4j",
                            "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));

            assertThat(connection)
                    .negotiatesDefaultVersion()
                    .receivesFailure(
                            Status.Security.AuthenticationRateLimit,
                            "The client has provided incorrect authentication details too many times in a row.")
                    .isEventuallyTerminated();
        });
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldBeAbleToChangePasswordUsingSystemCommand(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess(meta -> assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        // When
        connection
                .send(run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        singletonMap("password", "secret"),
                        singletonMap("db", SYSTEM_DATABASE_NAME)))
                .send(pull());

        // Then
        assertThat(connection).receivesSuccess(2);

        // If I reconnect I cannot use the old password
        connection
                .reconnect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        assertThat(connection)
                .negotiatesDefaultVersion()
                .receivesFailure(
                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.");

        // But the new password works fine
        connection
                .reconnect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "secret")));
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailWhenReusingTheSamePassword(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess(meta -> assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        // When
        connection
                .send(run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        singletonMap("password", "neo4j"),
                        singletonMap("db", SYSTEM_DATABASE_NAME)))
                .send(pull());

        // Then
        assertThat(connection)
                .receivesFailureFuzzy(
                        Status.Statement.ArgumentError, "Old password and new password cannot be the same.")
                .receivesIgnored();

        // However, you should also be able to recover
        connection
                .send(reset())
                .send(run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        singletonMap("password", "abc"),
                        singletonMap("db", SYSTEM_DATABASE_NAME)))
                .send(pull());

        assertThat(connection).receivesSuccess(3);
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldFailWhenSubmittingEmptyPassword(TransportConnection.Factory connectionFactory) throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess(meta -> assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        // When
        connection
                .send(run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        singletonMap("password", ""),
                        singletonMap("db", SYSTEM_DATABASE_NAME)))
                .send(pull());

        // Then
        assertThat(connection)
                .receivesFailure(Status.Statement.ArgumentError, "A password cannot be empty.")
                .receivesIgnored();

        // However, you should also be able to recover
        connection
                .send(reset())
                .send(run(
                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
                        singletonMap("password", "abc"),
                        singletonMap("db", SYSTEM_DATABASE_NAME)))
                .send(pull());

        assertThat(connection).receivesSuccess(3);
    }

    @ParameterizedTest(name = "{displayName} {arguments}")
    @MethodSource("argumentsProvider")
    void shouldNotBeAbleToReadWhenPasswordChangeRequired(TransportConnection.Factory connectionFactory)
            throws Exception {
        initParameters(connectionFactory);

        // When
        connection
                .connect()
                .sendDefaultProtocolVersion()
                .send(hello(Map.of(
                        "scheme", "basic",
                        "principal", "neo4j",
                        "credentials", "neo4j")));

        // Then
        assertThat(connection).negotiatesDefaultVersion().receivesSuccess(meta -> assertThat(meta)
                .containsEntry("credentials_expired", true)
                .containsKeys("server", "connection_id"));

        // When
        connection.send(run("MATCH (n) RETURN n")).send(pull());

        // Then
        try {
            assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        } catch (AssertionError ignore) {
            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means the RUN
            // message will
            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
            assertThat(connection)
                    .receivesFailureFuzzy(
                            Status.Security.CredentialsExpired,
                            "The credentials you provided were valid, but must be changed before you can use this instance.");
        }
    }

    private static MapValue singletonMap(String key, Object value) {
        return VirtualValues.map(new String[] {key}, new AnyValue[] {ValueUtils.of(value)});
    }
}
