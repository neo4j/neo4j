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
package org.neo4j.server.httpv2;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.httpv2.HttpV2ClientUtil.baseRequestBuilder;
import static org.neo4j.server.httpv2.HttpV2ClientUtil.encodedCredentials;
import static org.neo4j.server.httpv2.HttpV2ClientUtil.resolveDependency;
import static org.neo4j.server.httpv2.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.httpv2.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceAuthenticationIT {

    private static DatabaseManagementService database;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    public void beforeEach() {
        var builder = new TestDatabaseManagementServiceBuilder();
        database = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(GraphDatabaseSettings.auth_enabled, true)
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = resolveDependency(database, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterEach
    public void cleanUp() {
        database.shutdown();
    }

    @Test
    void shouldRequireCredentialChange() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "system")
                .header("Authorization", encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"SHOW USERS\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);

        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"error\":\"Neo.ClientError.Security.CredentialsExpired\","
                        + "\"message\":\"Permission denied.\\n\\nThe credentials you provided were valid, but must be "
                        + "changed before you can use this instance. If this is the first time you are using Neo4j, this "
                        + "is to ensure you are not using the default credentials in production. If you are not using "
                        + "default credentials, you are getting this message because an administrator requires a "
                        + "password change.\\nTo change your password, issue an `ALTER CURRENT USER SET PASSWORD "
                        + "FROM 'current password' TO 'new password'` statement against the system database.\"}]}");
    }

    @Test
    void shouldAllowAccessWhenPasswordChanged() throws IOException, InterruptedException {
        updateInitialPassword();

        var accessRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .header("Authorization", encodedCredentials("neo4j", "secretPassword"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var accessResponse = client.send(accessRequest, HttpResponse.BodyHandlers.ofString());
        var parsedJson = MAPPER.readTree(accessResponse.body());

        assertThat(accessResponse.statusCode()).isEqualTo(202);

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).asInt()).isEqualTo(1);
        assertThat(parsedJson.get(ERRORS_KEY)).isNull();
    }

    @Test
    void shouldReturnUnauthorizedWithWrongCredentials() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .header("Authorization", encodedCredentials("neo4j", "I'm sneaky!"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);

        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Security.Unauthorized\",\"message\":\"Invalid username or password.\"}]}");
    }

    @Test
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);

        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Security.Unauthorized\",\"message\":\"No authentication header supplied.\"}]}");
    }

    @Test
    void shouldReturnUnauthorizedWithInvalidAuthHeader() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .header("Authorization", "Just let me in. Thanks!")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);

        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Request.InvalidFormat\",\"message\":\"Invalid authentication header.\"}]}");
    }

    @Test
    @Timeout(30)
    void shouldErrorWhenTooManyIncorrectPasswordAttempts() throws IOException, InterruptedException {
        updateInitialPassword();

        HttpResponse<?> response;

        do {
            var req = baseRequestBuilder(queryEndpoint, "neo4j")
                    .header("Authorization", encodedCredentials("neo4j", "WrongPasswordBud"))
                    .POST(HttpRequest.BodyPublishers.ofString("shouldn't be parsing this"))
                    .build();
            response = client.send(req, HttpResponse.BodyHandlers.ofString());
        } while (response.statusCode() != 429);
        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Security.AuthenticationRateLimit\",\"message\":\"Too many failed authentication requests. Please wait 5 seconds and try again.\"}]}");
    }

    private static void updateInitialPassword() throws IOException, InterruptedException {
        var updatePasswordReq = baseRequestBuilder(queryEndpoint, "system")
                .header("Authorization", encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'\"}"))
                .build();

        var updatePasswordResp = client.send(updatePasswordReq, HttpResponse.BodyHandlers.ofString());

        assertThat(updatePasswordResp.statusCode()).isEqualTo(202);
    }

    //
    //    @ProtocolTest
    //    void shouldFailIfWrongCredentialsFollowingSuccessfulLogin(
    //            BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
    //
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        // authenticate normally using the preset credentials and update the password to a new value
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        connection.send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x ->
    // x.withParameters(
    //                        singletonMap("password", "secretPassword"))
    //                .withDatabase(SYSTEM_DATABASE_NAME)));
    //        connection.send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess(2);
    //
    //        // attempt to authenticate again with the new password
    //        connection.reconnect();
    //        wire.negotiate(connection);
    //
    //        connection.send(wire.hello());
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "secretPassword")));
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        // attempt to authenticate again with the old password
    //        connection.reconnect();
    //        wire.negotiate(connection);
    //
    //        connection.send(wire.hello());
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailure(
    //                        Status.Security.Unauthorized, "The client is unauthorized due to authentication failure.")
    //                .isEventuallyTerminated();
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailIfMalformedAuthTokenWrongType(BoltWire wire, @VersionSelected TransportConnection connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", List.of("neo4j"),
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailureFuzzy(
    //                        Status.Security.Unauthorized,
    //                        "Unsupported authentication token, the value associated with the key `principal` must be a
    // String but was: ArrayList")
    //                .isEventuallyTerminated();
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailIfMalformedAuthTokenMissingKey(BoltWire wire, @VersionSelected TransportConnection connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "this-should-have-been-credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailureFuzzy(
    //                        Status.Security.Unauthorized, "Unsupported authentication token, missing key
    // `credentials`")
    //                .isEventuallyTerminated();
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailIfMalformedAuthTokenMissingScheme(BoltWire wire, @VersionSelected TransportConnection
    // connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailureFuzzy(
    //                        Status.Security.Unauthorized, "Unsupported authentication token, missing key `scheme`")
    //                .isEventuallyTerminated();
    //    }
    //
    //    @ProtocolTest
    //    protected void shouldFailIfMalformedAuthTokenUnknownScheme(
    //            BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "unknown",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailure(
    //                        Status.Security.Unauthorized,
    //                        "Unsupported authentication token, scheme 'unknown' is not supported.")
    //                .isEventuallyTerminated();
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailDifferentlyIfTooManyFailedAuthAttempts(BoltWire wire, TransportConnection connection) {
    //        awaitUntilAsserted(() -> {
    //            connection.reconnect();
    //            wire.negotiate(connection);
    //
    //            connection.send(wire.hello());
    //            // ensure that the server returns the expected set of metadata
    //            BoltConnectionAssertions.assertThat(connection)
    //                    .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //            connection.send(wire.logon(Map.of(
    //                    "scheme", "basic",
    //                    "principal", "neo4j",
    //                    "credentials", "WHAT_WAS_THE_PASSWORD_AGAIN")));
    //
    //            BoltConnectionAssertions.assertThat(connection)
    //                    .receivesFailure(
    //                            Status.Security.AuthenticationRateLimit,
    //                            "The client has provided incorrect authentication details too many times in a row.")
    //                    .isEventuallyTerminated();
    //        });
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailWhenReusingTheSamePassword(BoltWire wire, @VersionSelected TransportConnection connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));
    //
    //        connection
    //                .send(wire.reset())
    //                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
    //                                singletonMap("password", "password"))
    //                        .withDatabase(SYSTEM_DATABASE_NAME)))
    //                .send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    //
    //        connection
    //                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x ->
    // x.withParameters(
    //                                singletonMap("password", "password"))
    //                        .withDatabase(SYSTEM_DATABASE_NAME)))
    //                .send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailureFuzzy(
    //                        Status.Statement.ArgumentError, "Old password and new password cannot be the same.")
    //                .receivesIgnored();
    //
    //        connection
    //                .send(wire.reset())
    //                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'password' TO $password", x ->
    // x.withParameters(
    //                                singletonMap("password", "abcdefgh"))
    //                        .withDatabase(SYSTEM_DATABASE_NAME)))
    //                .send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    //    }
    //
    //    @ProtocolTest
    //    void shouldFailWhenSubmittingEmptyPassword(BoltWire wire, @VersionSelected TransportConnection connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));
    //
    //        connection
    //                .send(wire.run(
    //                        "ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password",
    //                        x -> x.withParameters(singletonMap("password", "")).withDatabase(SYSTEM_DATABASE_NAME)))
    //                .send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailure(Status.Statement.ArgumentError, "A password cannot be empty.")
    //                .receivesIgnored();
    //
    //        connection
    //                .send(wire.reset())
    //                .send(wire.run("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO $password", x -> x.withParameters(
    //                                singletonMap("password", "abcdefgh"))
    //                        .withDatabase(SYSTEM_DATABASE_NAME)))
    //                .send(wire.pull());
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess(3);
    //    }
    //
    //    @ProtocolTest
    //    void shouldNotBeAbleToReadWhenPasswordChangeRequired(BoltWire wire, @VersionSelected TransportConnection
    // connection)
    //            throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        // authenticate with the default (expired) credentials
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsEntry("credentials_expired", true));
    //
    //        // attempt to execute a query
    //        connection.send(wire.run("MATCH (n) RETURN n")).send(wire.pull());
    //
    //        // which should fail with one of two possible errors
    //        try {
    //            BoltConnectionAssertions.assertThat(connection)
    //                    .receivesFailureFuzzy(
    //                            Status.Security.CredentialsExpired,
    //                            "The credentials you provided were valid, but must be changed before you can use this
    // instance.");
    //        } catch (AssertionError ignore) {
    //            // Compiled runtime triggers the AuthorizationViolation exception on the PULL_N message, which means
    // the RUN
    //            // message will
    //            // give a Success response. This should not matter much since RUN + PULL_N are always sent together.
    //            BoltConnectionAssertions.assertThat(connection)
    //                    .receivesFailureFuzzy(
    //                            Status.Security.CredentialsExpired,
    //                            "The credentials you provided were valid, but must be changed before you can use this
    // instance.");
    //        }
    //    }
    //
    //    @ProtocolTest
    //    void shouldBeAbleToLogoffAfterBeingAuthenticatedThenLogBackOn(
    //            BoltWire wire, @VersionSelected TransportConnection connection) throws IOException {
    //        connection.send(wire.hello());
    //        // ensure that the server returns the expected set of metadata
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesSuccess(meta -> Assertions.assertThat(meta).containsKeys("server", "connection_id"));
    //
    //        // authenticate normally using the preset credentials and update the password to a new value
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        connection.send(wire.logoff());
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        // Should be back in authentication state so should be able to log back on
    //        connection.send(wire.logon(Map.of(
    //                "scheme", "basic",
    //                "principal", "neo4j",
    //                "credentials", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //    }
    //
    //    @ProtocolTest
    //    void shouldNotBeAbleToAuthenticateOnHelloMessage(BoltWire wire, @VersionSelected TransportConnection
    // connection)
    //            throws IOException {
    //        // authenticate normally using the preset credentials and update the password to a new value
    //        connection.send(wire.hello(x -> x.withBasicAuth("neo4j", "neo4j")));
    //
    //        BoltConnectionAssertions.assertThat(connection).receivesSuccess();
    //
    //        // Attempt to start a transaction this will fail because you are in authentication state and not
    // authenticated
    //        connection.send(wire.begin());
    //
    //        BoltConnectionAssertions.assertThat(connection)
    //                .receivesFailureFuzzy(
    //                        Status.Request.Invalid, "cannot be handled by a session in the AUTHENTICATION state.");
    //    }

    @AfterAll
    public static void teardown() {
        database.shutdown();
    }
}
