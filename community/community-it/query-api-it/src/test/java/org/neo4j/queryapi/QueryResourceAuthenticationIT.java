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
package org.neo4j.queryapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

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
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceAuthenticationIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeEach
    void beforeEach() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(GraphDatabaseSettings.auth_enabled, true)
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterEach
    void cleanUp() {
        dbms.shutdown();
    }

    @Test
    void shouldRequireCredentialChange() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "system")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"SHOW USERS\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        var parsedResponse = MAPPER.readTree(response.body());

        assertThat(parsedResponse.get("errors").size()).isEqualTo(1);
        assertThat(parsedResponse.get("errors").get(0).get("error").asText())
                .isEqualTo(Status.Security.CredentialsExpired.code().serialize());
    }

    @Test
    void shouldAllowAccessWhenPasswordChanged() throws IOException, InterruptedException {
        updateInitialPassword();

        var accessRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "secretPassword"))
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
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "I'm sneaky!"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(401);

        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Security.Unauthorized\",\"message\":\"Invalid credential.\"}]}");
    }

    @Test
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
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
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
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
            var req = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                    .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "WrongPasswordBud"))
                    .POST(HttpRequest.BodyPublishers.ofString("shouldn't be parsing this"))
                    .build();
            response = client.send(req, HttpResponse.BodyHandlers.ofString());
        } while (response.statusCode() != 429);
        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Security.AuthenticationRateLimit\",\"message\":\"Too many failed authentication requests. Please wait 5 seconds and try again.\"}]}");
    }

    private static void updateInitialPassword() throws IOException, InterruptedException {
        var updatePasswordReq = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "system")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'\"}"))
                .build();

        var updatePasswordResp = client.send(updatePasswordReq, HttpResponse.BodyHandlers.ofString());

        assertThat(updatePasswordResp.statusCode()).isEqualTo(202);
    }

    @AfterAll
    static void teardown() {
        if (dbms != null) {
            dbms.shutdown();
        }
    }
}
