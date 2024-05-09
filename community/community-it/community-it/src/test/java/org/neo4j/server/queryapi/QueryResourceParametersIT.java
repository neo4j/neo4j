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
package org.neo4j.server.queryapi;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.server.queryapi.QueryClientUtil.baseRequestBuilder;
import static org.neo4j.server.queryapi.QueryClientUtil.resolveDependency;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
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

class QueryResourceParametersIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

    @BeforeAll
    static void beforeAll() {
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceParametersIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    public static Stream<Object> paramTypes() {
        return Stream.of(true, 123, 123L, 12.3F, 12.3D, Integer.MAX_VALUE, Long.MAX_VALUE);
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleParameters(Object parameter) throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + parameter + "}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).toString()).isEqualTo(parameter.toString());
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN $parameter\"," + "\"parameters\": {\"parameter\": \"Hello\"}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).asText()).isEqualTo("Hello");
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(Object parameter) throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"mappy\": " + parameter + "}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get("mappy").asText())
                .isEqualTo(parameter.toString());
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"mappy\": {\"inception\": 123}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get("mappy")
                        .get("inception")
                        .asInt())
                .isEqualTo(123);
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(Object parameter) throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": [" + parameter + "]}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).asText())
                .isEqualTo(parameter.toString());
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN $parameter\"," + "\"parameters\": {\"parameter\": [[123]]}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(0).asInt())
                .isEqualTo(123);
    }

    @Test
    void shouldReturnErrorIfParametersDoesNotContainMap() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN $parameter\"," + "\"parameters\": 123}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);

        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"error\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Bad Request\"}]}");
    }

    @Test
    @Disabled
    void shouldNotAcceptOutOfRangeNumbers() {
        // todo - needs additional validation in object mapper.
    }
}
