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
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
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
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceParametersIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;
    private static QueryAPITestClient testClient;

    private static String queryEndpoint;

    @BeforeAll
    static void beforeAll() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceParametersIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
        testClient = new QueryAPITestClient(queryEndpoint);
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
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", parameter))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords(parameter);
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", "Hello"))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords("Hello");
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(Object parameter) throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", Map.of("mappy", parameter)))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get("mappy").asText())
                .isEqualTo(parameter.toString());
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", Map.of("mappy", Map.of("inception", 123))))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();
        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get("mappy")
                        .get("inception")
                        .asInt())
                .isEqualTo(123);
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(Object parameter) throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", List.of(parameter)))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(0).asText()).isEqualTo(parameter.toString());
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", List.of(List.of(123))))
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();
        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(0).get(0).asInt())
                .isEqualTo(123);
    }

    @Test
    void shouldReturnErrorIfParametersDoesNotContainMap() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN $parameter\"," + "\"parameters\": 123}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);

        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Bad Request\"}]}");
    }

    @Test
    @Disabled
    void shouldNotAcceptOutOfRangeNumbers() {
        // todo - needs additional validation in object mapper.
    }
}
