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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceTypedParametersIT {

    private static DatabaseManagementService dbms;
    private static QueryAPITestClient testClient;

    @BeforeAll
    static void beforeAll() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceTypedParametersIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        var queryEndpoint =
                "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient = new QueryAPITestClient(
                queryEndpoint, QueryContentType.TYPED_V1_0, List.of(QueryContentType.TYPED_V1_0));
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    public static Stream<Arguments> paramTypes() {
        return Stream.of(
                Arguments.of("Integer", "123"),
                Arguments.of("Integer", String.valueOf(Integer.MAX_VALUE)),
                Arguments.of("Integer", String.valueOf(Long.MAX_VALUE)),
                Arguments.of("Float", "12.3"),
                Arguments.of("Base64", "YmFuYW5hcw=="),
                Arguments.of("OffsetDateTime", "2015-06-24T12:50:35.556+01:00"),
                Arguments.of("ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]"),
                Arguments.of("LocalDateTime", "2015-07-04T19:32:24"),
                Arguments.of("Date", "2015-03-26"),
                Arguments.of("Time", "12:50:35.556+01:00"),
                Arguments.of("LocalTime", "12:50:35.556"),
                Arguments.of("Duration", "P14DT16H12M"),
                Arguments.of("Point", "SRID=7203;POINT (2.3 4.5)"),
                Arguments.of("Point", "SRID=9157;POINT Z (2.3 4.5 6.7)"),
                Arguments.of("Point", "SRID=4326;POINT (2.3 4.5)"),
                Arguments.of("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)"));
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", of(typeString, value)))
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();
        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).asText())
                .isEqualTo(value.toString());
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo(typeString);
    }

    @Test
    void shouldHandleBooleanParameter() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", of("Boolean", true)))
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).asBoolean())
                .isEqualTo(true);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("Boolean");
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", of("String", "Hello")))
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).asText())
                .isEqualTo("Hello");
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("String");
    }

    @Test
    void shouldHandleNullParameter() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Null\",\"_value\": null}}}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertTrue(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).isNull());
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("Null");
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":" + "{\"mappy\": {\"$type\":\""
                + typeString + "\", \"_value\": \"" + value.toString() + "\"} }}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).textValue())
                .isEqualTo("Map");

        var insideParam = parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(insideParam.get("mappy").get(CYPHER_TYPE).textValue()).isEqualTo(typeString);
        assertThat(insideParam.get("mappy").get(CYPHER_VALUE).textValue()).isEqualTo(value.toString());
    }

    @Test
    void shouldHandleMapWithBoolean() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":"
                + "{\"true\": {\"$type\":\"Boolean\", \"_value\": true},"
                + "\"false\": {\"$type\":\"Boolean\", \"_value\": false} }}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).textValue())
                .isEqualTo("Map");

        var insideParam = parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(insideParam.get("true").get(CYPHER_TYPE).textValue()).isEqualTo("Boolean");
        assertThat(insideParam.get("true").get(CYPHER_VALUE).booleanValue()).isEqualTo(true);
        assertThat(insideParam.get("false").get(CYPHER_TYPE).textValue()).isEqualTo("Boolean");
        assertThat(insideParam.get("false").get(CYPHER_VALUE).booleanValue()).isEqualTo(false);
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Map\", \"_value\": {\"mappy\": "
                + "{\"$type\": \"Map\", \"_value\": {\"inception\": "
                + "{\"$type\": \"Integer\", \"_value\": \"123\"}}}}}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();
        var result = parsedJson.get(VALUES_KEY).get(0).get(0);

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(result.get(CYPHER_TYPE).asText()).isEqualTo("Map");
        assertThat(result.get(CYPHER_VALUE).get("mappy").get(CYPHER_TYPE).asText())
                .isEqualTo("Map");
        assertThat(result.get(CYPHER_VALUE)
                        .get("mappy")
                        .get(CYPHER_VALUE)
                        .get("inception")
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Integer");
        assertThat(result.get(CYPHER_VALUE)
                        .get("mappy")
                        .get(CYPHER_VALUE)
                        .get("inception")
                        .get(CYPHER_VALUE)
                        .asText())
                .isEqualTo("123");
    }

    @Test
    void shouldHandleMapNestedInList() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": "
                + "[{\"$type\":\"Map\",\"_value\":{\"innerMap\": "
                + "{\"$type\":\"Boolean\",\"_value\":true}}}]}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Map");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get("innerMap")
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Boolean");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get("innerMap")
                        .get(CYPHER_VALUE)
                        .asBoolean())
                .isEqualTo(true);
    }

    @Test
    void shouldHandleEmptyMaps() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\"," + "\"parameters\": {\"parameter\": "
                + "{\"$type\": \"Map\", \"_value\": {}}}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();
        var result = parsedJson.get(VALUES_KEY).get(0).get(0);

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(result.get(CYPHER_TYPE).asText()).isEqualTo("Map");
        assertThat(result.get(CYPHER_VALUE).size()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"" + typeString
                + "\", \"_value\": \"" + value.toString() + "\"}]}}}}");

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("List");

        var insideValue =
                parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).get(0);
        assertThat(insideValue.get(CYPHER_TYPE).asText()).isEqualTo(typeString);
        assertThat(insideValue.get(CYPHER_VALUE).asText()).isEqualTo(value.toString());
    }

    @Test
    void shouldHandleEmptyList() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\"," + "\"parameters\": {\"parameter\": "
                + "{\"$type\": \"List\", \"_value\": []}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();
        var result = parsedJson.get(VALUES_KEY).get(0).get(0);

        assertThat(result.get(CYPHER_TYPE).asText()).isEqualTo("List");
        assertThat(result.get(CYPHER_VALUE).size()).isEqualTo(0);
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var response = testClient.sendRaw(
                "{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"List\",\"_value\":[{\"$type\":\"Boolean\",\"_value\":true}]}]}}}");

        QueryResponseAssertions.assertThat(response)
                .hasContentType(QueryContentType.TYPED_V1_0)
                .wasSuccessful();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Boolean");
        assertThat(parsedJson
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .asBoolean())
                .isEqualTo(true);
    }

    @Test
    void shouldReturnErrorIfParametersDoesNotContainMap() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\"," + "\"parameters\": 123}");

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
    }

    @Test
    void shouldHandleInvalidType() throws IOException, InterruptedException {
        var response = testClient.sendRaw("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\": \"Bananas\", \"_value\": []}}}");

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
    }

    @Test
    @Disabled
    void shouldNotAcceptOutOfRangeNumbers() {
        // todo - needs additional validation in object mapper.
    }

    private Map<String, Object> of(String type, Object value) {
        // todo For preserving order. Remove after fixing NETCORE-180
        var map = new LinkedHashMap<String, Object>();
        map.put(CYPHER_TYPE, type);
        map.put(CYPHER_VALUE, value);
        return map;
    }
}
