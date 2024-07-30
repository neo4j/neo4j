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
import static org.neo4j.server.queryapi.response.TypedJsonDriverResultWriter.TYPED_JSON_MIME_TYPE_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
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
import org.junit.jupiter.params.provider.Arguments;
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

class QueryResourceTypedParametersIT {

    private static DatabaseManagementService dbms;
    private static HttpClient client;

    private static String queryEndpoint;

    private final ObjectMapper MAPPER = new ObjectMapper();

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
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        client = HttpClient.newBuilder().build();
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    public static Stream<Arguments> paramTypes() {
        return Stream.of(
                Arguments.of("Integer", 123),
                Arguments.of("Integer", Integer.MAX_VALUE),
                Arguments.of("Integer", Long.MAX_VALUE),
                Arguments.of("Float", 12.3F),
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
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"" + typeString + "\",\"_value\":\""
                        + value.toString() + "\"}}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .asText())
                .isEqualTo(value.toString());
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo(typeString);
    }

    @Test
    void shouldHandleBooleanParameter() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"Boolean\",\"_value\": true}}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .asBoolean())
                .isEqualTo(true);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Boolean");
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + "{\"$type\":\"String\",\"_value\":\"Hello\"}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .asText())
                .isEqualTo("Hello");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("String");
    }

    @Test
    void shouldHandleNullParameter() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"Null\",\"_value\": null}}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertTrue(parsedJson
                .get(DATA_KEY)
                .get(VALUES_KEY)
                .get(0)
                .get(0)
                .get(CYPHER_VALUE)
                .isNull());
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Null");
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(String typeString, Object value) throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":" + "{\"mappy\": {\"$type\":\""
                        + typeString + "\", \"_value\": \"" + value.toString() + "\"} }}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .textValue())
                .isEqualTo("Map");

        var insideParam = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(insideParam.get("mappy").get(CYPHER_TYPE).textValue()).isEqualTo(typeString);
        assertThat(insideParam.get("mappy").get(CYPHER_VALUE).textValue()).isEqualTo(value.toString());
    }

    @Test
    void shouldHandleMapWithBoolean() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":"
                        + "{\"true\": {\"$type\":\"Boolean\", \"_value\": true},"
                        + "\"false\": {\"$type\":\"Boolean\", \"_value\": false} }}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .textValue())
                .isEqualTo("Map");

        var insideParam = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(insideParam.get("true").get(CYPHER_TYPE).textValue()).isEqualTo("Boolean");
        assertThat(insideParam.get("true").get(CYPHER_VALUE).booleanValue()).isEqualTo(true);
        assertThat(insideParam.get("false").get(CYPHER_TYPE).textValue()).isEqualTo("Boolean");
        assertThat(insideParam.get("false").get(CYPHER_VALUE).booleanValue()).isEqualTo(false);
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Map\", \"_value\": {\"mappy\": "
                        + "{\"$type\": \"Map\", \"_value\": {\"inception\": "
                        + "{\"$type\": \"Integer\", \"_value\": \"123\"}}}}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var result = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
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
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": "
                        + "[{\"$type\":\"Map\",\"_value\":{\"innerMap\": "
                        + "{\"$type\":\"Boolean\",\"_value\":true}}}]}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Map");
        assertThat(parsedJson
                        .get(DATA_KEY)
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
                        .get(DATA_KEY)
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
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Map\", \"_value\": {}}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var result = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(result.get(CYPHER_TYPE).asText()).isEqualTo("Map");
        assertThat(result.get(CYPHER_VALUE).size()).isEqualTo(0);
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(String typeString, Object value) throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"" + typeString
                        + "\", \"_value\": \"" + value.toString() + "\"}]}}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("List");

        var insideValue = parsedJson
                .get(DATA_KEY)
                .get(VALUES_KEY)
                .get(0)
                .get(0)
                .get(CYPHER_VALUE)
                .get(0);
        assertThat(insideValue.get(CYPHER_TYPE).asText()).isEqualTo(typeString);
        assertThat(insideValue.get(CYPHER_VALUE).asText()).isEqualTo(value.toString());
    }

    @Test
    void shouldHandleEmptyList() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + "{\"$type\": \"List\", \"_value\": []}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var result = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);

        assertThat(result.get(CYPHER_TYPE).asText()).isEqualTo("List");
        assertThat(result.get(CYPHER_VALUE).size()).isEqualTo(0);
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(
                        HttpRequest.BodyPublishers.ofString(
                                "{\"statement\": \"RETURN $parameter\","
                                        + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"List\",\"_value\":[{\"$type\":\"Boolean\",\"_value\":true}]}]}}}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("List");
        assertThat(parsedJson
                        .get(DATA_KEY)
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
                        .get(DATA_KEY)
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
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
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
    void shouldHandleInvalidType() throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Bananas\", \"_value\": []}}}"))
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

    public static HttpRequest.Builder baseRequestBuilder(String endpoint, String databaseName) {
        return HttpRequest.newBuilder()
                .uri(URI.create(endpoint.replace("{databaseName}", databaseName)))
                .header("Content-Type", TYPED_JSON_MIME_TYPE_VALUE)
                .header("Accept", TYPED_JSON_MIME_TYPE_VALUE);
    }
}
