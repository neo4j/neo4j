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

import static com.fasterxml.jackson.databind.node.TextNode.valueOf;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.server.queryapi.response.TypedJsonDriverResultWriter.TYPED_JSON_MIME_TYPE_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames._ELEMENT_ID;
import static org.neo4j.server.queryapi.response.format.Fieldnames._END_NODE_ELEMENT_ID;
import static org.neo4j.server.queryapi.response.format.Fieldnames._LABELS;
import static org.neo4j.server.queryapi.response.format.Fieldnames._PROPERTIES;
import static org.neo4j.server.queryapi.response.format.Fieldnames._RELATIONSHIP_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames._START_NODE_ELEMENT_ID;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.EnumSet;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.response.format.Fieldnames;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceTypedJsonIT {

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
                        QueryResourceTypedJsonIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
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

    @Test
    void basicTypes() throws IOException, InterruptedException {
        var response = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN true as bool, 1 as number, "
                        + "null as aNull, 1.23 as float, 'hello' as string\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(5);
        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY))
                .containsExactly(
                        valueOf("bool"), valueOf("number"), valueOf("aNull"), valueOf("float"), valueOf("string"));
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
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
                        .asBoolean())
                .isEqualTo(true);
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(1, "Integer", "1");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(2)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Null");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(2)
                        .get(CYPHER_VALUE)
                        .isNull())
                .isEqualTo(true);
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(3, "Float", "1.23");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(4, "String", "hello");
    }

    @Test
    void temporalTypes() throws IOException, InterruptedException {
        var response = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN datetime('2015-06-24T12:50:35.556+0100') AS theOffsetDateTime, "
                        + "datetime('2015-11-21T21:40:32.142[Antarctica/Troll]') AS theZonedDateTime, "
                        + "localdatetime('2015185T19:32:24') AS theLocalDateTime, "
                        + "date('+2015-W13-4') AS theDate, "
                        + "time('125035.556+0100') AS theTime, "
                        + "localtime('12:50:35.556') AS theLocalTime\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY))
                .containsExactly(
                        valueOf("theOffsetDateTime"),
                        valueOf("theZonedDateTime"),
                        valueOf("theLocalDateTime"),
                        valueOf("theDate"),
                        valueOf("theTime"),
                        valueOf("theLocalTime"));

        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0);
        assertThat(results.size()).isEqualTo(6);
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(0, "OffsetDateTime", "2015-06-24T12:50:35.556+01:00");
        QueryAssertions.assertThat(parsedJson)
                .hasTypedResultAt(1, "ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(2, "LocalDateTime", "2015-07-04T19:32:24");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(3, "Date", "2015-03-26");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(4, "Time", "12:50:35.556+01:00");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(5, "LocalTime", "12:50:35.556");
    }

    @Test
    void duration() throws IOException, InterruptedException {
        var response = simpleRequest(
                client, queryEndpoint, "{\"statement\": \"RETURN duration('P14DT16H12M') AS theDuration\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY)).containsExactly(valueOf("theDuration"));

        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(0, "Duration", "P14DT16H12M");
    }

    @Test
    void binary() throws IOException, InterruptedException {
        try (var tx = dbms.database("neo4j").beginTx()) {
            tx.createNode(Label.label("FindMe")).setProperty("binaryGoodness", new byte[] {1, 2, 3, 4, 5});
            tx.commit();
        }

        var response = simpleRequest(client, queryEndpoint, "{\"statement\": \"MATCH (n:FindMe) return n\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());
        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY);
        QueryAssertions.assertThat(
                        results.get(0).get(0).get(CYPHER_VALUE).get(_PROPERTIES).get("binaryGoodness"))
                .hasTypedResult("Base64", "AQIDBAU=");
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN {key: 'Value', listKey: [{inner1: 'Map1'}, {inner2: 'Map2'}]} AS map\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).get(0).asText()).isEqualTo("map");

        var map = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);

        assertThat(map.get(CYPHER_TYPE).asText()).isEqualTo("Map");

        QueryAssertions.assertThat(map.get(CYPHER_VALUE).get("key")).hasTypedResult("String", "Value");
        QueryAssertions.assertThat(map.get(CYPHER_VALUE)
                        .get("listKey")
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_VALUE)
                        .get("inner1"))
                .hasTypedResult("String", "Map1");
        assertThat(map.get(CYPHER_VALUE)
                        .get("listKey")
                        .get(CYPHER_VALUE)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Map");
        QueryAssertions.assertThat(map.get(CYPHER_VALUE)
                        .get("listKey")
                        .get(CYPHER_VALUE)
                        .get(1)
                        .get(CYPHER_VALUE)
                        .get("inner2"))
                .hasTypedResult("String", "Map2");
        assertThat(map.get(CYPHER_VALUE)
                        .get("listKey")
                        .get(CYPHER_VALUE)
                        .get(1)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Map");
    }

    @Test
    void list() throws IOException, InterruptedException {
        var response = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN [1,true,'hello',date('+2015-W13-4'), {amap: 'hello'}] as list\"}");

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

        var resultArray = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(resultArray.size()).isEqualTo(5);
        QueryAssertions.assertThat(resultArray.get(0)).hasTypedResult("Integer", "1");
        assertThat(resultArray.get(1).get(CYPHER_TYPE).asText()).isEqualTo("Boolean");
        assertThat(resultArray.get(1).get(CYPHER_VALUE).asBoolean()).isEqualTo(true);
        QueryAssertions.assertThat(resultArray.get(2)).hasTypedResult("String", "hello");
        QueryAssertions.assertThat(resultArray.get(3)).hasTypedResult("Date", "2015-03-26");
        assertThat(resultArray.get(4).get(CYPHER_TYPE).asText()).isEqualTo("Map");
        assertThat(resultArray
                        .get(4)
                        .get(CYPHER_VALUE)
                        .get("amap")
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("String");
        assertThat(resultArray
                        .get(4)
                        .get(CYPHER_VALUE)
                        .get("amap")
                        .get(CYPHER_VALUE)
                        .asText())
                .isEqualTo("hello");
    }

    @Test
    void node() throws IOException, InterruptedException {
        var response = simpleRequest(
                client, queryEndpoint, "{\"statement\": \"CREATE (n:MyLabel {aNumber: 1234}) RETURN n\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        var node = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);
        assertThat(node.get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(node.get(CYPHER_VALUE).get(Fieldnames._ELEMENT_ID).asText()).isNotBlank();
        assertThat(node.get(CYPHER_VALUE).get(_LABELS).size()).isEqualTo(1);
        assertThat(node.get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("MyLabel");
        QueryAssertions.assertThat(node.get(CYPHER_VALUE).get(_PROPERTIES).get("aNumber"))
                .hasTypedResult("Integer", "1234");
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = simpleRequest(
                client, queryEndpoint, "{\"statement\": \"CREATE (a)-[r:RELTYPE {onFire: 'owch!'}]->(b) RETURN r\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var rel = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0);
        assertThat(rel.get(CYPHER_TYPE).asText()).isEqualTo("Relationship");
        assertThat(rel.get(CYPHER_VALUE).get(_ELEMENT_ID).asText()).isNotBlank();
        assertThat(rel.get(CYPHER_VALUE).get(_START_NODE_ELEMENT_ID).asText()).isNotBlank();
        assertThat(rel.get(CYPHER_VALUE).get(_END_NODE_ELEMENT_ID).asText()).isNotBlank();
        assertThat(rel.get(CYPHER_VALUE).get(_RELATIONSHIP_TYPE).asText()).isEqualTo("RELTYPE");
        QueryAssertions.assertThat(rel.get(CYPHER_VALUE).get(_PROPERTIES).get("onFire"))
                .hasTypedResult("String", "owch!");
    }

    @Test
    void simplePath() throws IOException, InterruptedException {
        var createPathReq =
                simpleRequest(client, queryEndpoint, "{\"statement\": \"CREATE (a:LabelA)-[rel1:REL]->(b:LabelB)\"}");

        assertThat(createPathReq.statusCode()).isEqualTo(202);
        var response = simpleRequest(
                client, queryEndpoint, "{\"statement\": \"MATCH p=(a:LabelA)-[rel1:REL]->(b:LabelB) RETURN p\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Path");

        var path = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(path.get(0).get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(path.get(0).get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("LabelA");

        assertThat(path.get(1).get(CYPHER_TYPE).asText()).isEqualTo("Relationship");
        assertThat(path.get(1).get(CYPHER_VALUE).get(_RELATIONSHIP_TYPE).asText())
                .isEqualTo("REL");

        // ensure relationship points the correct way.
        assertThat(path.get(1).get(CYPHER_VALUE).get(_START_NODE_ELEMENT_ID))
                .isEqualTo(path.get(0).get(CYPHER_VALUE).get(_ELEMENT_ID));
        assertThat(path.get(1).get(CYPHER_VALUE).get(_END_NODE_ELEMENT_ID))
                .isEqualTo(path.get(2).get(CYPHER_VALUE).get(_ELEMENT_ID));

        assertThat(path.get(2).get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(path.get(2).get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("LabelB");
    }

    @Test
    void path() throws IOException, InterruptedException {
        var createPathReq = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)\"}");

        assertThat(createPathReq.statusCode()).isEqualTo(202);
        var response = simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Path");

        var path = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

        assertThat(path.get(0).get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(path.get(0).get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("LabelA");

        assertThat(path.get(1).get(CYPHER_TYPE).asText()).isEqualTo("Relationship");
        assertThat(path.get(1).get(CYPHER_VALUE).get(_RELATIONSHIP_TYPE).asText())
                .isEqualTo("RELAB");

        assertThat(path.get(2).get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(path.get(2).get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("LabelB");

        assertThat(path.get(3).get(CYPHER_TYPE).asText()).isEqualTo("Relationship");
        assertThat(path.get(3).get(CYPHER_VALUE).get(_RELATIONSHIP_TYPE).asText())
                .isEqualTo("RELCB");

        assertThat(path.get(4).get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(path.get(4).get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("LabelC");
    }

    public static HttpRequest.Builder baseRequestBuilder(String endpoint, String databaseName) {
        return HttpRequest.newBuilder()
                .uri(URI.create(endpoint.replace("{databaseName}", databaseName)))
                .header("Content-Type", "application/json")
                .header("Accept", TYPED_JSON_MIME_TYPE_VALUE);
    }

    public static HttpResponse<String> simpleRequest(
            HttpClient client, String endpoint, String database, String requestBody)
            throws IOException, InterruptedException {
        var httpRequest = baseRequestBuilder(endpoint, database)
                .POST(HttpRequest.BodyPublishers.ofString(requestBody))
                .build();

        return client.send(httpRequest, HttpResponse.BodyHandlers.ofString());
    }

    public static HttpResponse<String> simpleRequest(HttpClient client, String endpoint, String requestBody)
            throws IOException, InterruptedException {
        return simpleRequest(client, endpoint, "neo4j", requestBody);
    }
}
