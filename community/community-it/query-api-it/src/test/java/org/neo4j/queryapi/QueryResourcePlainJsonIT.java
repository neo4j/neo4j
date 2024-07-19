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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.server.queryapi.response.format.Fieldnames.DATA_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpClient;
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

class QueryResourcePlainJsonIT {

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
                        QueryResourcePlainJsonIT.class.getSimpleName())
                .impermanent()
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
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
        var response = QueryApiTestUtil.simpleRequest(
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
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).asBoolean()).isEqualTo(true);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(1).asInt()).isEqualTo(1);
        assertTrue(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(2).isNull());
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(3).asDouble()).isEqualTo(1.23);
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(4).asText()).isEqualTo("hello");
    }

    @Test
    void temporalTypes() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
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

        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY);
        assertThat(results.size()).isEqualTo(6);
        assertThat(results.get(0).asText()).isEqualTo("2015-06-24T12:50:35.556+01:00");
        assertThat(results.get(1).asText()).isEqualTo("2015-11-21T21:40:32.142Z");
        assertThat(results.get(2).asText()).isEqualTo("2015-07-04T19:32:24");
        assertThat(results.get(3).asText()).isEqualTo("2015-03-26");
        assertThat(results.get(4).asText()).isEqualTo("12:50:35.556+01:00");
        assertThat(results.get(5).asText()).isEqualTo("12:50:35.556");
    }

    @Test
    void point() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN point({x: 2.3, y: 4.5})," + "point({x: 2.3, y: 4.5, z: 6.7}),"
                        + "point({x:2.3, y:4.5, srid:4326}),"
                        + "point({x: 2.3, y: 4.5, crs: 'WGS-84'}),"
                        + "point({x:2.3, y:4.5, z:6.7, srid:4979}),"
                        + "point({x: 2.3, y: 4.5, z: 6.7, crs: 'WGS-84-3D'}),"
                        + "point({longitude: 56.7, latitude: 12.78}),"
                        + "point({longitude: 56.7, latitude: 12.78, height: 8})\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());

        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY);
        assertThat(results.get(0).asText()).isEqualTo("SRID=7203;POINT (2.3 4.5)");
        assertThat(results.get(1).asText()).isEqualTo("SRID=9157;POINT Z (2.3 4.5 6.7)");
        assertThat(results.get(2).asText()).isEqualTo("SRID=4326;POINT (2.3 4.5)");
        assertThat(results.get(3).asText()).isEqualTo("SRID=4326;POINT (2.3 4.5)");
        assertThat(results.get(4).asText()).isEqualTo("SRID=4979;POINT Z (2.3 4.5 6.7)");
        assertThat(results.get(5).asText()).isEqualTo("SRID=4979;POINT Z (2.3 4.5 6.7)");
        assertThat(results.get(6).asText()).isEqualTo("SRID=4326;POINT (56.7 12.78)");
        assertThat(results.get(7).asText()).isEqualTo("SRID=4979;POINT Z (56.7 12.78 8.0)");
    }

    @Test
    void duration() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"RETURN duration('P14DT16H12M') AS theDuration\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY)).containsExactly(valueOf("theDuration"));

        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY);
        assertThat(results.get(0).asText()).isEqualTo("P14DT16H12M");
    }

    @Test
    void binary() throws IOException, InterruptedException {
        try (var tx = dbms.database("neo4j").beginTx()) {
            tx.createNode(Label.label("FindMe")).setProperty("binaryGoodness", new byte[] {1, 2, 3, 4, 5});
            tx.commit();
        }

        var response =
                QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"MATCH (n:FindMe) return n\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());
        var results = parsedJson.get(DATA_KEY).get(VALUES_KEY);
        assertThat(results.get(0)
                        .get(Fieldnames.PROPERTIES)
                        .get("binaryGoodness")
                        .asText())
                .isEqualTo("AQIDBAU=");
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"RETURN {key: 'Value', listKey: [{inner: 'Map1'}, {inner: 'Map2'}]} AS map\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).get(0).asText()).isEqualTo("map");
        assertThat(parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0).get("key").asText())
                .isEqualTo("Value");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get("listKey")
                        .get(0)
                        .get("inner")
                        .asText())
                .isEqualTo("Map1");
        assertThat(parsedJson
                        .get(DATA_KEY)
                        .get(VALUES_KEY)
                        .get(0)
                        .get("listKey")
                        .get(1)
                        .get("inner")
                        .asText())
                .isEqualTo("Map2");
    }

    @Test
    void list() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"RETURN [1,true,'hello',date('+2015-W13-4')] as list\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(DATA_KEY).get(FIELDS_KEY).size()).isEqualTo(1);

        var resultArray = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0);
        assertThat(resultArray.size()).isEqualTo(4);
        assertThat(resultArray.get(0).asInt()).isEqualTo(1);
        assertThat(resultArray.get(1).asBoolean()).isEqualTo(true);
        assertThat(resultArray.get(2).asText()).isEqualTo("hello");
        assertThat(resultArray.get(3).asText()).isEqualTo("2015-03-26");
    }

    @Test
    void node() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"CREATE (n:MyLabel {aNumber: 1234}) RETURN n\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedJson = MAPPER.readTree(response.body());

        var node = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0);
        assertThat(node.get("elementId").asText()).isNotBlank();
        assertThat(node.get("labels").size()).isEqualTo(1);
        assertThat(node.get("labels").get(0).asText()).isEqualTo("MyLabel");
        assertThat(node.get(Fieldnames.PROPERTIES).get("aNumber").asInt()).isEqualTo(1234);
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"CREATE (a)-[r:RELTYPE {onFire: true}]->(b) RETURN r\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var rel = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0);
        assertThat(rel.get("elementId").asText()).isNotBlank();
        assertThat(rel.get("startNodeElementId").asText()).isNotBlank();
        assertThat(rel.get("endNodeElementId").asText()).isNotBlank();
        assertThat(rel.get("type").asText()).isEqualTo("RELTYPE");
        assertThat(rel.get(Fieldnames.PROPERTIES).get("onFire").asBoolean()).isEqualTo(true);
    }

    @Test
    void path() throws IOException, InterruptedException {
        var createPathReq = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)\"}");

        assertThat(createPathReq.statusCode()).isEqualTo(202);
        var response = QueryApiTestUtil.simpleRequest(
                client,
                queryEndpoint,
                "{\"statement\": \"MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        var parsedJson = MAPPER.readTree(response.body());
        var path = parsedJson.get(DATA_KEY).get(VALUES_KEY).get(0);

        assertThat(path.get(0).get("labels").get(0).asText()).isEqualTo("LabelA");
        assertThat(path.get(1).get("type").asText()).isEqualTo("RELAB");
        assertThat(path.get(2).get("labels").get(0).asText()).isEqualTo("LabelB");
        assertThat(path.get(3).get("type").asText()).isEqualTo("RELCB");
        assertThat(path.get(4).get("labels").get(0).asText()).isEqualTo("LabelC");
    }
}
