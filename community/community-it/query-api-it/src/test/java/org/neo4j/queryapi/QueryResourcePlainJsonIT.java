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
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import java.io.IOException;
import java.util.List;
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
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.response.format.Fieldnames;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourcePlainJsonIT {

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
                        QueryResourcePlainJsonIT.class.getSimpleName())
                .impermanent()
                .setConfig(BoltConnector.enabled, true)
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        var queryEndpoint =
                "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient = new QueryAPITestClient(queryEndpoint);
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    @Test
    void basicTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN true as bool, 1 as number, 1.23 as float, 'hello' as string")
                .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasFieldNames("bool", "number", "float", "string")
                .hasRecords(List.of(List.of(true, 1, 1.23f, "hello")));
    }

    @Test
    void nullType() throws IOException, InterruptedException {
        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("RETURN null as aNull").build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasFieldNames("aNull");

        assertTrue(response.body().data().get(VALUES_KEY).get(0).get(0).isNull());
    }

    @Test
    void temporalTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN datetime('2015-06-24T12:50:35.556+0100') AS theOffsetDateTime, "
                        + "datetime('2015-11-21T21:40:32.142[Antarctica/Troll]') AS theZonedDateTime,"
                        + "datetime('2025-10-26T02:30:00+01:00[Europe/Stockholm]') AS theZonedDateTimeOnDSTSwitch,"
                        + "localdatetime('2015185T19:32:24') AS theLocalDateTime,"
                        + "date('+2015-W13-4') AS theDate,"
                        + "time('125035.556+0100') AS theTime,"
                        + "localtime('12:50:35.556') AS theLocalTime")
                .build());

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasFieldNames(
                        "theOffsetDateTime",
                        "theZonedDateTime",
                        "theZonedDateTimeOnDSTSwitch",
                        "theLocalDateTime",
                        "theDate",
                        "theTime",
                        "theLocalTime");

        var results = response.body().data().get(VALUES_KEY).get(0);
        assertThat(results.size()).isEqualTo(7);
        assertThat(results.get(0).asText()).isEqualTo("2015-06-24T12:50:35.556+01:00");
        assertThat(results.get(1).asText()).isEqualTo("2015-11-21T21:40:32.142Z");
        assertThat(results.get(2).asText()).isEqualTo("2025-10-26T02:30:00+01:00");
        assertThat(results.get(3).asText()).isEqualTo("2015-07-04T19:32:24");
        assertThat(results.get(4).asText()).isEqualTo("2015-03-26");
        assertThat(results.get(5).asText()).isEqualTo("12:50:35.556+01:00");
        assertThat(results.get(6).asText()).isEqualTo("12:50:35.556");
    }

    @Test
    void point() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN point({x: 2.3, y: 4.5}), point({x: 2.3, y: 4.5, z: 6.7}),"
                        + "point({x:2.3, y:4.5, srid:4326}),"
                        + "point({x: 2.3, y: 4.5, crs: 'WGS-84'}),"
                        + "point({x:2.3, y:4.5, z:6.7, srid:4979}),"
                        + "point({x: 2.3, y: 4.5, z: 6.7, crs: 'WGS-84-3D'}),"
                        + "point({longitude: 56.7, latitude: 12.78}),"
                        + "point({longitude: 56.7, latitude: 12.78, height: 8})")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var results = response.body().data().get(VALUES_KEY).get(0);
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
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN duration('P14DT16H12M') AS theDuration")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasFieldNames("theDuration");

        var results = response.body().data().get(VALUES_KEY);
        assertThat(results.get(0).get(0).asText()).isEqualTo("P14DT16H12M");
    }

    @Test
    void binary() throws IOException, InterruptedException {
        try (var tx = dbms.database("neo4j").beginTx()) {
            tx.createNode(Label.label("FindMe")).setProperty("binaryGoodness", new byte[] {1, 2, 3, 4, 5});
            tx.commit();
        }

        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("MATCH (n:FindMe) return n").build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var results = response.body().data().get(VALUES_KEY);
        assertThat(results.get(0)
                        .get(0)
                        .get(Fieldnames.PROPERTIES)
                        .get("binaryGoodness")
                        .asText())
                .isEqualTo("AQIDBAU=");
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN {key: 'Value', listKey: [{inner: 'Map1'}, {inner: 'Map2'}]} AS map")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasFieldNames("map");

        var values = response.body().data().get(VALUES_KEY);
        assertThat(values.get(0).get(0).get("key").asText()).isEqualTo("Value");
        assertThat(values.get(0).get(0).get("listKey").get(0).get("inner").asText())
                .isEqualTo("Map1");
        assertThat(values.get(0).get(0).get("listKey").get(1).get("inner").asText())
                .isEqualTo("Map2");
    }

    @Test
    void list() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN [1,true,'hello',date('+2015-W13-4')] as list")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasFieldNames("list");

        var resultArray = response.body().data().get(VALUES_KEY).get(0).get(0);
        assertThat(resultArray.size()).isEqualTo(4);
        assertThat(resultArray.get(0).asInt()).isEqualTo(1);
        assertThat(resultArray.get(1).asBoolean()).isEqualTo(true);
        assertThat(resultArray.get(2).asText()).isEqualTo("hello");
        assertThat(resultArray.get(3).asText()).isEqualTo("2015-03-26");
    }

    @Test
    void node() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (n:MyLabel {aNumber: 1234}) RETURN n")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var node = response.body().data().get(VALUES_KEY).get(0).get(0);
        assertThat(node.get("elementId").asText()).isNotBlank();
        assertThat(node.get("labels").size()).isEqualTo(1);
        assertThat(node.get("labels").get(0).asText()).isEqualTo("MyLabel");
        assertThat(node.get(Fieldnames.PROPERTIES).get("aNumber").asInt()).isEqualTo(1234);
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (a)-[r:RELTYPE {onFire: true}]->(b) RETURN r")
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        var rel = response.body().data().get(VALUES_KEY).get(0).get(0);
        assertThat(rel.get("elementId").asText()).isNotBlank();
        assertThat(rel.get("startNodeElementId").asText()).isNotBlank();
        assertThat(rel.get("endNodeElementId").asText()).isNotBlank();
        assertThat(rel.get("type").asText()).isEqualTo("RELTYPE");
        assertThat(rel.get(Fieldnames.PROPERTIES).get("onFire").asBoolean()).isEqualTo(true);
    }

    @Test
    void path() throws IOException, InterruptedException {
        var createPathReq = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)")
                .build());

        QueryResponseAssertions.assertThat(createPathReq).wasSuccessful();
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p")
                .build());

        var path = response.body().data().get(VALUES_KEY).get(0).get(0);

        assertThat(path.get(0).get("labels").get(0).asText()).isEqualTo("LabelA");
        assertThat(path.get(1).get("type").asText()).isEqualTo("RELAB");
        assertThat(path.get(2).get("labels").get(0).asText()).isEqualTo("LabelB");
        assertThat(path.get(3).get("type").asText()).isEqualTo("RELCB");
        assertThat(path.get(4).get("labels").get(0).asText()).isEqualTo("LabelC");
    }
}
