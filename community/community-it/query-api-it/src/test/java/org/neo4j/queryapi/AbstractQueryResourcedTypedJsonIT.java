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
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.FIELDS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames._ELEMENT_ID;
import static org.neo4j.server.queryapi.response.format.Fieldnames._END_NODE_ELEMENT_ID;
import static org.neo4j.server.queryapi.response.format.Fieldnames._LABELS;
import static org.neo4j.server.queryapi.response.format.Fieldnames._PROPERTIES;
import static org.neo4j.server.queryapi.response.format.Fieldnames._RELATIONSHIP_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames._START_NODE_ELEMENT_ID;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.response.format.Fieldnames;

abstract class AbstractQueryResourcedTypedJsonIT {
    private final DatabaseManagementService dbms;
    protected final QueryAPITestClient testClient;

    AbstractQueryResourcedTypedJsonIT(DatabaseManagementService dbms, QueryAPITestClient testClient) {
        this.dbms = dbms;
        this.testClient = testClient;
    }

    protected abstract QueryContentType contentType();

    @Test
    void basicTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN true as bool, 1 as number, 1.23 as float, 'hello' as string")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("bool", "number", "float", "string")
                .hasTimers();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("Boolean");
        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE).asBoolean())
                .isEqualTo(true);
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(1, "Integer", "1");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(2, "Float", "1.23");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(3, "String", "hello");
    }

    @Test
    void nullType() throws IOException, InterruptedException {
        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("RETURN null as aNull").build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("aNull")
                .hasTimers();

        assertThat(response.body()
                .data()
                .get(VALUES_KEY)
                .get(0)
                .get(0)
                .get(CYPHER_VALUE)
                .isNull());
        assertThat(response.body()
                        .data()
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .get(CYPHER_TYPE)
                        .asText())
                .isEqualTo("Null");
    }

    @Test
    void floatTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement(
                        "RETURN 1.23 as float, -1.23 as negativeFloat, NaN as nan, Infinity as infinity, -Infinity as negativeInfinity ")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("float", "negativeFloat", "nan", "infinity", "negativeInfinity")
                .hasTimers();

        var parsedJson = response.body().data();

        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(0, "Float", "1.23");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(1, "Float", "-1.23");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(2, "Float", "NaN");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(3, "Float", "Infinity");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(4, "Float", "-Infinity");
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
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames(
                        "theOffsetDateTime",
                        "theZonedDateTime",
                        "theZonedDateTimeOnDSTSwitch",
                        "theLocalDateTime",
                        "theDate",
                        "theTime",
                        "theLocalTime")
                .hasTimers();

        var parsedJson = response.body().data();

        var results = parsedJson.get(VALUES_KEY).get(0);
        assertThat(results.size()).isEqualTo(7);
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(0, "OffsetDateTime", "2015-06-24T12:50:35.556+01:00");
        QueryAssertions.assertThat(parsedJson)
                .hasTypedResultAt(1, "ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]");
        QueryAssertions.assertThat(parsedJson)
                .hasTypedResultAt(2, "ZonedDateTime", "2025-10-26T02:30:00+01:00[Europe/Stockholm]");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(3, "LocalDateTime", "2015-07-04T19:32:24");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(4, "Date", "2015-03-26");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(5, "Time", "12:50:35.556+01:00");
        QueryAssertions.assertThat(parsedJson).hasTypedResultAt(6, "LocalTime", "12:50:35.556");
    }

    @Test
    void duration() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN duration('P14DT16H12M') AS theDuration")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("theDuration")
                .hasTimers();

        QueryAssertions.assertThat(response.body().data()).hasTypedResultAt(0, "Duration", "P14DT16H12M");
    }

    @Test
    void binary() throws IOException, InterruptedException {
        try (var tx = dbms.database("neo4j").beginTx()) {
            tx.createNode(Label.label("FindMe")).setProperty("binaryGoodness", new byte[] {1, 2, 3, 4, 5});
            tx.commit();
        }

        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("MATCH (n:FindMe) return n").build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasTimers();

        var parsedJson = response.body().data();
        var results = parsedJson.get(VALUES_KEY);
        QueryAssertions.assertThat(
                        results.get(0).get(0).get(CYPHER_VALUE).get(_PROPERTIES).get("binaryGoodness"))
                .hasTypedResult("Base64", "AQIDBAU=");
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

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasTimers();
        var parsedJson = response.body().data();

        var results = parsedJson.get(VALUES_KEY).get(0);
        QueryAssertions.assertThat(results.get(0)).hasTypedResult("Point", "SRID=7203;POINT (2.3 4.5)");
        QueryAssertions.assertThat(results.get(1)).hasTypedResult("Point", "SRID=9157;POINT Z (2.3 4.5 6.7)");
        QueryAssertions.assertThat(results.get(2)).hasTypedResult("Point", "SRID=4326;POINT (2.3 4.5)");
        QueryAssertions.assertThat(results.get(3)).hasTypedResult("Point", "SRID=4326;POINT (2.3 4.5)");
        QueryAssertions.assertThat(results.get(4)).hasTypedResult("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)");
        QueryAssertions.assertThat(results.get(5)).hasTypedResult("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)");
        QueryAssertions.assertThat(results.get(6)).hasTypedResult("Point", "SRID=4326;POINT (56.7 12.78)");
        QueryAssertions.assertThat(results.get(7)).hasTypedResult("Point", "SRID=4979;POINT Z (56.7 12.78 8.0)");
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN {key: 'Value', listKey: [{inner1: 'Map1'}, {inner2: 'Map2'}]} AS map")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("map")
                .hasTimers();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);
        assertThat(parsedJson.get(FIELDS_KEY).get(0).asText()).isEqualTo("map");

        var map = parsedJson.get(VALUES_KEY).get(0).get(0);

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
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN [1,true,'hello',date('+2015-W13-4'), {amap: 'hello'}] as list")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("list")
                .hasTimers();

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(FIELDS_KEY).size()).isEqualTo(1);

        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("List");

        var resultArray = parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

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
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (n:MyLabel {aNumber: 1234}) RETURN n")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasTimers();

        var parsedJson = response.body().data();

        var node = parsedJson.get(VALUES_KEY).get(0).get(0);
        assertThat(node.get(CYPHER_TYPE).asText()).isEqualTo("Node");
        assertThat(node.get(CYPHER_VALUE).get(Fieldnames._ELEMENT_ID).asText()).isNotBlank();
        assertThat(node.get(CYPHER_VALUE).get(_LABELS).size()).isEqualTo(1);
        assertThat(node.get(CYPHER_VALUE).get(_LABELS).get(0).asText()).isEqualTo("MyLabel");
        QueryAssertions.assertThat(node.get(CYPHER_VALUE).get(_PROPERTIES).get("aNumber"))
                .hasTypedResult("Integer", "1234");
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (a)-[r:RELTYPE {onFire: 'owch!'}]->(b) RETURN r")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasTimers();

        var parsedJson = response.body().data();
        var rel = parsedJson.get(VALUES_KEY).get(0).get(0);
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
        var createPathReq = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:REL]->(b:LabelB)")
                .build());

        QueryResponseAssertions.assertThat(createPathReq).wasSuccessful();
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:REL]->(b:LabelB) RETURN p")
                .build());

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("Path");

        var path = parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

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
        var createPathReq = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)")
                .build());

        QueryResponseAssertions.assertThat(createPathReq)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasTimers();
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p")
                .build());

        var parsedJson = response.body().data();

        assertThat(parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_TYPE).asText())
                .isEqualTo("Path");

        var path = parsedJson.get(VALUES_KEY).get(0).get(0).get(CYPHER_VALUE);

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

    @Test
    void uuid() throws IOException, InterruptedException {

        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN UUID('ca3d9a43-09e3-4b66-9384-87ea25e27d01') AS theUUID")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasContentType(contentType())
                .wasSuccessful()
                .hasFieldNames("theUUID");

        QueryAssertions.assertThat(response.body().data())
                .hasTypedResultAt(0, "UUID", "ca3d9a43-09e3-4b66-9384-87ea25e27d01");
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldReturnQueryType(TransactionType transactionType, String statement, String expectedQueryType)
            throws IOException, InterruptedException {
        var response = testClient.executeQuery(
                transactionType, QueryRequest.newBuilder().statement(statement).build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasQueryType(expectedQueryType);
    }

    @ParameterizedTest
    @MethodSource("queryRequestElements")
    void shouldHandleRequestFieldsInAnyOrder(List<String> elements) throws IOException, InterruptedException {
        var response = testClient.sendRaw("{ %s }".formatted(String.join(",", elements)));

        QueryResponseAssertions.assertThat(response).wasSuccessful();
    }

    static Stream<Arguments> queryTypes() {
        return Stream.of(TransactionType.values())
                .flatMap(type -> Stream.of(
                        Arguments.of(type, "RETURN 1", "r"),
                        Arguments.of(type, "CREATE ()", "w"),
                        Arguments.of(type, "CREATE (p:Person{name: 'Vozinha'}) RETURN p", "rw"),
                        Arguments.of(
                                type,
                                "CREATE CONSTRAINT constraint_name_%d FOR (n:Label) REQUIRE n.property_%d IS UNIQUE"
                                        .formatted(type.ordinal(), type.ordinal()),
                                "s")));
    }

    static Stream<Arguments> queryRequestElements() {
        var includeCounters = """
                "includeCounters": true""";
        var parameters = """
                    "parameters": {
                      "value": {
                        "$type": "Integer",
                        "_value": "1"\s
                      }
                    }\
                """;
        var statement = """
                "statement": "RETURN $value AS one\"""";
        return Stream.of(
                        List.of(includeCounters, parameters, statement),
                        List.of(includeCounters, statement, parameters),
                        List.of(statement, includeCounters, parameters),
                        List.of(statement, parameters, includeCounters),
                        List.of(parameters, statement, includeCounters),
                        List.of(parameters, includeCounters, statement))
                .map(Arguments::of);
    }
}
