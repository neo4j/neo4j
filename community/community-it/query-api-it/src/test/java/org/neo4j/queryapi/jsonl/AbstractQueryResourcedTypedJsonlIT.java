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
package org.neo4j.queryapi.jsonl;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions.hasTypeAndValue;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions.hasTypeAndValueSatisfies;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions.typeAndValue;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.response.format.Fieldnames;

abstract class AbstractQueryResourcedTypedJsonlIT {
    private final DatabaseManagementService dbms;
    private final QueryAPITestClient testClient;

    AbstractQueryResourcedTypedJsonlIT(DatabaseManagementService dbms, QueryAPITestClient testClient) {
        this.dbms = dbms;
        this.testClient = testClient;
    }

    protected abstract QueryContentType expectedContentType();

    @Test
    void basicTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN true as bool, 1 as number, 1.23 as float, 'hello' as string")
                .build());

        assertResponseWithValues(
                response,
                fields("bool", "number", "float", "string"),
                recordValues(
                        hasTypeAndValue("Boolean", true),
                        hasTypeAndValue("Integer", "1"),
                        hasTypeAndValue("Float", "1.23"),
                        hasTypeAndValue("String", "hello")));
    }

    @Test
    void nullType() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("RETURN null as aNull").build());

        assertResponseWithValues(response, fields("aNull"), recordValues(hasTypeAndValue("Null", null)));
    }

    @Test
    void temporalTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN datetime('2015-06-24T12:50:35.556+0100') AS theOffsetDateTime, "
                        + "datetime('2015-11-21T21:40:32.142[Antarctica/Troll]') AS theZonedDateTime,"
                        + "datetime('2025-10-26T02:30:00+01:00[Europe/Stockholm]') AS theZonedDateTimeOnDSTSwitch,"
                        + "localdatetime('2015185T19:32:24') AS theLocalDateTime,"
                        + "date('+2015-W13-4') AS theDate,"
                        + "time('125035.556+0100') AS theTime,"
                        + "localtime('12:50:35.556') AS theLocalTime")
                .build());

        assertResponseWithValues(
                response,
                fields(
                        "theOffsetDateTime",
                        "theZonedDateTime",
                        "theZonedDateTimeOnDSTSwitch",
                        "theLocalDateTime",
                        "theDate",
                        "theTime",
                        "theLocalTime"),
                recordValues(
                        hasTypeAndValue("OffsetDateTime", "2015-06-24T12:50:35.556+01:00"),
                        hasTypeAndValue("ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]"),
                        hasTypeAndValue("ZonedDateTime", "2025-10-26T02:30:00+01:00[Europe/Stockholm]"),
                        hasTypeAndValue("LocalDateTime", "2015-07-04T19:32:24"),
                        hasTypeAndValue("Date", "2015-03-26"),
                        hasTypeAndValue("Time", "12:50:35.556+01:00"),
                        hasTypeAndValue("LocalTime", "12:50:35.556")));
    }

    @Test
    void duration() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN duration('P14DT16H12M') AS theDuration")
                .build());

        assertResponseWithValues(
                response, fields("theDuration"), recordValues(hasTypeAndValue("Duration", "P14DT16H12M")));
    }

    @Test
    void binary() throws IOException, InterruptedException {
        try (var tx = dbms.database("neo4j").beginTx()) {
            tx.createNode(Label.label("FindMe")).setProperty("binaryGoodness", new byte[] {1, 2, 3, 4, 5});
            tx.commit();
        }

        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("MATCH (n:FindMe) return n.binaryGoodness as binaryGoodness")
                .build());

        assertResponseWithValues(
                response, fields("binaryGoodness"), recordValues(hasTypeAndValue("Base64", "AQIDBAU=")));
    }

    @Test
    void point() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN point({x: 2.3, y: 4.5}) as xy, point({x: 2.3, y: 4.5, z: 6.7}) as xyz,"
                        + "point({x:2.3, y:4.5, srid:4326}) as xys,"
                        + "point({x: 2.3, y: 4.5, crs: 'WGS-84'}) as xyc,"
                        + "point({x:2.3, y:4.5, z:6.7, srid:4979}) as xyzs,"
                        + "point({x: 2.3, y: 4.5, z: 6.7, crs: 'WGS-84-3D'}) as xyzc,"
                        + "point({longitude: 56.7, latitude: 12.78}) as ll,"
                        + "point({longitude: 56.7, latitude: 12.78, height: 8}) as llh")
                .build());

        assertResponseWithValues(
                response,
                fields("xy", "xyz", "xys", "xyc", "xyzs", "xyzc", "ll", "llh"),
                recordValues(
                        hasTypeAndValue("Point", "SRID=7203;POINT (2.3 4.5)"),
                        hasTypeAndValue("Point", "SRID=9157;POINT Z (2.3 4.5 6.7)"),
                        hasTypeAndValue("Point", "SRID=4326;POINT (2.3 4.5)"),
                        hasTypeAndValue("Point", "SRID=4326;POINT (2.3 4.5)"),
                        hasTypeAndValue("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)"),
                        hasTypeAndValue("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)"),
                        hasTypeAndValue("Point", "SRID=4326;POINT (56.7 12.78)"),
                        hasTypeAndValue("Point", "SRID=4979;POINT Z (56.7 12.78 8.0)")));
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN {key: 'Value', listKey: [{inner1: 'Map1'}, {inner2: 'Map2'}]} AS map")
                .build());

        assertResponseWithValues(
                response,
                fields("map"),
                recordValues(hasTypeAndValue(
                        "Map",
                        Map.of(
                                "key", typeAndValue("String", "Value"),
                                "listKey",
                                        typeAndValue(
                                                "List",
                                                List.of(
                                                        typeAndValue(
                                                                "Map",
                                                                Map.of("inner1", typeAndValue("String", "Map1"))),
                                                        typeAndValue(
                                                                "Map",
                                                                Map.of("inner2", typeAndValue("String", "Map2")))))))));
    }

    @Test
    void list() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN [1,true,'hello',date('+2015-W13-4'), {amap: 'hello'}] as list")
                .build());

        assertResponseWithValues(
                response,
                fields("list"),
                recordValues(hasTypeAndValue(
                        "List",
                        List.of(
                                typeAndValue("Integer", "1"),
                                typeAndValue("Boolean", true),
                                typeAndValue("String", "hello"),
                                typeAndValue("Date", "2015-03-26"),
                                typeAndValue("Map", Map.of("amap", typeAndValue("String", "hello")))))));
    }

    @Test
    void node() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n:MyLabel {aNumber: 1234}) RETURN n")
                .build());

        assertResponseWithValues(response, fields("n"), recordValues(hasTypeAndValueSatisfies("Node", node -> {
            var nodeAssertions = assertThat(node).asInstanceOf(MAP);
            nodeAssertions.containsOnlyKeys(Fieldnames._ELEMENT_ID, Fieldnames._LABELS, Fieldnames._PROPERTIES);
            nodeAssertions.extracting(Fieldnames._ELEMENT_ID).isNotNull().isNotEqualTo("");
            nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("MyLabel"));
            nodeAssertions
                    .extracting(Fieldnames._PROPERTIES)
                    .isEqualTo(Map.of("aNumber", typeAndValue("Integer", "1234")));
        })));
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (a)-[r:RELTYPE {onFire: 'owch!'}]->(b) RETURN r")
                .build());

        assertResponseWithValues(response, fields("r"), recordValues(hasTypeAndValueSatisfies("Relationship", rel -> {
            var relAssertions = assertThat(rel).asInstanceOf(MAP);
            relAssertions.containsOnlyKeys(
                    Fieldnames._ELEMENT_ID,
                    Fieldnames._START_NODE_ELEMENT_ID,
                    Fieldnames._END_NODE_ELEMENT_ID,
                    Fieldnames._RELATIONSHIP_TYPE,
                    Fieldnames._PROPERTIES);
            relAssertions.extracting(Fieldnames._ELEMENT_ID).isNotNull().isNotEqualTo("");
            relAssertions
                    .extracting(Fieldnames._START_NODE_ELEMENT_ID)
                    .isNotNull()
                    .isNotEqualTo("");
            relAssertions
                    .extracting(Fieldnames._END_NODE_ELEMENT_ID)
                    .isNotNull()
                    .isNotEqualTo("");
            relAssertions.extracting(Fieldnames._RELATIONSHIP_TYPE).isEqualTo("RELTYPE");
            relAssertions
                    .extracting(Fieldnames._PROPERTIES)
                    .isEqualTo(Map.of("onFire", typeAndValue("String", "owch!")));
        })));
    }

    @Test
    void simplePath() throws IOException, InterruptedException {
        var createPathReq = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:REL]->(b:LabelB)")
                .build());

        QueryResponseJsonlAssertions.assertThat(createPathReq).wasSuccessful();
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:REL]->(b:LabelB) RETURN p")
                .build());

        assertResponseWithValues(response, fields("p"), recordValues(hasTypeAndValueSatisfies("Path", path -> {
            var elementIdCapture = new Capture<String>();
            var pathAssertions = assertThat(path).asInstanceOf(LIST);
            pathAssertions.hasSize(3);
            // Node 1
            pathAssertions.element(0).satisfies(cypherTypeNode -> {
                var cypherTypeNodeAssertions = assertThat(cypherTypeNode).asInstanceOf(MAP);
                cypherTypeNodeAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeNodeAssertions.extracting("$type").isEqualTo("Node");

                var nodeAssertions =
                        cypherTypeNodeAssertions.extracting("_value").asInstanceOf(MAP);
                nodeAssertions
                        .extracting(Fieldnames._ELEMENT_ID)
                        .isNotNull()
                        .isNotEqualTo("")
                        .asString()
                        .satisfies(elementIdCapture.capture());
                nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("LabelA"));
                nodeAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Node 2
            pathAssertions.element(2).satisfies(cypherTypeNode -> {
                var cypherTypeNodeAssertions = assertThat(cypherTypeNode).asInstanceOf(MAP);
                cypherTypeNodeAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeNodeAssertions.extracting("$type").isEqualTo("Node");

                var nodeAssertions =
                        cypherTypeNodeAssertions.extracting("_value").asInstanceOf(MAP);
                nodeAssertions
                        .extracting(Fieldnames._ELEMENT_ID)
                        .isNotNull()
                        .isNotEqualTo("")
                        .asString()
                        .satisfies(elementIdCapture.capture());
                nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("LabelB"));
                nodeAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Rel between them
            pathAssertions.element(1).satisfies(cypherTypeRel -> {
                var cypherTypeRelAssertions = assertThat(cypherTypeRel).asInstanceOf(MAP);
                cypherTypeRelAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeRelAssertions.extracting("$type").isEqualTo("Relationship");

                var relAssertions = cypherTypeRelAssertions.extracting("_value").asInstanceOf(MAP);
                relAssertions.containsOnlyKeys(
                        Fieldnames._ELEMENT_ID,
                        Fieldnames._START_NODE_ELEMENT_ID,
                        Fieldnames._END_NODE_ELEMENT_ID,
                        Fieldnames._RELATIONSHIP_TYPE,
                        Fieldnames._PROPERTIES);
                relAssertions.extracting(Fieldnames._ELEMENT_ID).isNotNull().isNotEqualTo("");
                // ensure relationship points the correct way.
                relAssertions
                        .extracting(Fieldnames._START_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().getFirst());
                relAssertions
                        .extracting(Fieldnames._END_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().getLast());
                relAssertions.extracting(Fieldnames._RELATIONSHIP_TYPE).isEqualTo("REL");
                relAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });
        })));
    }

    @Test
    void path() throws IOException, InterruptedException {
        var createPathReq = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)")
                .build());

        QueryResponseJsonlAssertions.assertThat(createPathReq)
                .hasContentType(expectedContentType())
                .wasSuccessful();
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p")
                .build());

        assertResponseWithValues(response, fields("p"), recordValues(hasTypeAndValueSatisfies("Path", path -> {
            var elementIdCapture = new Capture<String>();
            var pathAssertions = assertThat(path).asInstanceOf(LIST);
            pathAssertions.hasSize(5);
            // Node 1
            pathAssertions.element(0).satisfies(cypherTypeNode -> {
                var cypherTypeNodeAssertions = assertThat(cypherTypeNode).asInstanceOf(MAP);
                cypherTypeNodeAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeNodeAssertions.extracting("$type").isEqualTo("Node");

                var nodeAssertions =
                        cypherTypeNodeAssertions.extracting("_value").asInstanceOf(MAP);
                nodeAssertions
                        .extracting(Fieldnames._ELEMENT_ID)
                        .isNotNull()
                        .isNotEqualTo("")
                        .asString()
                        .satisfies(elementIdCapture.capture());
                nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("LabelA"));
                nodeAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Node 2
            pathAssertions.element(2).satisfies(cypherTypeNode -> {
                var cypherTypeNodeAssertions = assertThat(cypherTypeNode).asInstanceOf(MAP);
                cypherTypeNodeAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeNodeAssertions.extracting("$type").isEqualTo("Node");

                var nodeAssertions =
                        cypherTypeNodeAssertions.extracting("_value").asInstanceOf(MAP);
                nodeAssertions
                        .extracting(Fieldnames._ELEMENT_ID)
                        .isNotNull()
                        .isNotEqualTo("")
                        .asString()
                        .satisfies(elementIdCapture.capture());
                nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("LabelB"));
                nodeAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Node 3
            pathAssertions.element(4).satisfies(cypherTypeNode -> {
                var cypherTypeNodeAssertions = assertThat(cypherTypeNode).asInstanceOf(MAP);
                cypherTypeNodeAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeNodeAssertions.extracting("$type").isEqualTo("Node");

                var nodeAssertions =
                        cypherTypeNodeAssertions.extracting("_value").asInstanceOf(MAP);
                nodeAssertions
                        .extracting(Fieldnames._ELEMENT_ID)
                        .isNotNull()
                        .isNotEqualTo("")
                        .asString()
                        .satisfies(elementIdCapture.capture());
                nodeAssertions.extracting(Fieldnames._LABELS).isEqualTo(List.of("LabelC"));
                nodeAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Rel between them node 1  and node 2
            pathAssertions.element(1).satisfies(cypherTypeRel -> {
                var cypherTypeRelAssertions = assertThat(cypherTypeRel).asInstanceOf(MAP);
                cypherTypeRelAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeRelAssertions.extracting("$type").isEqualTo("Relationship");

                var relAssertions = cypherTypeRelAssertions.extracting("_value").asInstanceOf(MAP);
                relAssertions.containsOnlyKeys(
                        Fieldnames._ELEMENT_ID,
                        Fieldnames._START_NODE_ELEMENT_ID,
                        Fieldnames._END_NODE_ELEMENT_ID,
                        Fieldnames._RELATIONSHIP_TYPE,
                        Fieldnames._PROPERTIES);
                relAssertions.extracting(Fieldnames._ELEMENT_ID).isNotNull().isNotEqualTo("");
                // ensure relationship points the correct way.
                relAssertions
                        .extracting(Fieldnames._START_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().getFirst());
                relAssertions
                        .extracting(Fieldnames._END_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().get(1));
                relAssertions.extracting(Fieldnames._RELATIONSHIP_TYPE).isEqualTo("RELAB");
                relAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });

            // Rel between them node 3  and node 2
            pathAssertions.element(3).satisfies(cypherTypeRel -> {
                var cypherTypeRelAssertions = assertThat(cypherTypeRel).asInstanceOf(MAP);
                cypherTypeRelAssertions.containsOnlyKeys("$type", "_value");
                cypherTypeRelAssertions.extracting("$type").isEqualTo("Relationship");

                var relAssertions = cypherTypeRelAssertions.extracting("_value").asInstanceOf(MAP);
                relAssertions.containsOnlyKeys(
                        Fieldnames._ELEMENT_ID,
                        Fieldnames._START_NODE_ELEMENT_ID,
                        Fieldnames._END_NODE_ELEMENT_ID,
                        Fieldnames._RELATIONSHIP_TYPE,
                        Fieldnames._PROPERTIES);
                relAssertions.extracting(Fieldnames._ELEMENT_ID).isNotNull().isNotEqualTo("");
                // ensure relationship points the correct way.
                relAssertions
                        .extracting(Fieldnames._START_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().getLast());
                relAssertions
                        .extracting(Fieldnames._END_NODE_ELEMENT_ID)
                        .isEqualTo(elementIdCapture.getCaptured().get(1));
                relAssertions.extracting(Fieldnames._RELATIONSHIP_TYPE).isEqualTo("RELCB");
                relAssertions.extracting(Fieldnames._PROPERTIES).isEqualTo(Map.of());
            });
        })));
    }

    private void assertResponseWithValues(
            HttpResponse<Stream<String>> response, String[] fields, Consumer<CypherValueAssertions>[] valuesAssertions)
            throws IOException {
        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(expectedContentType())
                .wasSuccessful()
                .receivesHeader(fields)
                .receivesTypedRecord(valuesAssertions)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    private static String[] fields(String... fields) {
        return fields;
    }

    @SafeVarargs
    private static Consumer<CypherValueAssertions>[] recordValues(Consumer<CypherValueAssertions>... assertions) {
        return assertions;
    }
}
