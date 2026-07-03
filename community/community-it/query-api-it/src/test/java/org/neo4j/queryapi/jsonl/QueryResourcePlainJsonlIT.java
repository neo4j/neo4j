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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.InstanceOfAssertFactories;
import org.assertj.core.data.Index;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.Label;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.response.format.Fieldnames;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L},
        enabledFeatureFlagForUUID = true)
class QueryResourcePlainJsonlIT {

    private final DatabaseManagementService dbms;
    private final QueryAPITestClient testClient;

    QueryResourcePlainJsonlIT(DatabaseManagementService dbms, QueryAPITestClient testClient) {
        this.dbms = dbms;
        this.testClient = testClient;
    }

    @Test
    void basicTypes() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN true as bool, 1 as number, 1.23 as float, 'hello' as string")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("bool", "number", "float", "string")
                .receivesRecord(true, 1, 1.23, "hello")
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void nullType() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("RETURN null as aNull").build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("aNull")
                .receivesRecord(new Object[] {null})
                .receivesSummary()
                .hasNoRemainingEvents();
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

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(
                        "theOffsetDateTime",
                        "theZonedDateTime",
                        "theZonedDateTimeOnDSTSwitch",
                        "theLocalDateTime",
                        "theDate",
                        "theTime",
                        "theLocalTime")
                .receivesRecord(
                        "2015-06-24T12:50:35.556+01:00",
                        "2015-11-21T21:40:32.142Z",
                        "2025-10-26T02:30:00+01:00",
                        "2015-07-04T19:32:24",
                        "2015-03-26",
                        "12:50:35.556+01:00",
                        "12:50:35.556");
    }

    @Test
    void point() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN point({x: 2.3, y: 4.5}), point({x: 2.3, y: 4.5, z: 6.7}),"
                        + "point({x:2.3, y:4.5, srid:4326}),"
                        + "point({x: 2.3, y: 4.5, crs: 'WGS-84'}),"
                        + "point({x:2.3, y:4.5, z:6.7, srid:4979}),"
                        + "point({x: 2.3, y: 4.5, z: 6.7, crs: 'WGS-84-3D'}),"
                        + "point({longitude: 56.7, latitude: 12.78}),"
                        + "point({longitude: 56.7, latitude: 12.78, height: 8})")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesRecord(
                        "SRID=7203;POINT (2.3 4.5)",
                        "SRID=9157;POINT Z (2.3 4.5 6.7)",
                        "SRID=4326;POINT (2.3 4.5)",
                        "SRID=4326;POINT (2.3 4.5)",
                        "SRID=4979;POINT Z (2.3 4.5 6.7)",
                        "SRID=4979;POINT Z (2.3 4.5 6.7)",
                        "SRID=4326;POINT (56.7 12.78)",
                        "SRID=4979;POINT Z (56.7 12.78 8.0)")
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void duration() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN duration('P14DT16H12M') AS theDuration")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("theDuration")
                .receivesRecord("P14DT16H12M")
                .receivesSummary()
                .hasNoRemainingEvents();
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

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("binaryGoodness")
                .receivesRecord("AQIDBAU=")
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void map() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN {key: 'Value', listKey: [{inner: 'Map1'}, {inner: 'Map2'}]} AS map")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("map")
                .receivesRecord(
                        Map.of("key", "Value", "listKey", List.of(Map.of("inner", "Map1"), Map.of("inner", "Map2"))))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void list() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN [1,true,'hello',date('+2015-W13-4')] as list")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("list")
                .receivesRecord(List.of(1, true, "hello", "2015-03-26"))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void node() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n:MyLabel {aNumber: 1234}) RETURN n")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("n")
                .receivesRecord(assertions -> assertions
                        .hasSize(1)
                        .singleElement()
                        .isInstanceOf(Map.class)
                        .extracting("elementId", "labels", Fieldnames.PROPERTIES)
                        .satisfies(
                                elementId -> Assertions.assertThat(elementId)
                                        .asString()
                                        .isNotEmpty(),
                                Index.atIndex(0))
                        .satisfies(
                                labels -> Assertions.assertThat(labels).isEqualTo(List.of("MyLabel")), Index.atIndex(1))
                        .satisfies(
                                properties -> Assertions.assertThat(properties).isEqualTo(Map.of("aNumber", 1234)),
                                Index.atIndex(2)))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void relationship() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (a)-[r:RELTYPE {onFire: true}]->(b) RETURN r")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("r")
                .receivesRecord(assertions -> assertions
                        .hasSize(1)
                        .singleElement()
                        .isInstanceOf(Map.class)
                        .extracting(
                                "elementId", "startNodeElementId", "startNodeElementId", "type", Fieldnames.PROPERTIES)
                        .satisfies(
                                elementId -> Assertions.assertThat(elementId)
                                        .asString()
                                        .isNotEmpty(),
                                Index.atIndex(0))
                        .satisfies(
                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                        .asString()
                                        .isNotEmpty(),
                                Index.atIndex(1))
                        .satisfies(
                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                        .asString()
                                        .isNotEmpty(),
                                Index.atIndex(2))
                        .satisfies(type -> Assertions.assertThat(type).isEqualTo("RELTYPE"), Index.atIndex(3))
                        .satisfies(
                                properties -> Assertions.assertThat(properties).isEqualTo(Map.of("onFire", true)),
                                Index.atIndex(4)))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void path() throws IOException, InterruptedException {
        var createPathReq = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC)")
                .build());

        QueryResponseJsonlAssertions.assertThat(createPathReq)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(0)
                .receivesSummary()
                .hasNoRemainingEvents();

        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("MATCH p=(a:LabelA)-[rel1:RELAB]->(b:LabelB)<-[rel2:RELCB]-(c:LabelC) RETURN p")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("p")
                .receivesRecord(assertions -> assertions
                        .hasSize(1)
                        .singleElement()
                        .asInstanceOf(InstanceOfAssertFactories.LIST)
                        .satisfies(
                                a -> Assertions.assertThat(a)
                                        .isInstanceOf(Map.class)
                                        .extracting("elementId", "labels", Fieldnames.PROPERTIES)
                                        .satisfies(
                                                elementId -> Assertions.assertThat(elementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(0))
                                        .satisfies(
                                                labels -> Assertions.assertThat(labels)
                                                        .isEqualTo(List.of("LabelA")),
                                                Index.atIndex(1))
                                        .satisfies(
                                                properties -> Assertions.assertThat(properties)
                                                        .isEqualTo(Map.of()),
                                                Index.atIndex(2)),
                                Index.atIndex(0))
                        .satisfies(
                                rel1 -> Assertions.assertThat(rel1)
                                        .isInstanceOf(Map.class)
                                        .extracting(
                                                "elementId",
                                                "startNodeElementId",
                                                "startNodeElementId",
                                                "type",
                                                Fieldnames.PROPERTIES)
                                        .satisfies(
                                                elementId -> Assertions.assertThat(elementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(0))
                                        .satisfies(
                                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(1))
                                        .satisfies(
                                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(2))
                                        .satisfies(
                                                type -> Assertions.assertThat(type)
                                                        .isEqualTo("RELAB"),
                                                Index.atIndex(3))
                                        .satisfies(
                                                properties -> Assertions.assertThat(properties)
                                                        .isEqualTo(Map.of()),
                                                Index.atIndex(4)),
                                Index.atIndex(1))
                        .satisfies(
                                b -> Assertions.assertThat(b)
                                        .isInstanceOf(Map.class)
                                        .extracting("elementId", "labels", Fieldnames.PROPERTIES)
                                        .satisfies(
                                                elementId -> Assertions.assertThat(elementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(0))
                                        .satisfies(
                                                labels -> Assertions.assertThat(labels)
                                                        .isEqualTo(List.of("LabelB")),
                                                Index.atIndex(1))
                                        .satisfies(
                                                properties -> Assertions.assertThat(properties)
                                                        .isEqualTo(Map.of()),
                                                Index.atIndex(2)),
                                Index.atIndex(2))
                        .satisfies(
                                rel2 -> Assertions.assertThat(rel2)
                                        .isInstanceOf(Map.class)
                                        .extracting(
                                                "elementId",
                                                "startNodeElementId",
                                                "startNodeElementId",
                                                "type",
                                                Fieldnames.PROPERTIES)
                                        .satisfies(
                                                elementId -> Assertions.assertThat(elementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(0))
                                        .satisfies(
                                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(1))
                                        .satisfies(
                                                startNodeElementId -> Assertions.assertThat(startNodeElementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(2))
                                        .satisfies(
                                                type -> Assertions.assertThat(type)
                                                        .isEqualTo("RELCB"),
                                                Index.atIndex(3))
                                        .satisfies(
                                                properties -> Assertions.assertThat(properties)
                                                        .isEqualTo(Map.of()),
                                                Index.atIndex(4)),
                                Index.atIndex(3))
                        .satisfies(
                                c -> Assertions.assertThat(c)
                                        .isInstanceOf(Map.class)
                                        .extracting("elementId", "labels", Fieldnames.PROPERTIES)
                                        .satisfies(
                                                elementId -> Assertions.assertThat(elementId)
                                                        .asString()
                                                        .isNotEmpty(),
                                                Index.atIndex(0))
                                        .satisfies(
                                                labels -> Assertions.assertThat(labels)
                                                        .isEqualTo(List.of("LabelC")),
                                                Index.atIndex(1))
                                        .satisfies(
                                                properties -> Assertions.assertThat(properties)
                                                        .isEqualTo(Map.of()),
                                                Index.atIndex(2)),
                                Index.atIndex(4)))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void uuid() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN UUID('ca3d9a43-09e3-4b66-9384-87ea25e27d01') AS theUUID")
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("theUUID")
                .receivesRecord("ca3d9a43-09e3-4b66-9384-87ea25e27d01")
                .receivesSummary()
                .hasNoRemainingEvents();
    }
}
