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
import static org.junit.jupiter.api.Assertions.assertNotNull;

import java.io.IOException;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.Arrays;
import java.util.stream.Stream;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.TransactionType;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;

@QueryAPITestExtension
class QueryResourceConfigJsonlIT {

    private final DatabaseManagementService dbms;
    private final HttpClient client;
    private final String queryEndpoint;

    QueryResourceConfigJsonlIT(DatabaseManagementService dbms, QueryAPITestClient client) {
        this.dbms = dbms;
        this.client = HttpClient.newBuilder().build();
        this.queryEndpoint = client.getEndpoint();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldUseWriteAccessModeByDefault(TransactionType transactionType) throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(transactionType.endpoint(queryEndpoint), "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"CREATE (n) RETURN n\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @ValueSource(strings = {"WRITE", "write", "Write"})
    void shouldUseWriteAccessModeExplicitly(String input) throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"CREATE (n) RETURN n\",\"accessMode\": \"" + input + "\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary()
                .hasNoRemainingEvents();

        // todo we can look at query plan
    }

    @ParameterizedTest
    @ValueSource(strings = {"READ", "read", "Read"})
    void shouldUseReadAccessModeExplicitly(String input) throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN 1\",\"accessMode\": \"" + input + "\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary()
                .hasNoRemainingEvents();

        // todo we can look at query plan
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldErrorIfWrongAccessModeUsed(TransactionType transactionType) throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(transactionType.endpoint(queryEndpoint), "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"CREATE (n) RETURN n\",\"accessMode\": \"READ\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(
                        Status.Statement.AccessMode,
                        "Writing in read access mode not allowed. Attempted write to neo4j")
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldErrorIfInvalidAccessModeGiven(TransactionType transactionType) throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(transactionType.endpoint(queryEndpoint), "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"CREATE (n) RETURN n\",\"accessMode\": \"bananas\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "Bad Request")
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldReturnQueryPlan(TransactionType transactionType) throws IOException, InterruptedException {
        var dbName = dbms.database("neo4j").databaseName();
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(transactionType.endpoint(queryEndpoint), "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"EXPLAIN RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                // Explain doesn't return data
                .receivesSummary(assertions -> assertions.hasQueryPlan(queryPlan -> {
                    assertThat(queryPlan.get("operatorType").asText()).isEqualTo("ProduceResults@" + dbName);
                    assertNotNull(queryPlan.get("arguments"));
                    assertThat(queryPlan.get("identifiers").size()).isEqualTo(1);
                    assertThat(queryPlan.get("identifiers").get(0).asText()).isEqualTo("`1`");
                    assertThat(queryPlan.get("children").size()).isEqualTo(1);
                    assertThat(queryPlan.get("identifiers").get(0).asText()).isEqualTo("`1`");
                    var childPlan = queryPlan.get("children").get(0);

                    assertThat(childPlan.get("operatorType").asText()).isEqualTo("Projection@" + dbName);
                    assertNotNull(childPlan.get("arguments"));
                    assertThat(childPlan.get("identifiers").size()).isEqualTo(1);
                }))
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("transactionTypes")
    void shouldReturnProfile(TransactionType transactionType) throws IOException, InterruptedException {
        var dbName = dbms.database("neo4j").databaseName(); // names changes in SPD
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(transactionType.endpoint(queryEndpoint), "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"PROFILE RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesRecord(1)
                .receivesSummary(assertions -> assertions.hasProfiledQueryPlan(profiledQueryPlan -> {
                    assertThat(profiledQueryPlan.get("dbHits").asInt()).isEqualTo(0);
                    assertThat(profiledQueryPlan.get("records").asInt()).isEqualTo(1);
                    assertThat(profiledQueryPlan.get("hasPageCacheStats").asBoolean())
                            .isEqualTo(false);
                    assertThat(profiledQueryPlan.get("pageCacheHits").asInt()).isEqualTo(0);
                    assertThat(profiledQueryPlan.get("pageCacheMisses").asInt()).isEqualTo(0);
                    assertThat(profiledQueryPlan.get("pageCacheHitRatio").asDouble())
                            .isEqualTo(0);

                    assertThat(profiledQueryPlan.get("operatorType").asText()).isEqualTo("ProduceResults@" + dbName);
                    assertNotNull(profiledQueryPlan.get("arguments"));
                    assertThat(profiledQueryPlan.get("identifiers").size()).isEqualTo(1);
                    assertThat(profiledQueryPlan.get("identifiers").get(0).asText())
                            .isEqualTo("`1`");
                    assertThat(profiledQueryPlan.get("time")).isNull();
                    assertNotNull(profiledQueryPlan);

                    var childProfile = profiledQueryPlan.get("children");

                    assertThat(childProfile.size()).isEqualTo(1);

                    assertThat(childProfile.get(0).asInt()).isEqualTo(0);
                    assertThat(childProfile.get(0).get("records").asInt()).isEqualTo(1);
                    assertThat(childProfile.get(0).get("hasPageCacheStats").asBoolean())
                            .isEqualTo(false);
                    assertThat(childProfile.get(0).get("pageCacheHits").asInt()).isEqualTo(0);
                    assertThat(childProfile.get(0).get("pageCacheMisses").asInt())
                            .isEqualTo(0);
                    assertThat(childProfile.get(0).get("pageCacheHitRatio").asDouble())
                            .isEqualTo(0);
                    assertThat(childProfile.get(0).get("time")).isNull();
                    assertThat(childProfile.get(0).get("operatorType").asText()).isEqualTo("Projection@" + dbName);
                    assertNotNull(childProfile.get(0).get("arguments"));
                    assertThat(childProfile.get(0).get("identifiers").size()).isEqualTo(1);
                    assertThat(childProfile.get(0).get("identifiers").get(0).asText())
                            .isEqualTo("`1`");
                }))
                .hasNoRemainingEvents();
    }

    public static Stream<Arguments> transactionTypes() {
        return Arrays.stream(TransactionType.values()).map(Arguments::of);
    }
}
