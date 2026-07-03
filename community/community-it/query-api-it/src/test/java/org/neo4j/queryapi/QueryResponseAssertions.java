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

import static org.assertj.core.api.Assertions.fail;
import static org.assertj.core.api.Assertions.within;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_CODE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_MESSAGE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.TX_EXPIRY_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import com.fasterxml.jackson.databind.JsonNode;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryResponse;

public final class QueryResponseAssertions
        extends AbstractAssert<QueryResponseAssertions, HttpResponse<QueryResponse>> {

    private final HttpResponse<QueryResponse> queryResponse;

    private QueryResponseAssertions(HttpResponse<QueryResponse> queryResponse) {
        super(queryResponse, QueryResponseAssertions.class);
        this.queryResponse = queryResponse;
    }

    public static QueryResponseAssertions assertThat(HttpResponse<QueryResponse> queryResponse) {
        return new QueryResponseAssertions(queryResponse);
    }

    public QueryResponseAssertions hasContentType(QueryContentType queryContentType) {
        var contentType = queryResponse.headers().firstValue("Content-Type").orElse("");
        Assertions.assertThat(contentType).isEqualTo(queryContentType.mimeType());
        return this;
    }

    public QueryResponseAssertions wasSuccessful() {
        Assertions.assertThat(queryResponse.statusCode())
                .as(
                        "Expected successful response but was a %s with body: %s",
                        queryResponse.statusCode(), queryResponse.body())
                .isEqualTo(202);

        Assertions.assertThat(queryResponse.body().errors()).isNull();
        return this;
    }

    public QueryResponseAssertions wasNotFound() {
        Assertions.assertThat(queryResponse.statusCode()).isEqualTo(404);
        Assertions.assertThat(queryResponse.body().errors().size()).isEqualTo(1);
        Assertions.assertThat(
                        queryResponse.body().errors().get(0).get(ERROR_CODE).asText())
                .isEqualTo(Status.Request.Invalid.code().serialize());
        Assertions.assertThat(
                        queryResponse.body().errors().get(0).get(ERROR_MESSAGE).asText())
                .isNotBlank();
        return this;
    }

    public QueryResponseAssertions wasDatabaseNotFound() {
        Assertions.assertThat(queryResponse.statusCode()).isEqualTo(404);
        Assertions.assertThat(queryResponse.body().errors().size()).isEqualTo(1);
        Assertions.assertThat(
                        queryResponse.body().errors().get(0).get(ERROR_CODE).asText())
                .isEqualTo(Status.Database.DatabaseNotFound.code().serialize());
        Assertions.assertThat(
                        queryResponse.body().errors().get(0).get(ERROR_MESSAGE).asText())
                .isNotBlank();
        return this;
    }

    public QueryResponseAssertions hasErrorStatus(int httpCode, Status status) {
        return hasErrorStatus(httpCode, status, (Consumer<String>) null);
    }

    public QueryResponseAssertions hasErrorStatus(int httpCode, Status status, String message) {
        return hasErrorStatus(httpCode, status, actual -> Objects.equals(actual, message));
    }

    public QueryResponseAssertions hasErrorStatus(int httpCode, Status status, Consumer<String> messageRequirements) {
        Assertions.assertThat(queryResponse.statusCode()).isEqualTo(httpCode);
        Assertions.assertThat(queryResponse.body().errors().size()).isEqualTo(1);
        Assertions.assertThat(
                        queryResponse.body().errors().get(0).get(ERROR_CODE).asText())
                .isEqualTo(status.code().serialize());
        if (messageRequirements == null) {
            Assertions.assertThat(queryResponse
                            .body()
                            .errors()
                            .get(0)
                            .get(ERROR_MESSAGE)
                            .asText())
                    .isNotBlank();
        } else {
            Assertions.assertThat(queryResponse
                            .body()
                            .errors()
                            .get(0)
                            .get(ERROR_MESSAGE)
                            .asText())
                    .satisfies(messageRequirements);
        }

        return this;
    }

    public QueryResponseAssertions hasFieldNames(String... expectedFieldNames) {
        var responseFieldNamesIt = queryResponse.body().data().get("fields");

        for (int i = 0; i < expectedFieldNames.length; i++) {
            Assertions.assertThat(expectedFieldNames[i])
                    .isEqualTo(responseFieldNamesIt.get(i).asText());
        }

        return this;
    }

    public QueryResponseAssertions hasRecord() {
        hasRecord(1);
        return this;
    }

    public QueryResponseAssertions hasRecords(List<List<Object>> expectedRecords) {
        var responseRecords = queryResponse.body().data().get("values");
        for (int i = 0; i < expectedRecords.size(); i++) {
            var expectedRecord = expectedRecords.get(i);
            var responseRecord = responseRecords.get(i);

            for (int j = 0; j < expectedRecord.size(); j++) {
                Object unwrapped = unwrapValue(responseRecord.get(j), expectedRecord.get(j));

                Assertions.assertThat(unwrapped).isEqualTo(expectedRecord.get(j));
            }
        }
        return this;
    }

    public QueryResponseAssertions hasRecords(Object... expectedValues) {
        var listOfLists = new ArrayList<List<Object>>();

        for (Object expectedRecord : expectedValues) {
            listOfLists.add(List.of(expectedRecord));
        }

        hasRecords(listOfLists);
        return this;
    }

    public QueryResponseAssertions hasRecord(int expectedRecordValue) {
        Assertions.assertThat(queryResponse.body().data().get(VALUES_KEY).size())
                .isEqualTo(1);
        Assertions.assertThat(queryResponse
                        .body()
                        .data()
                        .get(VALUES_KEY)
                        .get(0)
                        .get(0)
                        .asInt())
                .isEqualTo(expectedRecordValue);
        return this;
    }

    private void hasNoRecords() {
        Assertions.assertThat(queryResponse.body().data().get(VALUES_KEY).size())
                .isEqualTo(0);
    }

    public QueryResponseAssertions hasTypedRecord() {
        Assertions.assertThat(queryResponse.body().data().get(VALUES_KEY).size())
                .isEqualTo(1);
        var value = queryResponse.body().data().get(VALUES_KEY).get(0);
        Assertions.assertThat(value.get(0).get(CYPHER_VALUE).asText()).isEqualTo("1");
        Assertions.assertThat(value.get(0).get(CYPHER_TYPE).asText()).isEqualTo("Integer");
        return this;
    }

    public QueryResponseAssertions hasTransaction() {
        Assertions.assertThat(queryResponse.body().transaction()).isNotEmpty();
        return this;
    }

    public QueryResponseAssertions hasNoTransaction() {
        Assertions.assertThat(queryResponse.body().transaction()).isNull();
        return this;
    }

    public QueryResponseAssertions hasBookmark() {
        Assertions.assertThat(queryResponse.body().bookmarks()).hasSize(1);
        return this;
    }

    public QueryResponseAssertions hasBookmark(String bookmark) {
        Assertions.assertThat(queryResponse.body().bookmarks().size()).isEqualTo(1);
        Assertions.assertThat(queryResponse.body().bookmarks().getFirst()).isEqualTo(bookmark);
        return this;
    }

    public QueryResponseAssertions hasUpdatedTimeout() {
        Assertions.assertThat(Instant.parse(
                        queryResponse.body().transaction().get(TX_EXPIRY_KEY).asText()))
                .isCloseTo(Instant.now().plus(Duration.ofSeconds(5)), within(3, ChronoUnit.SECONDS));
        return this;
    }

    public QueryResponseAssertions hasNotifications(NotificationCodeWithDescription... notifications) {
        var respNotifications = queryResponse.body().notifications();

        Assertions.assertThat(respNotifications.size()).isEqualTo(notifications.length);

        for (int i = 0; i < notifications.length; i++) {
            var reqNotifications = queryResponse.body().notifications().get(i);
            Assertions.assertThat(reqNotifications.get("code").asText())
                    .isEqualTo(notifications[i].getStatus().code().serialize());
            Assertions.assertThat(reqNotifications.get("title").asText())
                    .isEqualTo(notifications[i].getStatus().code().description());
            Assertions.assertThat(reqNotifications.get("description").asText()).isNotBlank();
            Assertions.assertThat(reqNotifications.get("position").get("offset").asInt())
                    .isNotNegative();
            Assertions.assertThat(reqNotifications.get("position").get("line").asInt())
                    .isPositive();
            Assertions.assertThat(reqNotifications.get("position").get("column").asInt())
                    .isPositive();
            Assertions.assertThat(reqNotifications.get("severity").asText()).isNotBlank();
            Assertions.assertThat(reqNotifications.get("category").asText()).isNotBlank();
        }

        return this;
    }

    public QueryResponseAssertions hasNoNotifications() {
        Assertions.assertThat(queryResponse.body().notifications()).isNull();
        return this;
    }

    public QueryResponseAssertions hasQueryStatistics() {
        var queryStatsMap = queryResponse.body().counters();

        Assertions.assertThat(queryStatsMap.size()).isEqualTo(14);
        Assertions.assertThat(queryStatsMap.get("containsUpdates").asBoolean()).isEqualTo(false);
        Assertions.assertThat(queryStatsMap.get("containsSystemUpdates").asBoolean())
                .isEqualTo(false);
        Assertions.assertThat(queryStatsMap.get("nodesCreated").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("nodesDeleted").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("propertiesSet").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("relationshipsCreated").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("relationshipsDeleted").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("labelsAdded").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("labelsRemoved").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("indexesAdded").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("indexesRemoved").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("constraintsAdded").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("constraintsRemoved").asInt()).isEqualTo(0);
        Assertions.assertThat(queryStatsMap.get("systemUpdates").asInt()).isEqualTo(0);

        return this;
    }

    public QueryResponseAssertions hasNoQueryStatistics() {
        Assertions.assertThat(queryResponse.body().counters()).isNull();
        return this;
    }

    public QueryResponseAssertions hasQueryPlan() {
        var queryPlan = queryResponse.body().queryPlan();

        Assertions.assertThat(queryPlan.get("operatorType").asText()).startsWith("ProduceResults@neo4j");
        assertNotNull(queryPlan.get("arguments"));
        Assertions.assertThat(queryPlan.get("identifiers").size()).isEqualTo(1);
        Assertions.assertThat(queryPlan.get("identifiers").get(0).asText()).isEqualTo("`1`");
        Assertions.assertThat(queryPlan.get("children").size()).isEqualTo(1);

        var childPlan = queryPlan.get("children").get(0);

        Assertions.assertThat(childPlan.get("operatorType").asText()).startsWith("Projection@neo4j");
        assertNotNull(childPlan.get("arguments"));
        Assertions.assertThat(childPlan.get("identifiers").size()).isEqualTo(1);
        Assertions.assertThat(queryPlan.get("identifiers").get(0).asText()).isEqualTo("`1`");

        // EXPLAIN does not return values
        assertThat(queryResponse).hasNoRecords();
        return this;
    }

    public QueryResponseAssertions hasNoQueryPlan() {
        Assertions.assertThat(queryResponse.body().queryPlan()).isNull();
        return this;
    }

    public void hasProfiledQueryPlan() {
        var profiledQueryPlan = queryResponse.body().profiledQueryPlan();

        Assertions.assertThat(profiledQueryPlan.get("dbHits").asInt()).isEqualTo(0);
        Assertions.assertThat(profiledQueryPlan.get("records").asInt()).isEqualTo(1);
        Assertions.assertThat(profiledQueryPlan.get("hasPageCacheStats").asBoolean())
                .isEqualTo(false);
        Assertions.assertThat(profiledQueryPlan.get("pageCacheHits").asInt()).isEqualTo(0);
        Assertions.assertThat(profiledQueryPlan.get("pageCacheMisses").asInt()).isEqualTo(0);
        Assertions.assertThat(profiledQueryPlan.get("pageCacheHitRatio").asDouble())
                .isEqualTo(0);
        Assertions.assertThat(profiledQueryPlan.get("operatorType").asText()).startsWith("ProduceResults@neo4j");
        assertNotNull(profiledQueryPlan.get("arguments"));
        Assertions.assertThat(profiledQueryPlan.get("identifiers").size()).isEqualTo(1);
        Assertions.assertThat(profiledQueryPlan.get("identifiers").get(0).asText())
                .isEqualTo("`1`");
        Assertions.assertThat(profiledQueryPlan.get("time")).isNull();

        var childProfile = profiledQueryPlan.get("children");

        Assertions.assertThat(childProfile.size()).isEqualTo(1);

        Assertions.assertThat(childProfile.get(0).asInt()).isEqualTo(0);
        Assertions.assertThat(childProfile.get(0).get("records").asInt()).isEqualTo(1);
        Assertions.assertThat(childProfile.get(0).get("hasPageCacheStats").asBoolean())
                .isEqualTo(false);
        Assertions.assertThat(childProfile.get(0).get("pageCacheHits").asInt()).isEqualTo(0);
        Assertions.assertThat(childProfile.get(0).get("pageCacheMisses").asInt())
                .isEqualTo(0);
        Assertions.assertThat(childProfile.get(0).get("pageCacheHitRatio").asDouble())
                .isEqualTo(0);
        Assertions.assertThat(childProfile.get(0).get("time")).isNull();
        Assertions.assertThat(childProfile.get(0).get("operatorType").asText()).startsWith("Projection@neo4j");
        assertNotNull(childProfile.get(0).get("arguments"));
        Assertions.assertThat(childProfile.get(0).get("identifiers").size()).isEqualTo(1);
        Assertions.assertThat(childProfile.get(0).get("identifiers").get(0).asText())
                .isEqualTo("`1`");

        assertThat(queryResponse).hasRecord();
    }

    public void hasNoProfiledQueryPlan() {
        Assertions.assertThat(queryResponse.body().profiledQueryPlan()).isNull();
    }

    public void hasQueryType(String queryType) {
        Assertions.assertThat(queryResponse.body().queryType()).isEqualTo(queryType);
    }

    public QueryResponseAssertions hasTimers() {
        Assertions.assertThat(queryResponse.body().resultAvailableAfter()).isGreaterThanOrEqualTo(0);
        Assertions.assertThat(queryResponse.body().resultConsumedAfter())
                .isGreaterThanOrEqualTo(queryResponse.body().resultAvailableAfter());
        return this;
    }

    public QueryResponseAssertions hasNoTimers() {
        Assertions.assertThat(queryResponse.body().resultAvailableAfter()).isNull();
        Assertions.assertThat(queryResponse.body().resultConsumedAfter()).isNull();
        return this;
    }

    private Object unwrapValue(JsonNode responseRecord, Object expectedRecord) {
        Object unwrapped = null;
        switch (responseRecord.getNodeType()) {
            case NUMBER -> unwrapped = unwrapNumber(responseRecord, expectedRecord);
            case STRING -> unwrapped = responseRecord.asText();
            case BOOLEAN -> unwrapped = responseRecord.asBoolean();
            case OBJECT -> unwrapped = unwrapObject(responseRecord);
            case ARRAY -> unwrapped = unwrapArray(responseRecord);
            case NULL -> unwrapped = null;
            default -> fail();
        }
        return unwrapped;
    }

    private Object unwrapNumber(JsonNode responseValue, Object expectedValue) {
        if (Objects.isNull(expectedValue)) {
            return unwrapNumber(responseValue);
        }

        if (expectedValue instanceof Integer) {
            return responseValue.asInt();
        } else if (expectedValue instanceof Double) {
            return responseValue.asDouble();
        } else if (expectedValue instanceof Long) {
            return responseValue.asLong();
        } else if (expectedValue instanceof Float) {
            return (float) responseValue.asDouble();
        }
        return null;
    }

    private Object unwrapNumber(JsonNode value) {
        if (value.isIntegralNumber()) {
            return value.asLong();
        } else if (value.isFloatingPointNumber()) {
            return value.asDouble();
        }
        return null;
    }

    private Map<String, Object> unwrapObject(JsonNode responseRecord) {
        return responseRecord
                .propertyStream()
                .collect(Collectors.toMap(Map.Entry::getKey, entry -> unwrapValue(entry.getValue(), null)));
    }

    private List<Object> unwrapArray(JsonNode responseRecord) {
        return responseRecord.valueStream().map(v -> unwrapValue(v, null)).collect(Collectors.toList());
    }
}
