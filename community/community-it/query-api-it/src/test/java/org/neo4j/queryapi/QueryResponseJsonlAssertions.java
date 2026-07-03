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

import static org.assertj.core.api.Assertions.within;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.assertj.core.api.AbstractAssert;
import org.assertj.core.api.Assertions;
import org.assertj.core.api.ObjectArrayAssert;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.testclient.QueryContentType;

public class QueryResponseJsonlAssertions
        extends AbstractAssert<QueryResponseJsonlAssertions, HttpResponse<Stream<String>>> {
    private final ObjectMapper mapper;
    private final Iterator<String> lines;

    protected QueryResponseJsonlAssertions(HttpResponse<Stream<String>> streamHttpResponse) {
        super(streamHttpResponse, QueryResponseJsonlAssertions.class);
        mapper = new ObjectMapper();
        this.lines = this.actual.body().iterator();
    }

    public static QueryResponseJsonlAssertions assertThat(HttpResponse<Stream<String>> queryResponse) {
        return new QueryResponseJsonlAssertions(queryResponse);
    }

    public QueryResponseJsonlAssertions isTransferEncodingChunked() {
        var transferEncoding = actual.headers().firstValue("Transfer-Encoding").orElse(null);
        Assertions.assertThat(transferEncoding).isEqualTo("chunked");
        return this;
    }

    public QueryResponseJsonlAssertions hasContentType(QueryContentType queryContentType) {
        return hasContentType(queryContentType.mimeType());
    }

    public QueryResponseJsonlAssertions hasContentType(String queryContentType) {
        var contentType = this.actual.headers().firstValue("Content-Type").orElse("");
        Assertions.assertThat(contentType).isEqualTo(queryContentType);
        return this;
    }

    public QueryResponseJsonlAssertions wasSuccessful() {
        Assertions.assertThat(this.actual.statusCode())
                .as("Expected successful response but was a status was %s.", this.actual.statusCode())
                .isEqualTo(202);

        return this;
    }

    public QueryResponseJsonlAssertions hasStatus(int statusCode) {
        Assertions.assertThat(this.actual.statusCode())
                .as("Expected response status but was a status was %s.", statusCode, this.actual.statusCode())
                .isEqualTo(statusCode);

        return this;
    }

    public QueryResponseJsonlAssertions receivesHeader(String... fields) throws IOException {
        return receivesHeader(assertions -> assertions.hasFields(fields));
    }

    public QueryResponseJsonlAssertions receivesHeader(Consumer<HeaderAssertions> headerAssertions) throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Header.name());
            var data = coerceValue("Header body", event.body(), HeaderEventBody.class);

            headerAssertions.accept(HeaderAssertions.assertThat(data));
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesNHeaders(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            var maybeEvent = nextEvent();

            Assertions.assertThat(maybeEvent)
                    .as("Expected %d headers events but got %d", count, i)
                    .isNotEmpty()
                    .hasValueSatisfying(event -> {
                        Assertions.assertThat(event.type()).isEqualTo(EventType.Header.name());
                        coerceValue("Header body", event.body(), HeaderEventBody.class);
                    });
        }
        return this;
    }

    public QueryResponseJsonlAssertions receivesRecord(Object... values) throws IOException {
        return receivesRecord(recordAssertions -> recordAssertions.isEqualTo(values));
    }

    public QueryResponseJsonlAssertions receivesRecord(Consumer<ObjectArrayAssert<Object>> recordAssertions)
            throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Record.name());
            var data = coerceValue("Record body", event.body(), Object[].class);
            recordAssertions.accept(Assertions.assertThat(data));
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesNRecords(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            var maybeEvent = nextEvent();

            Assertions.assertThat(maybeEvent)
                    .as("Expected %d records events but got %d", count, i)
                    .isNotEmpty()
                    .hasValueSatisfying(event -> {
                        Assertions.assertThat(event.type()).isEqualTo(EventType.Record.name());
                        coerceValue("Record body", event.body(), Object[].class);
                    });
        }

        return this;
    }

    @SafeVarargs
    public final QueryResponseJsonlAssertions receivesTypedRecord(Consumer<CypherValueAssertions>... assertions)
            throws IOException {
        var maybeEvent = nextEvent();
        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Record.name());
            var values = coerceValue("Record body", event.body(), CypherValue[].class);
            Assertions.assertThat(values).hasSize(assertions.length);
            for (var i = 0; i < assertions.length; i++) {
                assertions[i].accept(CypherValueAssertions.assertThat(values[i]));
            }
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesNTypedRecords(int count) throws IOException {
        for (int i = 0; i < count; i++) {
            var maybeEvent = nextEvent();

            Assertions.assertThat(maybeEvent)
                    .as("Expected %d records events but got %d", count, i)
                    .isNotEmpty()
                    .hasValueSatisfying(event -> {
                        Assertions.assertThat(event.type()).isEqualTo(EventType.Record.name());
                        coerceValue("Record body", event.body(), CypherValue[].class);
                    });
        }

        return this;
    }

    public QueryResponseJsonlAssertions receivesSummary() throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Summary.name());
            coerceValue("Summary body", event.body(), SummaryEventBody.class);
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesSummary(Consumer<SummaryAssertions> summaryAssertions)
            throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Summary.name());
            var summaryEventData = coerceValue("Summary body", event.body(), SummaryEventBody.class);
            summaryAssertions.accept(SummaryAssertions.assertThat(summaryEventData));
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesError(Status... statuses) throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Error.name());
            var errors = coerceValue("Error body", event.body(), ErrorEventBody[].class);
            Assertions.assertThat(errors).hasSize(statuses.length);
            for (int i = 0; i < statuses.length; i++) {
                var status = statuses[i];
                var error = errors[i];
                Assertions.assertThat(error.code())
                        .describedAs("_data[%d].code", i)
                        .isEqualTo(status.code().serialize());

                Assertions.assertThat(error.message())
                        .describedAs("_data[%d].message", i)
                        .isNotBlank();
            }
        });

        return this;
    }

    public QueryResponseJsonlAssertions receivesError(Status status, String message) throws IOException {
        var maybeEvent = nextEvent();

        Assertions.assertThat(maybeEvent).isNotEmpty().hasValueSatisfying(event -> {
            Assertions.assertThat(event.type()).isEqualTo(EventType.Error.name());
            var errors = coerceValue("Error body", event.body(), ErrorEventBody[].class);
            Assertions.assertThat(errors).hasSize(1);

            var error = errors[0];
            Assertions.assertThat(error.code())
                    .describedAs("_data[%d].code", 0)
                    .isEqualTo(status.code().serialize());

            Assertions.assertThat(error.message())
                    .describedAs("_data[%d].message", 0)
                    .isEqualTo(message);
        });

        return this;
    }

    public void hasNoRemainingEvents() throws IOException {
        Assertions.assertThat(this.lines.hasNext())
                .as("Should have not remaining events, but it has.")
                .isFalse();
    }

    public static class HeaderAssertions extends AbstractAssert<HeaderAssertions, HeaderEventBody> {
        private HeaderAssertions(HeaderEventBody actual) {
            super(actual, HeaderAssertions.class);
        }

        private static HeaderAssertions assertThat(HeaderEventBody headerEventBody) {
            return new HeaderAssertions(headerEventBody);
        }

        public HeaderAssertions hasFields(String... fields) {
            Assertions.assertThat(actual().fields).isEqualTo(List.of(fields));
            return this;
        }

        public HeaderAssertions doesNotHaveFields() {
            Assertions.assertThat(actual().fields).isNull();
            return this;
        }
    }

    public static class CypherValueAssertions extends AbstractAssert<CypherValueAssertions, CypherValue> {
        private CypherValueAssertions(CypherValue actual) {
            super(actual, CypherValueAssertions.class);
        }

        private static CypherValueAssertions assertThat(CypherValue cypherValue) {
            return new CypherValueAssertions(cypherValue);
        }

        public CypherValueAssertions hasType(String type) {
            Assertions.assertThat(actual.type).as("should have type").isEqualTo(type);
            return this;
        }

        public CypherValueAssertions hasValue(Object value) {
            Assertions.assertThat(actual.value).as("should have value").isEqualTo(value);
            return this;
        }

        public CypherValueAssertions valueSatisfies(Consumer<Object> requirements) {
            Assertions.assertThat(actual.value).as("should value satisfies").satisfies(requirements);
            return this;
        }

        public static Consumer<CypherValueAssertions> hasTypeAndValue(String type, Object value) {
            return assertions -> assertions.hasType(type).hasValue(value);
        }

        public static Consumer<CypherValueAssertions> hasTypeAndValueSatisfies(
                String type, Consumer<Object> requirements) {
            return assertions -> assertions.hasType(type).valueSatisfies(requirements);
        }

        public static Map<String, Object> typeAndValue(String type, Object value) {
            return Map.of("$type", type, "_value", value);
        }
    }

    public static class SummaryAssertions extends AbstractAssert<SummaryAssertions, SummaryEventBody> {
        private SummaryAssertions(SummaryEventBody summaryEventBody) {
            super(summaryEventBody, SummaryAssertions.class);
        }

        private static SummaryAssertions assertThat(SummaryEventBody summaryEventBody) {
            return new SummaryAssertions(summaryEventBody);
        }

        public SummaryAssertions hasBookmarks() {
            Assertions.assertThat(this.actual.bookmarks())
                    .as("Should have bookmarks")
                    .isNotEmpty();

            return this;
        }

        public SummaryAssertions hasBookmarks(Consumer<List<String>> informBookmarks) {
            Assertions.assertThat(this.actual.bookmarks())
                    .as("Should have bookmarks")
                    .isNotEmpty();
            informBookmarks.accept(this.actual.bookmarks());
            return this;
        }

        public SummaryAssertions hasBookmarksEqualTo(List<String> bookmarks) {
            Assertions.assertThat(this.actual.bookmarks())
                    .as("Should have bookmarks")
                    .isEqualTo(bookmarks);

            return this;
        }

        public SummaryAssertions hasQueryTypeEqualTo(String queryType) {
            Assertions.assertThat(this.actual.queryType())
                    .as("Should have query type")
                    .isEqualTo(queryType);

            return this;
        }

        public SummaryAssertions hasTimers() {
            Assertions.assertThat(this.actual.resultAvailableAfter())
                    .as("Should have result available after greater or equal to 0")
                    .isGreaterThanOrEqualTo(0);

            Assertions.assertThat(this.actual.resultConsumedAfter())
                    .as("Should have result consumed after greater or equal to resultAvailableAfter")
                    .isGreaterThanOrEqualTo(this.actual.resultAvailableAfter());

            return this;
        }

        public SummaryAssertions doesNotHaveTimers() {
            Assertions.assertThat(this.actual.resultAvailableAfter())
                    .as("Should not have result available after")
                    .isNull();

            Assertions.assertThat(this.actual.resultConsumedAfter())
                    .as("Should not have result consumed after")
                    .isNull();

            return this;
        }

        public SummaryAssertions hasBookmarksNotEqualTo(List<String> bookmarks) {
            Assertions.assertThat(this.actual.bookmarks())
                    .as("Should have bookmarks")
                    .isNotEqualTo(bookmarks);

            return this;
        }

        public SummaryAssertions hasBookmarksDoesNotContainAnyElementsOf(List<String> bookmarks) {
            Assertions.assertThat(this.actual.bookmarks())
                    .as("Should have bookmarks")
                    .isNotEmpty()
                    .doesNotContainAnyElementsOf(bookmarks);
            return this;
        }

        public SummaryAssertions hasTransaction(Consumer<String> transactionIdConsumer) {
            hasTransaction();
            transactionIdConsumer.accept(actual().transaction.id);
            return this;
        }

        public SummaryAssertions hasTransaction(String transactionId) {
            hasTransaction();
            Assertions.assertThat(this.actual.transaction.id).isEqualTo(transactionId);
            return this;
        }

        public SummaryAssertions hasTransaction() {
            Assertions.assertThat(this.actual.transaction())
                    .as("Should have transaction")
                    .isNotNull()
                    .extracting("id", "expires")
                    .isNotNull();
            return this;
        }

        public SummaryAssertions hasUpdatedTimeout() {
            Assertions.assertThat(this.actual.transaction())
                    .as("Should have transaction")
                    .isNotNull()
                    .extracting("expires")
                    .isNotNull();

            Assertions.assertThat(Instant.parse(this.actual.transaction.expires))
                    .isCloseTo(Instant.now().plus(Duration.ofSeconds(5)), within(3, ChronoUnit.SECONDS));
            return this;
        }

        public SummaryAssertions doesNotHaveTransaction() {
            Assertions.assertThat(actual().transaction()).isNull();
            return this;
        }

        public SummaryAssertions hasNotifications(NotificationCodeWithDescription... notifications) {
            Assertions.assertThat(this.actual.notifications)
                    .as("Should have notifications")
                    .hasSize(notifications.length)
                    .satisfies(actualNotifications -> {
                        for (int i = 0; i < notifications.length; i++) {
                            var expectedNotification = notifications[i];
                            var actualNotification = actualNotifications.get(i);
                            Assertions.assertThat(actualNotification.get("code").asText())
                                    .describedAs("code", i)
                                    .isEqualTo(expectedNotification
                                            .getStatus()
                                            .code()
                                            .serialize());
                        }
                    });
            return this;
        }

        public SummaryAssertions hasNotifications(Consumer<List<JsonNode>> notificationsAssertions) {
            notificationsAssertions.accept(this.actual.notifications());
            return this;
        }

        public SummaryAssertions doesNotHaveNotifications() {
            Assertions.assertThat(this.actual.notifications())
                    .as("Should not have notifications")
                    .isNull();
            return this;
        }

        public SummaryAssertions hasCounters(Consumer<JsonNode> countersAssertions) {
            Assertions.assertThat(this.actual.counters())
                    .as("Should have counters")
                    .isNotEmpty();
            countersAssertions.accept(this.actual.counters());
            return this;
        }

        public SummaryAssertions doesNotHaveCounters() {
            Assertions.assertThat(this.actual.counters())
                    .as("Should have counters")
                    .isNull();
            return this;
        }

        public SummaryAssertions hasQueryPlan() {
            Assertions.assertThat(this.actual.queryPlan())
                    .as("Should have query plan")
                    .isNotEmpty();

            return this;
        }

        public SummaryAssertions hasQueryPlan(Consumer<JsonNode> queryPlanAssertions) {
            Assertions.assertThat(this.actual.queryPlan())
                    .as("Should have query plan")
                    .isNotEmpty();

            queryPlanAssertions.accept(this.actual.queryPlan());
            return this;
        }

        public SummaryAssertions doesNotHaveQueryPlan() {
            Assertions.assertThat(this.actual.queryPlan())
                    .as("Should not have query plan")
                    .isNull();

            return this;
        }

        public SummaryAssertions hasProfiledQueryPlan() {
            Assertions.assertThat(this.actual.profiledQueryPlan())
                    .as("Should have profiled query plan")
                    .isNotEmpty();
            return this;
        }

        public SummaryAssertions hasProfiledQueryPlan(Consumer<JsonNode> profiledQueryPlanAssertions) {
            Assertions.assertThat(this.actual.profiledQueryPlan())
                    .as("Should have profiled query plan")
                    .isNotEmpty();
            profiledQueryPlanAssertions.accept(this.actual.profiledQueryPlan());
            return this;
        }

        public SummaryAssertions doesNotHaveProfiledQueryPlan() {
            Assertions.assertThat(this.actual.profiledQueryPlan())
                    .as("Should not have profiled query plan")
                    .isNull();
            return this;
        }
    }

    private Optional<Event> nextEvent() throws JsonProcessingException {
        if (this.lines.hasNext()) {
            var event = this.lines.next();
            return Optional.of(mapper.readValue(event, Event.class));
        }
        return Optional.empty();
    }

    private <T> T coerceValue(String context, JsonNode jsonNode, Class<T> clazz) {
        try {
            return mapper.convertValue(jsonNode, clazz);
        } catch (IllegalArgumentException e) {
            Assertions.fail("Could not coerce value of " + context + " to type " + clazz, e);
        }
        throw new IllegalStateException("This path should not have been reached");
    }

    private record HeaderEventBody(List<String> fields) {}

    private record SummaryEventBodyTransaction(String id, String expires) {}

    private record SummaryEventBody(
            List<JsonNode> notifications,
            JsonNode counters,
            JsonNode profiledQueryPlan,
            JsonNode queryPlan,
            List<String> bookmarks,
            SummaryEventBodyTransaction transaction,
            Long resultAvailableAfter,
            Long resultConsumedAfter,
            String queryType) {}

    private record ErrorEventBody(String code, String message) {}

    private record Event(String type, JsonNode body) {
        public Event(@JsonProperty("$event") String type, @JsonProperty("_body") JsonNode body) {
            this.type = type;
            this.body = body;
        }
    }

    private record CypherValue(String type, Object value) {
        public CypherValue(@JsonProperty("$type") String type, @JsonProperty("_value") Object value) {
            this.type = type;
            this.value = value;
        }
    }

    private enum EventType {
        Header,
        Record,
        Summary,
        Error,
    }
}
