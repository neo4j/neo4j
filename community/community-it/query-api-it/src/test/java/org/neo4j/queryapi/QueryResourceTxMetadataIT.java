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

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension
class QueryResourceTxMetadataIT {

    private final QueryAPITestClient testClient;
    private final String queryEndpoint;

    QueryResourceTxMetadataIT(QueryAPITestClient testClient) {
        this.testClient = testClient;
        this.queryEndpoint = testClient.getEndpoint();
    }

    @ParameterizedTest
    @MethodSource("paramTypesWithoutTypeInfo")
    void shouldHandleTxMetadata(Object value) throws IOException, InterruptedException {
        Map<String, Object> metadata = Map.of("string", "metadata string field", "otherValue", value);

        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CALL tx.getMetaData()")
                .txMetadata(metadata)
                .withoutCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords(List.of(List.of(metadata)));
        ;
    }

    @Test
    void shouldHandleEmptyMeta() throws IOException, InterruptedException {
        Map<String, Object> metadata = Map.of();

        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CALL tx.getMetaData()")
                .txMetadata(metadata)
                .withoutCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords(List.of(List.of(metadata)));
    }

    @ParameterizedTest
    @MethodSource("paramTypesWithoutTypeInfo")
    void shouldHandleTxMetadataInTx(Object value) throws IOException, InterruptedException {
        Map<String, Object> metadata = Map.of("string", "metadata string field", "otherValue", value);

        var response = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CALL tx.getMetaData()")
                .txMetadata(metadata)
                .withoutCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords(List.of(List.of(metadata)));
    }

    @Test
    void shouldHandleEmptyMetaInTx() throws IOException, InterruptedException {
        Map<String, Object> metadata = Map.of();

        var response = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CALL tx.getMetaData()")
                .txMetadata(metadata)
                .withoutCounters()
                .build());

        QueryResponseAssertions.assertThat(response).wasSuccessful().hasRecords(List.of(List.of(metadata)));
    }

    @ParameterizedTest
    @MethodSource("paramTypesPerContentType")
    void shouldHandleTxMetadataTyped(QueryContentType contentType, CypherValue value)
            throws IOException, InterruptedException {
        var typedTestClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(contentType));

        Map<String, CypherValue> metadata =
                Map.of("string", new CypherValue("String", "metadata string field"), "otherValue", value);

        var response = typedTestClient.sendRaw(typedStatementWithMetadata(metadata));

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasRecords(List.of(List.of(cypherValueOfMap(metadata))));
        ;
    }

    @ParameterizedTest
    @MethodSource("invalidParamTypesPerContentType")
    void shouldFailOnTxMetadataInvalidTyped(QueryContentType contentType, CypherValue value)
            throws IOException, InterruptedException {
        var typedTestClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(contentType));

        Map<String, CypherValue> metadata =
                Map.of("string", new CypherValue("String", "metadata string field"), "otherValue", value);

        var response = typedTestClient.sendRaw(typedStatementWithMetadata(metadata));

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
        ;
    }

    @ParameterizedTest
    @MethodSource("paramTypesPerContentType")
    void shouldHandleTxMetadataInTxTyped(QueryContentType contentType, CypherValue value)
            throws IOException, InterruptedException {
        var typedTestClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(contentType));

        Map<String, CypherValue> metadata =
                Map.of("string", new CypherValue("String", "metadata string field"), "otherValue", value);

        var response = typedTestClient.sendRawBeginTx(typedStatementWithMetadata(metadata));

        QueryResponseAssertions.assertThat(response)
                .wasSuccessful()
                .hasRecords(List.of(List.of(cypherValueOfMap(metadata))));
        ;
    }

    @ParameterizedTest
    @MethodSource("invalidParamTypesPerContentType")
    void shouldFailOnTxMetadataInvalidInTxTyped(QueryContentType contentType, CypherValue value)
            throws IOException, InterruptedException {
        var typedTestClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(contentType));

        Map<String, CypherValue> metadata =
                Map.of("string", new CypherValue("String", "metadata string field"), "otherValue", value);

        var response = typedTestClient.sendRawBeginTx(typedStatementWithMetadata(metadata));

        QueryResponseAssertions.assertThat(response).hasErrorStatus(400, Status.Request.Invalid);
        ;
    }

    private static String typedStatementWithMetadata(Map<String, CypherValue> metadata) {
        return "{ \"statement\": \"CALL tx.getMetaData()\", \"parameters\": {},  \"txMetadata\": { %s }}"
                .formatted(metadata.entrySet().stream()
                        .map(entry -> String.format(
                                "\"%s\": %s", entry.getKey(), entry.getValue().stringify()))
                        .collect(Collectors.joining(",")));
    }

    private static Map<String, Object> cypherValueOfMap(Map<String, CypherValue> map) {
        return Map.of(
                "$type",
                "Map",
                "_value",
                map.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey, value -> value.getValue().asMap())));
    }

    public static Stream<Arguments> paramTypesWithoutTypeInfo() {
        return Stream.of(1L, List.of(1.3D, 3L), Map.of("value", 23d)).map(Arguments::of);
    }

    public static Stream<Arguments> paramTypesPerContentType() {
        return Stream.of(QueryContentType.values())
                .filter(QueryContentType::typed)
                .filter(Predicate.not(QueryContentType::events))
                .flatMap(contentType -> Stream.of(
                                new CypherValue("Integer", "123"),
                                new CypherValue("Integer", String.valueOf(Integer.MAX_VALUE)),
                                new CypherValue("Integer", String.valueOf(Long.MAX_VALUE)),
                                new CypherValue("Float", "12.3"),
                                new CypherValue("Float", "NaN"),
                                new CypherValue("Float", "Infinity"),
                                new CypherValue("Float", "-Infinity"),
                                new CypherValue("Base64", "YmFuYW5hcw=="),
                                new CypherValue("OffsetDateTime", "2015-06-24T12:50:35.556+01:00"),
                                new CypherValue("ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]"),
                                new CypherValue("LocalDateTime", "2015-07-04T19:32:24"),
                                new CypherValue("Date", "2015-03-26"),
                                new CypherValue("Time", "12:50:35.556+01:00"),
                                new CypherValue("LocalTime", "12:50:35.556"),
                                new CypherValue("Duration", "P14DT16H12M"),
                                new CypherValue("Point", "SRID=7203;POINT (2.3 4.5)"),
                                new CypherValue("Point", "SRID=9157;POINT Z (2.3 4.5 6.7)"),
                                new CypherValue("Point", "SRID=4326;POINT (2.3 4.5)"),
                                new CypherValue("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)"),
                                new CypherValue("List", List.of(new CypherValue("String", "A value on a list"))),
                                new CypherValue(
                                        "Map",
                                        Map.of("theKey", new CypherValue("String", "the string value in the map"))))
                        .map(cypherValue -> Arguments.of(contentType, cypherValue)));
    }

    public static Stream<Arguments> invalidParamTypesPerContentType() {
        return Stream.of(QueryContentType.values())
                .filter(QueryContentType::typed)
                .filter(Predicate.not(QueryContentType::events))
                .flatMap(contentType -> Stream.of(
                                new CypherValue("Integer", "Some made up int which doesnt exists"),
                                new CypherValue("MadeUpType", "random"),
                                // Graph types can't be ingested
                                new CypherValue(
                                        "Node",
                                        Map.of(
                                                "_element_id",
                                                "4:ff04df25-ff2b-4b55-98f8-6888297b025e:2",
                                                "_labels",
                                                List.of("Person"),
                                                "_properties",
                                                Map.of("name", new CypherValue("String", "Phill")))))
                        .map(cypherValue -> Arguments.of(contentType, cypherValue)));
    }
    /**
     * Represents a Cypher value
     * <p></p>
     * This record lives on class, but we might want to make it part of the test framework and we might need
     * a better approach using the strategy pattern
     * @param type
     * @param value
     */
    private record CypherValue(String type, Object value) {
        public String stringify() {
            if (value instanceof String) {
                return "{ \"$type\": \"%s\", \"_value\": \"%s\"}".formatted(type, value);
            } else if (value instanceof List<?> value) {
                return "{ \"$type\": \"%s\", \"_value\": %s}"
                        .formatted(
                                type,
                                "["
                                        + value.stream()
                                                .filter(e -> e instanceof CypherValue)
                                                .map(e -> (CypherValue) e)
                                                .map(CypherValue::stringify)
                                                .collect(Collectors.joining(","))
                                        + "]");
            } else if (value instanceof Map<?, ?> value) {
                return "{ \"$type\": \"%s\", \"_value\": %s}"
                        .formatted(
                                type,
                                "{"
                                        + value.entrySet().stream()
                                                .map(v -> "\"%s\": %s"
                                                        .formatted(
                                                                v.getKey(),
                                                                v.getValue() instanceof CypherValue
                                                                        ? ((CypherValue) v.getValue()).stringify()
                                                                        : v))
                                                .collect(Collectors.joining(","))
                                        + "}");
            }

            throw new AssertionError("_value should be of type String, List or Map");
        }

        public Map<String, Object> asMap() {
            var outputValue = this.value;
            if (value instanceof List<?> listValue) {
                outputValue = listValue.stream()
                        .filter(e -> e instanceof CypherValue)
                        .map(e -> (CypherValue) e)
                        .map(CypherValue::asMap)
                        .collect(Collectors.toList());
            } else if (value instanceof Map<?, ?> mapValue) {
                outputValue = mapValue.entrySet().stream()
                        .collect(Collectors.toMap(
                                Map.Entry::getKey,
                                e -> e.getValue() instanceof CypherValue
                                        ? ((CypherValue) e.getValue()).asMap()
                                        : e.getValue()));
            }
            return Map.of("$type", type, "_value", outputValue);
        }
    }
}
