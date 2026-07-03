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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.InstanceOfAssertFactories.LIST;
import static org.assertj.core.api.InstanceOfAssertFactories.MAP;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions.hasTypeAndValue;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions.hasTypeAndValueSatisfies;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_TYPE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.CYPHER_VALUE;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.QueryResponseJsonlAssertions.CypherValueAssertions;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;

abstract class AbstractQueryResourceTypedJsonlParametersIT {

    protected final QueryAPITestClient testClient;

    AbstractQueryResourceTypedJsonlParametersIT(QueryAPITestClient testClient) {
        this.testClient = testClient;
    }

    protected abstract QueryContentType expectedContentType();

    public static Stream<Arguments> paramTypes() {
        return Stream.of(
                Arguments.of("Integer", "123"),
                Arguments.of("Integer", String.valueOf(Integer.MAX_VALUE)),
                Arguments.of("Integer", String.valueOf(Long.MAX_VALUE)),
                Arguments.of("Float", "12.3"),
                Arguments.of("Base64", "YmFuYW5hcw=="),
                Arguments.of("OffsetDateTime", "2015-06-24T12:50:35.556+01:00"),
                Arguments.of("ZonedDateTime", "2015-11-21T21:40:32.142Z[Antarctica/Troll]"),
                Arguments.of("LocalDateTime", "2015-07-04T19:32:24"),
                Arguments.of("Date", "2015-03-26"),
                Arguments.of("Time", "12:50:35.556+01:00"),
                Arguments.of("LocalTime", "12:50:35.556"),
                Arguments.of("Duration", "P14DT16H12M"),
                Arguments.of("Point", "SRID=7203;POINT (2.3 4.5)"),
                Arguments.of("Point", "SRID=9157;POINT Z (2.3 4.5 6.7)"),
                Arguments.of("Point", "SRID=4326;POINT (2.3 4.5)"),
                Arguments.of("Point", "SRID=4979;POINT Z (2.3 4.5 6.7)"));
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl(format(
                "{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"%s\",\"_value\": \"%s\"}}}}}",
                typeString, value));

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue(typeString, value)));
    }

    @Test
    void shouldHandleBooleanParameter() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Boolean\",\"_value\": true}}}}}");

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue("Boolean", true)));
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"String\",\"_value\": \"Hello\"}}}}}");

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue("String", "Hello")));
    }

    @Test
    void shouldHandleNullParameter() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Null\",\"_value\": null}}}}}");

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue("Null", null)));
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":" + "{\"mappy\": {\"$type\":\""
                + typeString + "\", \"_value\": \"" + value.toString() + "\"} }}}}");

        assertResponseWithValues(
                response,
                fields("$parameter"),
                recordValues(hasTypeAndValueSatisfies(
                        "Map",
                        object -> assertThat(object)
                                .asInstanceOf(MAP)
                                .containsOnlyKeys("mappy")
                                .extracting("mappy")
                                .asInstanceOf(MAP)
                                .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                                .extracting(CYPHER_TYPE, CYPHER_VALUE)
                                .isEqualTo(List.of(typeString, value.toString())))));
    }

    @Test
    void shouldHandleMapWithBoolean() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"Map\",\"_value\":"
                + "{\"true\": {\"$type\":\"Boolean\", \"_value\": true},"
                + "\"false\": {\"$type\":\"Boolean\", \"_value\": false} }}}}");

        assertResponseWithValues(
                response, fields("$parameter"), recordValues(hasTypeAndValueSatisfies("Map", object -> {
                    var mapAssertion = assertThat(object).asInstanceOf(MAP).containsOnlyKeys("true", "false");

                    mapAssertion
                            .extracting("true")
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                            .extracting(CYPHER_TYPE, CYPHER_VALUE)
                            .isEqualTo(List.of("Boolean", true));

                    mapAssertion
                            .extracting("false")
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                            .extracting(CYPHER_TYPE, CYPHER_VALUE)
                            .isEqualTo(List.of("Boolean", false));
                })));
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Map\", \"_value\": {\"mappy\": "
                + "{\"$type\": \"Map\", \"_value\": {\"inception\": "
                + "{\"$type\": \"Integer\", \"_value\": \"123\"}}}}}}}");

        assertResponseWithValues(
                response, fields("$parameter"), recordValues(hasTypeAndValueSatisfies("Map", object -> {
                    var mapAssertion = assertThat(object)
                            .asInstanceOf(MAP)
                            .containsOnlyKeys("mappy")
                            .extracting("mappy")
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE);

                    mapAssertion.extracting(CYPHER_TYPE).isEqualTo("Map");

                    mapAssertion
                            .extracting(CYPHER_VALUE)
                            .asInstanceOf(MAP)
                            .containsOnlyKeys("inception")
                            .extracting("inception")
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                            .extracting(CYPHER_TYPE, CYPHER_VALUE)
                            .isEqualTo(List.of("Integer", "123"));
                })));
    }

    @Test
    void shouldHandleMapNestedInList() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": "
                + "[{\"$type\":\"Map\",\"_value\":{\"innerValue\": "
                + "{\"$type\":\"Boolean\",\"_value\":true}}}]}}}");

        assertResponseWithValues(
                response, fields("$parameter"), recordValues(hasTypeAndValueSatisfies("List", object -> {
                    var mapAssertion = assertThat(object)
                            .asInstanceOf(LIST)
                            .hasSize(1)
                            .element(0)
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE);

                    mapAssertion.extracting(CYPHER_TYPE).isEqualTo("Map");

                    mapAssertion
                            .extracting(CYPHER_VALUE)
                            .asInstanceOf(MAP)
                            .containsOnlyKeys("innerValue")
                            .extracting("innerValue")
                            .asInstanceOf(MAP)
                            .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                            .extracting(CYPHER_TYPE, CYPHER_VALUE)
                            .isEqualTo(List.of("Boolean", true));
                })));
    }

    @Test
    void shouldHandleEmptyMaps() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": " + "{\"$type\": \"Map\", \"_value\": {}}}}}");

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue("Map", Map.of())));
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(String typeString, Object value) throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"" + typeString
                + "\", \"_value\": \"" + value.toString() + "\"}]}}}}");

        assertResponseWithValues(
                response,
                fields("$parameter"),
                recordValues(hasTypeAndValueSatisfies(
                        "List",
                        object -> assertThat(object)
                                .asInstanceOf(LIST)
                                .hasSize(1)
                                .element(0)
                                .asInstanceOf(MAP)
                                .extracting(CYPHER_TYPE, CYPHER_VALUE)
                                .isEqualTo(List.of(typeString, value.toString())))));
    }

    @Test
    void shouldHandleEmptyList() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": " + "{\"$type\": \"List\", \"_value\": []}}}");

        assertResponseWithValues(response, fields("$parameter"), recordValues(hasTypeAndValue("List", List.of())));
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl(
                "{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"List\",\"_value\": [{\"$type\":\"List\",\"_value\":[{\"$type\":\"Boolean\",\"_value\":true}]}]}}}");

        assertResponseWithValues(
                response,
                fields("$parameter"),
                recordValues(hasTypeAndValueSatisfies(
                        "List",
                        object -> assertThat(object)
                                .asInstanceOf(LIST)
                                .hasSize(1)
                                .element(0)
                                .asInstanceOf(MAP)
                                .containsOnlyKeys(CYPHER_TYPE, CYPHER_VALUE)
                                .extracting(CYPHER_VALUE)
                                .asInstanceOf(LIST)
                                .hasSize(1)
                                .element(0)
                                .asInstanceOf(MAP)
                                .extracting(CYPHER_TYPE, CYPHER_VALUE)
                                .isEqualTo(List.of("Boolean", true)))));
    }

    @Test
    void shouldReturnErrorIfParametersDoesNotContainMap() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\"," + "\"parameters\": 123}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleInvalidType() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl("{\"statement\": \"RETURN $parameter\","
                + "\"parameters\": {\"parameter\": {\"$type\": \"Bananas\", \"_value\": []}}}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled
    void shouldNotAcceptOutOfRangeNumbers() {
        // todo - needs additional validation in object mapper.
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

    @Test
    void uuid() throws IOException, InterruptedException {
        var response = testClient.sendRawJsonl(format(
                "{\"statement\": \"RETURN $parameter\","
                        + "\"parameters\": {\"parameter\": {\"$type\":\"%s\",\"_value\": \"%s\"}}}}}",
                "UUID", "ca3d9a43-09e3-4b66-9384-87ea25e27d01"));

        assertResponseWithValues(
                response,
                fields("$parameter"),
                recordValues(hasTypeAndValue("UUID", "ca3d9a43-09e3-4b66-9384-87ea25e27d01")));
    }

    private static String[] fields(String... fields) {
        return fields;
    }

    @SafeVarargs
    private static Consumer<CypherValueAssertions>[] recordValues(Consumer<CypherValueAssertions>... assertions) {
        return assertions;
    }
}
