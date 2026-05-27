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
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L})
class QueryResourceParametersJsonlIT {

    private final HttpClient client;
    private final QueryAPITestClient testClient;
    private final String queryEndpoint;

    public QueryResourceParametersJsonlIT(QueryAPITestClient testClient) {
        this.testClient = testClient;
        this.client = HttpClient.newHttpClient();
        this.queryEndpoint = testClient.getEndpoint();
    }

    public static Stream<Arguments> paramTypes() {
        return Stream.of(
                Arguments.of(true, true),
                Arguments.of(123, 123),
                Arguments.of(123L, 123),
                Arguments.of(12.3F, 12.3),
                Arguments.of(12.3D, 12.3),
                Arguments.of(Integer.MAX_VALUE, Integer.MAX_VALUE),
                Arguments.of(Long.MAX_VALUE, Long.MAX_VALUE));
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleParameters(Object parameter, Object expectedParameter) throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", parameter))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord(expectedParameter)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleStringParam() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", "Hello"))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord("Hello")
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleMapParameters(Object parameter, Object expectedParameter)
            throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", Map.of("mappy", parameter)))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord(Map.of("mappy", expectedParameter))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleNestedMaps() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", Map.of("mappy", Map.of("inception", 123))))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord(Map.of("mappy", Map.of("inception", 123)))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("paramTypes")
    void shouldHandleListParameters(Object parameter, Object expectedParameter)
            throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", List.of(parameter)))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord(List.of(expectedParameter))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleNestedLists() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN $parameter")
                .parameters(Map.of("parameter", List.of(List.of(123))))
                .build());

        QueryResponseJsonlAssertions.assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("$parameter")
                .receivesRecord(List.of(List.of(123)))
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnErrorIfParametersDoesNotContainMap() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"RETURN $parameter\"," + "\"parameters\": 123}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "Bad Request")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled
    void shouldNotAcceptOutOfRangeNumbers() {
        // todo - needs additional validation in object mapper.
    }
}
