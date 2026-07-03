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
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.ws.rs.core.MediaType;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.server.queryapi.QueryMimeTypes;
import org.neo4j.test.extension.SkipOnSpd;

@QueryAPITestExtension
class QueryResourceErrorJsonlIT {

    private final HttpClient client;
    private final String queryEndpoint;

    QueryResourceErrorJsonlIT(QueryAPITestClient testClient) {
        this.client = HttpClient.newBuilder().build();
        this.queryEndpoint = testClient.getEndpoint();
    }

    @Test
    void blankRequestReturnsBadRequest() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "statement cannot be null or empty")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("TODO: Investigate why it triggering ErrorResponseWriter and not JsonlErrorResponseWriter")
    void invalidHTTPVerb() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .GET()
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(405)
                .receivesError(Status.Request.Invalid, "Method Not Allowed")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("TODO: Investigate why it triggering ErrorResponseWriter and not JsonlErrorResponseWriter")
    void invalidContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "text/csv")
                .header("Accept", "application/jsonl")
                .POST(HttpRequest.BodyPublishers.ofString("This is not acceptable"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(415)
                .receivesError(Status.Request.Invalid, "Unsupported Media Type")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("TODO: Investigate why it triggering ErrorResponseWriter and not JsonlErrorResponseWriter")
    void unknownContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/doesnt-exist")
                .header("Accept", "application/jsonl")
                .POST(HttpRequest.BodyPublishers.ofString("This is not acceptable"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(415)
                .receivesError(Status.Request.Invalid, "Unsupported Media Type")
                .hasNoRemainingEvents();
    }

    @Test
    void missingContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Accept", "application/jsonl")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(415)
                .receivesError(Status.Request.Invalid, "Unsupported Media Type")
                .hasNoRemainingEvents();
    }

    @Test
    void contentTypeHeaderDoesNotMatchBody() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("This is a random string!"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "Bad Request")
                .hasNoRemainingEvents();
    }

    @Test
    void unknownDatabase() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "thisDbisALie")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(404)
                .receivesError(Status.Database.DatabaseNotFound)
                .hasNoRemainingEvents();
    }

    @Test
    void invalidCypher() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequestJsonl(client, queryEndpoint, "{\"statement\": \"MATCH (n)\"}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                QueryMimeTypes.PLAIN_JSONL,
                QueryMimeTypes.TYPED_JSONL_V1x0,
                QueryMimeTypes.TYPED_JSONL_V1x1,
                QueryMimeTypes.TYPED_JSONL_V1x2
            })
    void invalidTypedCypher(String mimeType) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", MediaType.APPLICATION_JSON)
                .header("Accept", mimeType)
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"MATCH (n)\"}"))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(mimeType)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();
    }

    @Test
    void cypherButInAwayThatFieldsCanBeComputedButTheResultNot() throws IOException, InterruptedException {
        var response =
                QueryApiTestUtil.simpleRequestJsonl(client, queryEndpoint, "{\"statement\": \"RETURN 1/0 AS f\"}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(202)
                .receivesHeader("f")
                .receivesError(Status.Statement.ArithmeticError, "/ by zero")
                .hasNoRemainingEvents();
    }

    @Test
    void impersonationOnCommunityEditionAuthDisabled() throws IOException, InterruptedException {

        var body = """
            {"statement": "RETURN 1 AS n", "impersonatedUser": "Waldo"}
            """;

        var request = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/json")
                .header("Accept", "application/jsonl")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.ArgumentError, "Impersonation is not supported with auth disabled.")
                .hasNoRemainingEvents();
    }

    @Test
    @SkipOnSpd(reason = "SPD is enterprise and accepts system commands")
    void systemCommandsDontWork() throws IOException, InterruptedException {
        var response =
                QueryApiTestUtil.simpleRequestJsonl(client, queryEndpoint, "{\"statement\": \"CREATE DATABASE foo\"}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(
                        Status.Statement.UnsupportedAdministrationCommand,
                        "Unsupported administration command: CREATE DATABASE foo")
                .hasNoRemainingEvents();
    }

    @Test
    void errorDuringCypherExecution() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequestJsonl(
                client, queryEndpoint, "{\"statement\": \"UNWIND range(5, 0, -1) as N RETURN 3/N as f\"}");

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(202)
                .receivesHeader("f")
                .receivesNRecords(5)
                .receivesError(Status.Statement.ArithmeticError, "/ by zero")
                .hasNoRemainingEvents();
    }

    @Test
    void shouldRejectBlankStatement() throws Exception {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Request.Invalid, "statement cannot be null or empty")
                .hasNoRemainingEvents();
    }
}
