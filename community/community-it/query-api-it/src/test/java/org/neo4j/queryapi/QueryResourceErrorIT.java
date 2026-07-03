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
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERRORS_KEY;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_CODE;
import static org.neo4j.server.queryapi.response.format.Fieldnames.ERROR_MESSAGE;

import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import javax.ws.rs.core.HttpHeaders;
import javax.ws.rs.core.MediaType;
import org.assertj.core.util.Lists;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.server.queryapi.QueryMimeTypes;
import org.neo4j.test.extension.SkipOnSpd;

@QueryAPITestExtension
class QueryResourceErrorIT {

    private final HttpClient client;
    private final ObjectMapper MAPPER = new ObjectMapper();
    private final String queryEndpoint;

    QueryResourceErrorIT(QueryAPITestClient testClient) {
        this.queryEndpoint = testClient.getEndpoint();
        this.client = HttpClient.newHttpClient();
    }

    @Test
    void blankRequestReturnsBadRequest() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"statement cannot be null or empty\"}]}");
    }

    @Test
    void invalidHTTPVerb() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .GET()
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(405);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Method Not Allowed\"}]}");
    }

    @Test
    void invalidContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "text/csv")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("This is not acceptable"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(415);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Unsupported Media Type\"}]}");
    }

    @Test
    void unknownContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/doesnt-exist")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("This is not acceptable"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(415);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Unsupported Media Type\"}]}");
    }

    @Test
    void missingContentTypeHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(415);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Unsupported Media Type\"}]}");
    }

    @Test
    void contentTypeHeaderDoesNotMatchBody() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("This is a random string!"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\","
                        + "\"message\":\"Bad Request\"}]}");
    }

    @Test
    void invalidAcceptHeader() throws IOException, InterruptedException {
        var httpRequest = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/json")
                .header("Accept", "nonsense-value")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(406);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo(
                        "{\"errors\":[{\"code\":\"Neo.ClientError.Request.Invalid\",\"message\":\"Not Acceptable\"}]}");
    }

    @Test
    void unknownDatabase() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "thisDbisALie")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(404);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        var possibleBodies = Lists.newArrayList(
                "{\"errors\":[{\"code\":\"Neo.ClientError.Database.DatabaseNotFound\",\"message\":\"Graph not found: thisdbisalie\"}]}",
                // we get this result in SPD running non-community
                "{\"errors\":[{\"code\":\"Neo.ClientError.Database.DatabaseNotFound\",\"message\":\"Database does not exist. Database name: 'thisDbisALie'.\"}]}");
        assertThat(response.body()).isIn(possibleBodies);
    }

    @Test
    void invalidCypher() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"MATCH (n)\"}");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .startsWith("{\"errors\":[{\"code\":\"Neo.ClientError.Statement.SyntaxError\","
                        + "\"message\":\"Query cannot conclude with MATCH");
    }

    @ParameterizedTest
    @ValueSource(
            strings = {
                QueryMimeTypes.TYPED_JSON,
                QueryMimeTypes.TYPED_JSON_V1x0,
                QueryMimeTypes.TYPED_JSON_V1x1,
                QueryMimeTypes.TYPED_JSON_V1x2
            })
    void invalidTypedCypher(String mimeType) throws IOException, InterruptedException {
        var request = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", mimeType)
                .header("Accept", mimeType)
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"MATCH (n)\"}"))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(mimeType);
        assertThat(response.body())
                .startsWith("{\"errors\":[{\"code\":\"Neo.ClientError.Statement.SyntaxError\","
                        + "\"message\":\"Query cannot conclude with MATCH");
    }

    @Test
    void cypherButInAwayThatFieldsCanBeComputedButTheResultNot() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"RETURN 1/0 AS f\"}");

        assertThat(response.statusCode()).isEqualTo(202);
        assertThat(response.body())
                .startsWith(
                        "{\"data\":{\"fields\":[\"f\"],\"values\":[]},\"errors\":[{\"code\":\"Neo.ClientError.Statement.ArithmeticError\",\"message\":\"/ by zero\"}]}");
    }

    @Test
    void impersonationOnCommunityEditionAuthDisabled() throws IOException, InterruptedException {

        var body = """
            {"statement": "RETURN 1 AS n", "impersonatedUser": "Waldo"}
            """;

        var request = HttpRequest.newBuilder()
                .uri(URI.create(queryEndpoint.replace("{databaseName}", "neo4j")))
                .header("Content-Type", "application/json")
                .header("Accept", "application/json")
                .POST(HttpRequest.BodyPublishers.ofString(body))
                .build();

        var response = client.send(request, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.body()).contains("""
                        {"errors":[{"code":"Neo.ClientError.Statement.ArgumentError","message":"Impersonation is not supported with auth disabled."}]}""");
    }

    @Test
    @SkipOnSpd(reason = "SPD is enterprise and accepts system commands")
    void systemCommandsDontWork() throws IOException, InterruptedException {
        var response =
                QueryApiTestUtil.simpleRequest(client, queryEndpoint, "{\"statement\": \"CREATE DATABASE foo\"}");

        assertThat(response.statusCode()).isEqualTo(400);
        assertThat(response.headers().allValues(HttpHeaders.CONTENT_TYPE)).contains(MediaType.APPLICATION_JSON);
        assertThat(response.body())
                .isEqualTo("{\"errors\":[{\"code\":\"Neo.ClientError.Statement.UnsupportedAdministrationCommand\","
                        + "\"message\":\"Unsupported administration command: CREATE DATABASE foo\"}]}");
    }

    @Test
    void errorDuringCypherExecution() throws IOException, InterruptedException {
        var response = QueryApiTestUtil.simpleRequest(
                client, queryEndpoint, "{\"statement\": \"UNWIND range(5, 0, -1) as N RETURN 3/N\"}");

        assertThat(response.statusCode()).isEqualTo(202);

        var parsedBody = MAPPER.readTree(response.body());

        assertThat(parsedBody.get("errors").size()).isEqualTo(1);
        assertThat(parsedBody.get("errors").get(0).get("code").asText())
                .isEqualTo("Neo.ClientError.Statement.ArithmeticError");
        assertThat(parsedBody.get("errors").get(0).get("message").asText()).isEqualTo("/ by zero");
    }

    @Test
    void shouldRejectBlankStatement() throws Exception {
        var httpRequest = QueryApiTestUtil.baseRequestBuilder(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.noBody())
                .build();
        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofString());

        assertThat(response.statusCode()).isEqualTo(400);
        var parsedJson = MAPPER.readTree(response.body());

        assertThat(parsedJson.get(ERRORS_KEY).get(0).get(ERROR_CODE).asText())
                .isEqualTo(Status.Request.Invalid.code().serialize());
        assertThat(parsedJson.get(ERRORS_KEY).get(0).get(ERROR_MESSAGE).asText())
                .isEqualTo("statement cannot be null or empty");
    }
}
