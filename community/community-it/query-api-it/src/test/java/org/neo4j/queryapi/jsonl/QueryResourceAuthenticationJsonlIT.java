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
import java.util.stream.Stream;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;

/**
 * TODO: Enabled extension to configured to start and stop
 * database per method, not only per class. This test class
 * makes changes on the database which can not be easily reverted.
 * So, the re-creation of the database between tests were the
 * strategy for these tests.
 * However, since it is not possible in the current framework.
 * The tests were ordered in a way that the changes on the database
 * only affects tests which depends on it.
 */
@QueryAPITestExtension(authEnabled = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryResourceAuthenticationJsonlIT {

    private final HttpClient client;

    private final String queryEndpoint;

    QueryResourceAuthenticationJsonlIT(QueryAPITestClient testClient) {
        this.client = HttpClient.newBuilder().build();
        this.queryEndpoint = testClient.getEndpoint();
    }

    @Test
    @Order(1)
    void shouldRequireCredentialChange() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "system")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"SHOW USERS\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Security.CredentialsExpired)
                .hasNoRemainingEvents();
    }

    @Test
    @Order(2)
    void shouldAllowAccessWhenPasswordChanged() throws IOException, InterruptedException {
        updateInitialPassword();

        var accessRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "secretPassword"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(accessRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    void shouldReturnUnauthorizedWithWrongCredentials() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "I'm sneaky!"))
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(401)
                .receivesError(Status.Security.Unauthorized, "Invalid credential.")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(401)
                .receivesError(Status.Security.Unauthorized, "No authentication header supplied.")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    void shouldReturnUnauthorizedWithInvalidAuthHeader() throws IOException, InterruptedException {
        var httpRequest = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                .header("Authorization", "Just let me in. Thanks!")
                .POST(HttpRequest.BodyPublishers.ofString("{\"statement\": \"RETURN 1\"}"))
                .build();

        var response = client.send(httpRequest, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Security.Unauthorized, "Invalid authentication header.")
                .hasNoRemainingEvents();
    }

    @Test
    @Timeout(30)
    @Disabled("Need to check error handling for Auth")
    void shouldErrorWhenTooManyIncorrectPasswordAttempts() throws IOException, InterruptedException {
        updateInitialPassword();

        HttpResponse<Stream<String>> response;

        do {
            var req = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "neo4j")
                    .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "WrongPasswordBud"))
                    .POST(HttpRequest.BodyPublishers.ofString("shouldn't be parsing this"))
                    .build();
            response = client.send(req, HttpResponse.BodyHandlers.ofLines());
        } while (response.statusCode() != 429);

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(
                        Status.Security.AuthenticationRateLimit,
                        "Too many failed authentication requests. Please wait 5 seconds and try again.")
                .hasNoRemainingEvents();
    }

    private void updateInitialPassword() throws IOException, InterruptedException {
        var updatePasswordReq = QueryApiTestUtil.baseRequestBuilderJsonl(queryEndpoint, "system")
                .header("Authorization", QueryApiTestUtil.encodedCredentials("neo4j", "neo4j"))
                .POST(HttpRequest.BodyPublishers.ofString(
                        "{\"statement\": \"ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'\"}"))
                .build();

        var updatePasswordResp = client.send(updatePasswordReq, HttpResponse.BodyHandlers.ofLines());

        QueryResponseJsonlAssertions.assertThat(updatePasswordResp)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader()
                .receivesSummary()
                .hasNoRemainingEvents();
    }
}
