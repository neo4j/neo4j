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
package org.neo4j.queryapi.jsonl.tx;

import java.io.IOException;
import java.net.http.HttpResponse;
import java.util.List;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.tx.TransactionManager;

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
@QueryAPITestExtension(
        authEnabled = true,
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L})
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryResourceTxJsonlAuthenticationIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;
    private final String endpoint;

    QueryResourceTxJsonlAuthenticationIT(QueryAPITestClient testClient, TransactionManager txManager) {
        this.testClient = testClient;
        this.txManager = txManager;
        this.endpoint = testClient.getEndpoint();
    }

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    @Order(1)
    void shouldRequireCredentialChange() throws IOException, InterruptedException {
        var beginReq = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("SHOW USERS").build());

        QueryResponseJsonlAssertions.assertThat(beginReq)
                .hasStatus(400)
                .receivesError(Status.Security.CredentialsExpired)
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    void shouldReturnUnauthorizedWithWrongCredentials() throws IOException, InterruptedException {
        var badAuthClient = new QueryAPITestClient(
                endpoint, "neo4j", "I'm sneaky!", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));

        var res = badAuthClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(401)
                .receivesError(Status.Security.Unauthorized, "Invalid credential.")
                .hasNoRemainingEvents();
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var noAuth = new QueryAPITestClient(endpoint, QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));

        var res = noAuth.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(401)
                .receivesError(Status.Security.Unauthorized, "Invalid credential.")
                .hasNoRemainingEvents();
    }

    @Test
    @Order(2)
    void shouldAllowAccessWhenPasswordChanged() throws IOException, InterruptedException, QueryApiTestClientException {
        testClient.autoCommitJsonl(
                QueryRequest.newBuilder()
                        .statement("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'")
                        .build(),
                "system");

        var updateAuthClient = new QueryAPITestClient(
                endpoint, "neo4j", "secretPassword", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));

        var res = updateAuthClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        updateAuthClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
    }

    @Test
    @Disabled("Need to check error handling for Auth")
    @Timeout(30)
    void shouldErrorWhenTooManyIncorrectPasswordAttempts() throws IOException, InterruptedException {

        HttpResponse<Stream<String>> response;

        do {
            response = testClient.beginTxJsonl();
        } while (response.statusCode() != 429);

        QueryResponseJsonlAssertions.assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Security.AuthenticationRateLimit)
                .hasNoRemainingEvents();
    }

    @Test
    @Order(3)
    void shouldNotAllowUserToChangeMidTx() throws IOException, InterruptedException, QueryApiTestClientException {
        setupUsers();

        var txIdCapture = new Capture<String>();
        var bobClient = new QueryAPITestClient(
                endpoint, "bob", "secretPassword", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));
        var aliceClient = new QueryAPITestClient(
                endpoint, "alice", "secretPassword", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));

        var bobsTx = bobClient.beginTxJsonl();

        QueryResponseJsonlAssertions.assertThat(bobsTx)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var aliceTriesToSneak =
                aliceClient.runInTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(aliceTriesToSneak)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(404)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        var commit = bobClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
        QueryResponseJsonlAssertions.assertThat(commit)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    @Order(4)
    void shouldNotAllowUserToChangeOnCommit() throws IOException, InterruptedException, QueryApiTestClientException {

        var txIdCapture = new Capture<String>();
        var bobClient = new QueryAPITestClient(
                endpoint, "bob", "secretPassword", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));
        var aliceClient = new QueryAPITestClient(
                endpoint, "alice", "secretPassword", QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));

        var bobsTx = bobClient.beginTxJsonl();

        QueryResponseJsonlAssertions.assertThat(bobsTx)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var aliceTriesToSneak =
                aliceClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(aliceTriesToSneak)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(404)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        var commit = bobClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
        QueryResponseJsonlAssertions.assertThat(commit)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    private void setupUsers() throws IOException, InterruptedException {
        var updatedAuthClient = new QueryAPITestClient(endpoint, "neo4j", "secretPassword");
        updatedAuthClient.autoCommitJsonl(
                QueryRequest.newBuilder()
                        .statement("CREATE USER bob SET PASSWORD 'secretPassword' SET PASSWORD CHANGE NOT REQUIRED")
                        .build(),
                "system");
        updatedAuthClient.autoCommitJsonl(
                QueryRequest.newBuilder()
                        .statement("CREATE USER alice SET PASSWORD 'secretPassword' SET PASSWORD CHANGE NOT REQUIRED")
                        .build(),
                "system");
        updatedAuthClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("GRANT ROLE admin to bob").build(), "system");
        updatedAuthClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("GRANT ROLE admin to alice").build(), "system");
    }
}
