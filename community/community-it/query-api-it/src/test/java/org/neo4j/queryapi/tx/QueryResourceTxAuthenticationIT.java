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
package org.neo4j.queryapi.tx;

import java.io.IOException;
import java.net.http.HttpResponse;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.Timeout;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.queryapi.testclient.QueryResponse;
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
@QueryAPITestExtension(authEnabled = true)
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryResourceTxAuthenticationIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;
    private final String endpoint;

    QueryResourceTxAuthenticationIT(QueryAPITestClient testClient, TransactionManager txManager) {
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
        var beginReq = testClient.beginTx(
                QueryRequest.newBuilder().statement("SHOW USERS").build());

        QueryResponseAssertions.assertThat(beginReq).hasErrorStatus(400, Status.Security.CredentialsExpired);
    }

    @Test
    @Order(2)
    void shouldReturnUnauthorizedWithWrongCredentials() throws IOException, InterruptedException {
        var badAuthClient = new QueryAPITestClient(endpoint, "neo4j", "I'm sneaky!");

        var res = badAuthClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseAssertions.assertThat(res).hasErrorStatus(401, Status.Security.Unauthorized);
    }

    @Test
    @Order(3)
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var noAuth = new QueryAPITestClient(endpoint);

        var res = noAuth.beginTx(QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseAssertions.assertThat(res).hasErrorStatus(401, Status.Security.Unauthorized);
    }

    @Test
    @Order(4)
    void shouldAllowAccessWhenPasswordChanged() throws IOException, InterruptedException, QueryApiTestClientException {
        testClient.autoCommit(
                QueryRequest.newBuilder()
                        .statement("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'")
                        .build(),
                "system");
        var updateAuthClient = new QueryAPITestClient(endpoint, "neo4j", "secretPassword");

        var res = updateAuthClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseAssertions.assertThat(res).wasSuccessful();
        QueryResponseAssertions.assertThat(res).hasRecord();

        updateAuthClient.commitTx(res.body().txId());
    }

    @Test
    @Order(5)
    void shouldNotAllowUserToChangeMidTx() throws IOException, InterruptedException, QueryApiTestClientException {
        setupUsers();

        var bobClient = new QueryAPITestClient(endpoint, "bob", "secretPassword");
        var aliceClient = new QueryAPITestClient(endpoint, "alice", "secretPassword");

        var bobsTx = bobClient.beginTx();
        QueryResponseAssertions.assertThat(bobsTx).wasSuccessful();

        var aliceTriesToSneak = aliceClient.runInTx(bobsTx.body().txId());

        QueryResponseAssertions.assertThat(aliceTriesToSneak).hasErrorStatus(404, Status.Request.Invalid);
        var commit = bobClient.commitTx(bobsTx.body().txId());
        QueryResponseAssertions.assertThat(commit).wasSuccessful();
    }

    @Test
    @Order(6)
    void shouldNotAllowUserToChangeOnCommit() throws IOException, InterruptedException, QueryApiTestClientException {
        setupUsers();

        var bobClient = new QueryAPITestClient(endpoint, "bob", "secretPassword");
        var aliceClient = new QueryAPITestClient(endpoint, "alice", "secretPassword");

        var bobsTx = bobClient.beginTx();
        QueryResponseAssertions.assertThat(bobsTx).wasSuccessful();

        var aliceTriesToSneak = aliceClient.commitTx(bobsTx.body().txId());

        QueryResponseAssertions.assertThat(aliceTriesToSneak).hasErrorStatus(404, Status.Request.Invalid);
        bobClient.commitTx(bobsTx.body().txId());
    }

    @Test
    @Order(7)
    @Timeout(30)
    void shouldErrorWhenTooManyIncorrectPasswordAttempts() throws IOException, InterruptedException {
        HttpResponse<QueryResponse> response;

        do {
            response = testClient.beginTx();
        } while (response.statusCode() != 429);
        QueryResponseAssertions.assertThat(response).hasErrorStatus(429, Status.Security.AuthenticationRateLimit);
    }

    private void setupUsers() throws IOException, InterruptedException {
        testClient.autoCommit(
                QueryRequest.newBuilder()
                        .statement("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'")
                        .build(),
                "system");
        var updatedAuthClient = new QueryAPITestClient(endpoint, "neo4j", "secretPassword");
        updatedAuthClient.autoCommit(
                QueryRequest.newBuilder()
                        .statement("CREATE USER bob SET PASSWORD 'secretPassword' SET PASSWORD CHANGE NOT REQUIRED")
                        .build(),
                "system");
        updatedAuthClient.autoCommit(
                QueryRequest.newBuilder()
                        .statement("CREATE USER alice SET PASSWORD 'secretPassword' SET PASSWORD CHANGE NOT REQUIRED")
                        .build(),
                "system");
        updatedAuthClient.autoCommit(
                QueryRequest.newBuilder().statement("GRANT ROLE admin to bob").build(), "system");
        updatedAuthClient.autoCommit(
                QueryRequest.newBuilder().statement("GRANT ROLE admin to alice").build(), "system");
    }
}
