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

import static org.neo4j.queryapi.QueryApiTestUtil.resolveDependency;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;

import java.io.IOException;
import java.net.http.HttpResponse;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseAssertions;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.queryapi.testclient.QueryResponse;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceTxAuthenticationIT {

    private static QueryAPITestClient testClient;
    private static DatabaseManagementService dbms;
    private static TransactionManager txManager;
    private static String endpoint;

    @BeforeEach
    void beforeAll() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(BoltConnectorInternalSettings.local_channel_address, QueryResourceTxIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(GraphDatabaseSettings.auth_enabled, true)
                .impermanent()
                .build();

        txManager = resolveDependency(dbms, TransactionManager.class);
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        endpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient = new QueryAPITestClient(endpoint, "neo4j", "neo4j");
    }

    @AfterEach
    void cleanUp() {
        dbms.shutdown();
    }

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void shouldRequireCredentialChange() throws IOException, InterruptedException {
        var beginReq = testClient.beginTx(
                QueryRequest.newBuilder().statement("SHOW USERS").build());

        QueryResponseAssertions.assertThat(beginReq).hasErrorStatus(400, Status.Security.CredentialsExpired);
    }

    @Test
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
    void shouldReturnUnauthorizedWithWrongCredentials() throws IOException, InterruptedException {
        var badAuthClient = new QueryAPITestClient(endpoint, "neo4j", "I'm sneaky!");

        var res = badAuthClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseAssertions.assertThat(res).hasErrorStatus(401, Status.Security.Unauthorized);
    }

    @Test
    void shouldReturnUnauthorizedWithMissingAuthHeader() throws IOException, InterruptedException {
        var noAuth = new QueryAPITestClient(endpoint);

        var res = noAuth.beginTx(QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseAssertions.assertThat(res).hasErrorStatus(401, Status.Security.Unauthorized);
    }

    @Test
    @Timeout(30)
    void shouldErrorWhenTooManyIncorrectPasswordAttempts() throws IOException, InterruptedException {
        testClient.autoCommit(
                QueryRequest.newBuilder()
                        .statement("ALTER CURRENT USER SET PASSWORD FROM 'neo4j' TO 'secretPassword'")
                        .build(),
                "system");

        HttpResponse<QueryResponse> response;

        do {
            response = testClient.beginTx();
        } while (response.statusCode() != 429);
        QueryResponseAssertions.assertThat(response).hasErrorStatus(429, Status.Security.AuthenticationRateLimit);
    }

    @Test
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
