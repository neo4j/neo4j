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
import static org.neo4j.queryapi.QueryApiTestUtil.sleepProcedure;
import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;

import java.io.IOException;
import java.time.Duration;
import java.util.EnumSet;
import java.util.concurrent.TimeUnit;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceTxTimeoutIT {

    private static QueryAPITestClient testClient;
    private static DatabaseManagementService dbms;
    private static TransactionManager txManager;

    @BeforeAll
    static void beforeAll() throws ProcedureException {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(
                        BoltConnectorInternalSettings.local_channel_address,
                        QueryResourceTxTimeoutIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .setConfig(ServerSettings.queryapi_transaction_timeout, Duration.ofSeconds(5))
                .setConfig(GraphDatabaseSettings.transaction_timeout, Duration.ofSeconds(10))
                .impermanent()
                .build();
        resolveDependency(dbms, GlobalProcedures.class).register(sleepProcedure());
        txManager = resolveDependency(dbms, TransactionManager.class);
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        String queryEndpoint =
                "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient = new QueryAPITestClient(queryEndpoint);
    }

    @AfterAll
    static void afterAll() {
        dbms.shutdown();
    }

    @BeforeEach
    void beforeEach() {
        txManager.removeAllTransactions();
    }

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void shouldTimeoutTransactionAtAPILevelAfterCommit()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        assertThat(res).wasSuccessful();
        assertThat(res).hasTransaction();

        // timeout transaction
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        var timeout = testClient.commitTx(res.body().txId());
        assertThat(timeout).wasNotFound();
        assertThat(timeout).hasNoTransaction();
    }

    @Test
    void shouldTimeoutTransactionAtAPILevelAfterContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        assertThat(res).wasSuccessful();
        assertThat(res).hasTransaction();

        // timeout transaction
        Thread.sleep(TimeUnit.SECONDS.toMillis(10));

        var timeout = testClient.runInTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());
        assertThat(timeout).wasNotFound();
        assertThat(timeout).hasNoTransaction();
    }

    @Test
    void shouldIncreaseTimeoutAfterEachRequest() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        var extended = testClient.runInTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());

        assertThat(extended).wasSuccessful();
        assertThat(extended).hasUpdatedTimeout();
        testClient.commitTx(extended.body().txId());
    }

    @Test
    void shouldIncreaseTimeoutAfterBlankContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();

        Thread.sleep(TimeUnit.SECONDS.toMillis(1));

        var extended = testClient.runInTx(res.body().txId());

        assertThat(extended).wasSuccessful();
        assertThat(extended).hasUpdatedTimeout();

        testClient.commitTx(res.body().txId());
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();

        var longRunning = testClient.runInTx(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                        .build(),
                res.body().txId());
        assertThat(longRunning).hasErrorStatus(400, Status.Transaction.TransactionTimedOutClientConfiguration);
        assertThat(longRunning).hasNoTransaction();
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnBegin() throws IOException, InterruptedException {
        var longRunning = testClient.beginTx(QueryRequest.newBuilder()
                .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                .build());

        assertThat(longRunning).hasErrorStatus(400, Status.Transaction.TransactionTimedOutClientConfiguration);
        assertThat(longRunning).hasNoTransaction();
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnCommit() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var longRunning = testClient.commitTx(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                        .build(),
                res.body().txId());

        assertThat(longRunning).hasErrorStatus(400, Status.Transaction.TransactionTimedOutClientConfiguration);
        assertThat(longRunning).hasNoTransaction();
    }

    @Test
    void shouldConfigureKernelTimeout() throws IOException, InterruptedException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(200)")
                .maxExecutionTime(5)
                .build());

        assertThat(res).hasErrorStatus(400, Status.Transaction.TransactionTimedOutClientConfiguration);
    }
}
