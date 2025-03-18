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

import static org.assertj.core.api.Assertions.fail;
import static org.neo4j.kernel.api.exceptions.Status.Transaction.TransactionAccessedConcurrently;
import static org.neo4j.queryapi.QueryApiTestUtil.resolveDependency;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.queryapi.QueryApiTestUtil.sleepProcedure;
import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;

import java.io.IOException;
import java.util.EnumSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
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
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceTxErrorIT {

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
                        QueryResourceTxErrorIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
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

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void shouldNotSwitchDbMidTx() throws IOException, InterruptedException {
        var startTx = testClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        var newDb = testClient.runInTx(
                QueryRequest.newBuilder().build(), startTx.body().txId(), "anotherdb");

        assertThat(newDb).wasNotFound().hasNoTransaction();

        var commit = testClient.commitTx(startTx.body().txId());
        assertThat(commit).hasNoTransaction();
    }

    @Test
    void shouldNotSwitchDbOnCommit() throws IOException, InterruptedException {
        var startTx = testClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        var newDb = testClient.commitTx(
                QueryRequest.newBuilder().build(), startTx.body().txId(), "anotherdb");

        assertThat(newDb).wasNotFound().hasNoTransaction();

        testClient.commitTx(startTx.body().txId());
    }

    @Test
    void shouldHandleDatabaseNotFound() throws IOException, InterruptedException {
        var begin = testClient.beginTx(null, "doesnotexist");
        assertThat(begin).wasDatabaseNotFound();
    }

    @Test
    void shouldRejectCallInTransactions() throws IOException, InterruptedException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i")
                .build());

        assertThat(res).hasErrorStatus(202, Status.Transaction.TransactionStartFailed);
        assertThat(res).hasNoTransaction();
    }

    @Test
    void shouldReturn404ForUnknownTxId() throws IOException, InterruptedException {
        var continueTx = testClient.runInTx("isthatyou");
        var commit = testClient.commitTx("isthisme");
        var rollback = testClient.rollbackTx("whoami");

        assertThat(continueTx).wasNotFound();
        assertThat(commit).wasNotFound();
        assertThat(rollback).wasNotFound();
    }

    @Test
    void shouldNotAllowConcurrentTxAccess() throws IOException, InterruptedException {
        var res = testClient.beginTx();
        var latch = new CountDownLatch(1);

        Executors.newSingleThreadExecutor().submit(() -> {
            try {
                testClient.runInTx(
                        QueryRequest.newBuilder()
                                .statement("CALL queryAPI.nightnight(5000)")
                                .build(),
                        res.body().txId());
                latch.countDown();
            } catch (IOException | InterruptedException ignored) {
                fail("Error starting long running transaction");
            }
        });

        Thread.sleep(500);

        var concurrent = testClient.runInTx(res.body().txId());
        assertThat(concurrent).hasErrorStatus(400, TransactionAccessedConcurrently);

        // wait for tx to free up
        latch.await();

        var accessReq = testClient.commitTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());
        assertThat(accessReq).wasSuccessful();
        assertThat(accessReq).hasNoTransaction();
    }

    @Test
    void blankStatementShouldOpenTx() throws IOException, InterruptedException {
        var res = testClient.beginTx(QueryRequest.newBuilder().statement("").build());

        assertThat(res).wasSuccessful();
        assertThat(res).hasTransaction();
        testClient.commitTx(res.body().txId());
    }
}
