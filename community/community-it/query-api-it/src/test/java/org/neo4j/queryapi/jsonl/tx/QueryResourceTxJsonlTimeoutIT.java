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

import static java.time.Duration.ofSeconds;
import static org.neo4j.queryapi.QueryApiTestUtil.resolveDependency;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.queryapi.QueryApiTestUtil.sleepProcedure;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.assertThat;

import java.io.IOException;
import java.util.EnumSet;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.http.HttpStatus;
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
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceTxJsonlTimeoutIT {

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
                        QueryResourceTxJsonlTimeoutIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .setConfig(ServerSettings.queryapi_transaction_timeout, ofSeconds(5))
                .setConfig(GraphDatabaseSettings.transaction_timeout, ofSeconds(10))
                .impermanent()
                .build();
        resolveDependency(dbms, GlobalProcedures.class).register(sleepProcedure());
        txManager = resolveDependency(dbms, TransactionManager.class);
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        String queryEndpoint =
                "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient =
                new QueryAPITestClient(queryEndpoint, QueryContentType.UNTYPED, List.of(QueryContentType.UNTYPED_L));
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
        var txId = beginTransaction();

        // timeout transaction
        Thread.sleep(ofSeconds(10));

        var timeout = testClient.commitTxJsonl(txId);

        assertThat(timeout)
                .hasStatus(HttpStatus.NOT_FOUND_404)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldTimeoutTransactionAtAPILevelAfterContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var txId = beginTransaction();

        // timeout transaction
        Thread.sleep(ofSeconds(10));

        var timeout = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build(), txId);

        assertThat(timeout)
                .hasStatus(HttpStatus.NOT_FOUND_404)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldIncreaseTimeoutAfterEachRequest() throws IOException, InterruptedException, QueryApiTestClientException {
        var txId = beginTransaction();

        Thread.sleep(ofSeconds(1));

        var extended = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build(), txId);

        assertThat(extended)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasUpdatedTimeout)
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txId);

        assertThat(commit).wasSuccessful().receivesNHeaders(1).receivesSummary().hasNoRemainingEvents();
    }

    @Test
    void shouldIncreaseTimeoutAfterBlankContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var txId = beginTransaction();

        Thread.sleep(ofSeconds(1));

        var extended = testClient.runInTxJsonl(txId);

        assertThat(extended)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(0)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasUpdatedTimeout)
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txId);

        assertThat(commit).wasSuccessful().receivesNHeaders(1).receivesSummary().hasNoRemainingEvents();
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnContinue()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var txId = beginTransaction();

        var longRunning = testClient.runInTxJsonl(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                        .build(),
                txId);

        assertThat(longRunning)
                .hasStatus(HttpStatus.BAD_REQUEST_400)
                .receivesError(Status.Transaction.TransactionTimedOutClientConfiguration)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnBegin() throws IOException, InterruptedException {
        var longRunning = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                .build());

        assertThat(longRunning)
                .hasStatus(HttpStatus.BAD_REQUEST_400)
                .receivesError(Status.Transaction.TransactionTimedOutClientConfiguration)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldTimeoutTxAtKernelLevelOnCommit() throws IOException, InterruptedException, QueryApiTestClientException {
        var txId = beginTransaction();

        var longRunning = testClient.commitTxJsonl(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(500)")
                        .build(),
                txId);

        assertThat(longRunning)
                .hasStatus(HttpStatus.BAD_REQUEST_400)
                .receivesError(Status.Transaction.TransactionTimedOutClientConfiguration)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldConfigureKernelTimeout() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("UNWIND range(0, 200) AS i CALL queryAPI.nightnight(200)")
                .maxExecutionTime(5)
                .build());

        assertThat(res)
                .hasStatus(HttpStatus.BAD_REQUEST_400)
                .receivesError(Status.Transaction.TransactionTimedOutClientConfiguration)
                .hasNoRemainingEvents();
    }

    private static String beginTransaction() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl();
        var txIdCapture = new Capture<String>();
        assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();
        return txIdCapture.getCaptured().getFirst();
    }
}
