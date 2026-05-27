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

import static org.assertj.core.api.Assertions.fail;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.tx.TransactionManager;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L},
        sleepProcedureEnabled = true)
class QueryResourceTxJsonlErrorIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;

    QueryResourceTxJsonlErrorIT(QueryAPITestClient testClient, TransactionManager txManager) {
        this.testClient = testClient;
        this.txManager = txManager;
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
    void shouldNotSwitchDbMidTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var startTx = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseJsonlAssertions.assertThat(startTx)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var newDb = testClient.runInTxJsonl(
                QueryRequest.newBuilder().build(), txIdCapture.getCaptured().getLast(), "anotherdb");
        QueryResponseJsonlAssertions.assertThat(newDb)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldNotSwitchDbOnCommit() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var startTx = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());
        QueryResponseJsonlAssertions.assertThat(startTx)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var newDb = testClient.commitTxJsonl(
                QueryRequest.newBuilder().build(), txIdCapture.getCaptured().getLast(), "anotherdb");
        QueryResponseJsonlAssertions.assertThat(newDb)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleDatabaseNotFound() throws IOException, InterruptedException, QueryApiTestClientException {
        var begin = testClient.beginTxJsonl(null, "doesnotexist");
        QueryResponseJsonlAssertions.assertThat(begin)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Database.DatabaseNotFound)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldRejectCallInTransactionsWhenDelayedExecution() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i")
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasStatus(202)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesError(Status.Transaction.TransactionStartFailed)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldRejectCallInTransactions() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CALL() { CREATE (t:Test) } IN TRANSACTIONS OF 1 ROWS")
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasStatus(400)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Transaction.TransactionStartFailed)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldRunCallInTransactionsInImplicit() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i")
                .txType("IMPLICIT")
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(4)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturn404ForUnknownTxId() throws IOException, InterruptedException {
        var continueTx = testClient.runInTxJsonl("isthatyou");
        var commit = testClient.commitTxJsonl("isthisme");
        var rollback = testClient.rollbackTxJsonl("whoami");

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(commit)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(rollback)
                .hasStatus(404)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldNotAllowConcurrentTxAccess() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var res = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var latch = new CountDownLatch(1);

        try (var executorService = Executors.newSingleThreadExecutor()) {
            executorService.submit(() -> {
                try {
                    var run = testClient.runInTxJsonl(
                            QueryRequest.newBuilder()
                                    .statement("CALL queryAPI.nightnight(5000)")
                                    .build(),
                            txIdCapture.getCaptured().getLast());
                    QueryResponseJsonlAssertions.assertThat(run)
                            .wasSuccessful()
                            .hasContentType(QueryContentType.UNTYPED_L)
                            .receivesNHeaders(1)
                            .receivesSummary()
                            .hasNoRemainingEvents();
                    latch.countDown();
                } catch (IOException | InterruptedException ignored) {
                    fail("Error starting long running transaction");
                }
            });

            Thread.sleep(500);

            var concurrent = testClient.runInTxJsonl(txIdCapture.getCaptured().getLast());
            QueryResponseJsonlAssertions.assertThat(concurrent)
                    .hasStatus(400)
                    .hasContentType(QueryContentType.UNTYPED_L)
                    .receivesError(Status.Transaction.TransactionAccessedConcurrently)
                    .hasNoRemainingEvents();

            // wait for tx to free up
            latch.await();

            var accessReq = testClient.commitTxJsonl(
                    QueryRequest.newBuilder().statement("RETURN 1").build(),
                    txIdCapture.getCaptured().getLast());
            QueryResponseJsonlAssertions.assertThat(accessReq)
                    .wasSuccessful()
                    .hasContentType(QueryContentType.UNTYPED_L)
                    .receivesHeader("1")
                    .receivesRecord(1)
                    .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                    .hasNoRemainingEvents();
        }
    }

    @Test
    void blankStatementShouldOpenTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();

        var res =
                testClient.beginTxJsonl(QueryRequest.newBuilder().statement("").build());
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                .hasNoRemainingEvents();
    }
}
