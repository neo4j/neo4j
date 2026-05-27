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
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.assertThat;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.eclipse.jetty.http.HttpStatus;
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
        sleepProcedureEnabled = true,
        queryApiTransactionTimeoutInSeconds = 5,
        transactionTimeoutInSeconds = 10)
class QueryResourceTxJsonlTimeoutIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;

    QueryResourceTxJsonlTimeoutIT(QueryAPITestClient testClient, TransactionManager txManager) {
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

    private String beginTransaction() throws IOException, InterruptedException {
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
