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

import static java.time.Duration.ofSeconds;
import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;

import java.io.IOException;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.tx.TransactionManager;

@QueryAPITestExtension(
        sleepProcedureEnabled = true,
        queryApiTransactionTimeoutInSeconds = 5,
        transactionTimeoutInSeconds = 10)
class QueryResourceTxTimeoutIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;

    QueryResourceTxTimeoutIT(QueryAPITestClient testClient, TransactionManager txManager) {
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
        var res = testClient.beginTx();
        assertThat(res).wasSuccessful();
        assertThat(res).hasTransaction();

        // timeout transaction
        Thread.sleep(ofSeconds(10));

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
        Thread.sleep(ofSeconds(10));

        var timeout = testClient.runInTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());
        assertThat(timeout).wasNotFound();
        assertThat(timeout).hasNoTransaction();
    }

    @Test
    void shouldIncreaseTimeoutAfterEachRequest() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();

        Thread.sleep(ofSeconds(1));

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

        Thread.sleep(ofSeconds(1));

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
