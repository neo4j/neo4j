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

import static java.lang.String.format;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L},
        sleepProcedureEnabled = true)
class QueryResourceTxJsonlIT {

    private final QueryAPITestClient testClient;
    private final TransactionManager txManager;
    private final String queryEndpoint;

    QueryResourceTxJsonlIT(QueryAPITestClient testClient, TransactionManager txManager) {
        this.testClient = testClient;
        this.txManager = txManager;
        this.queryEndpoint = testClient.getEndpoint();
    }

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void shouldStartTx() throws IOException, InterruptedException {
        var startTx = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());
        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(startTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        commitCaptured(txIdCapture);
    }

    @Test
    void shouldStartTxWithoutStatement() throws IOException, InterruptedException {
        var txIdCapture = beginTxWithoutStatement();

        commitCaptured(txIdCapture);
    }

    @Test
    void shouldStartTxWithParams() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .parameters(Map.of("i", "0"))
                .build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        commitCaptured(txIdCapture);
    }

    @Test
    void shouldHandleStartTxRuntimeError() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("UNWIND range(5, 0, -1) as N RETURN 3/N as f")
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(202)
                .receivesHeader("f")
                .receivesNRecords(5)
                .receivesError(Status.Statement.ArithmeticError)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleStartTxSyntaxError() throws IOException, InterruptedException {
        var res = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("DO SOMETHING!").build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldContinueTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();

        var continueTx = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        commitCaptured(txIdCapture);
    }

    @Test
    void shouldContinueTxWithCreateNode() throws IOException, InterruptedException, QueryApiTestClientException {
        var nodeCount = currentNodeCount("ContinueNode");
        var capturedTxIdCapture = beginTxWithoutStatement();
        var continueTx = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("CREATE (n:ContinueNode)").build(),
                capturedTxIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::hasFields)
                .receivesSummary(that -> that.hasTransaction(
                                capturedTxIdCapture.getCaptured().getFirst())
                        .hasTimers())
                .hasNoRemainingEvents();

        Assertions.assertThat(currentNodeCount("ContinueNode")).isEqualTo(nodeCount);
        commitCaptured(capturedTxIdCapture);
        Assertions.assertThat(currentNodeCount("ContinueNode")).isEqualTo(nodeCount + 1);
    }

    @Test
    void shouldContinueWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var capturedTxIdCapture = beginTxWithoutStatement();
        var continueTx = testClient.runInTxJsonl(
                QueryRequest.newBuilder().build(),
                capturedTxIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(
                                capturedTxIdCapture.getCaptured().getFirst())
                        .doesNotHaveTimers())
                .hasNoRemainingEvents();

        commitCaptured(capturedTxIdCapture);
    }

    @Test
    void shouldHandleContinueWithRuntimeError() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();
        var cont = testClient.runInTxJsonl(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(5, 0, -1) as N RETURN 3/N as f")
                        .build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(cont)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(202)
                .receivesHeader("f")
                .receivesNRecords(5)
                .receivesError(Status.Statement.ArithmeticError)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleContinueWithSyntaxError() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();

        var cont = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("DO SOMETHING!").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(cont)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldCommitTx() throws IOException, InterruptedException {
        var txIdCapture = beginTxWithoutStatement();
        var commit = testClient.commitTxJsonl(
                QueryRequest.newBuilder().statement("CREATE (n:QueryAPINode)").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::hasFields)
                .receivesSummary(
                        that -> that.doesNotHaveTransaction().hasBookmarks().hasTimers())
                .hasNoRemainingEvents();

        // verify node createdX
        var count = currentNodeCount("QueryAPINode");
        Assertions.assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldCommitWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("CREATE (n:CommitBlank)").build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(assertions -> assertions.hasFields())
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        var commitRes = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(
                        that -> that.doesNotHaveTransaction().hasBookmarks().doesNotHaveTimers())
                .hasNoRemainingEvents();

        // verify node created
        var count = currentNodeCount("CommitBlank");
        Assertions.assertThat(count).isEqualTo(1);
    }

    @Test
    void shouldHandleCommitWithRuntimeError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n:CommitRuntimeError)")
                .build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::hasFields)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        var commitRes = testClient.commitTxJsonl(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(5, 0, -1) as N RETURN 3/N as f")
                        .build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(202)
                .receivesHeader("f")
                .receivesNRecords(5)
                .receivesError(Status.Statement.ArithmeticError)
                .hasNoRemainingEvents();

        // verify node not created
        var newNodeCheck = currentNodeCount("CommitRuntimeError");
        Assertions.assertThat(newNodeCheck).isEqualTo(0);
    }

    @Test
    void shouldHandleCommitWithSyntaxError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n:CommitSyntaxError)")
                .build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::hasFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes = testClient.commitTxJsonl(
                QueryRequest.newBuilder().statement("FLAMINGO").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();

        // verify node not created
        var newNodeCheck = currentNodeCount("CommitSyntaxError");
        Assertions.assertThat(newNodeCheck).isEqualTo(0);
    }

    @Test
    void shouldRollbackTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var nodeCount = currentNodeCount("QueryAPIRollbackNode");

        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n:QueryAPIRollbackNode)")
                .build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::hasFields)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasTimers())
                .hasNoRemainingEvents();

        var rollback = testClient.rollbackTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(rollback)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(200)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                .hasNoRemainingEvents();

        commitCapturedDeletedTransaction(txIdCapture);

        Assertions.assertThat(currentNodeCount("QueryAPIRollbackNode")).isEqualTo(nodeCount);
    }

    void shouldHandleRollbackError() {
        // probably not possible but lets see
    }

    void shouldHandleCommitError() {
        // probably not possible but lets see
    }

    @Test
    void shouldNotAllowContinueAfterError() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();

        var failure = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("Garbage").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(failure)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();

        var continueTx = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(404)
                .receivesError(Status.Request.Invalid)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldNotAllowCommitAfterError() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();
        var failure = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("Garbage").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(failure)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Statement.SyntaxError)
                .hasNoRemainingEvents();

        commitCapturedDeletedTransaction(txIdCapture);
    }

    @ParameterizedTest
    @MethodSource("typedMimes")
    void shouldRespondWithTypedFormat(QueryContentType contentType, QueryContentType accepted)
            throws IOException, InterruptedException {
        var typedClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(accepted));

        var param = new LinkedHashMap<String, Object>();
        param.put("$type", "Integer");
        param.put("_value", "1");

        var typedReq = QueryRequest.newBuilder()
                .statement("RETURN $p")
                .parameters(Map.of("p", param))
                .build();
        var res = typedClient.beginTxJsonl();
        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .hasContentType(accepted)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueTx =
                typedClient.runInTxJsonl(typedReq, txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(accepted)
                .wasSuccessful()
                .receivesHeader("$p")
                .receivesNTypedRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.getCaptured().getFirst()))
                .hasNoRemainingEvents();

        var commit =
                typedClient.commitTxJsonl(typedReq, txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .hasContentType(accepted)
                .wasSuccessful()
                .receivesHeader("$p")
                .receivesNTypedRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveTransaction)
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("typedMimes")
    void shouldHandleBlankTypedTx(QueryContentType contentType, QueryContentType accepted)
            throws IOException, InterruptedException, QueryApiTestClientException {
        var typedClient = new QueryAPITestClient(queryEndpoint, contentType, List.of(accepted));

        var txIdCapture = beginTxWithoutStatement();

        var continueTx = typedClient.runInTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(continueTx)
                .hasContentType(accepted)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.getCaptured().getFirst()))
                .hasNoRemainingEvents();

        commitCaptured(txIdCapture);
    }

    @Test
    void shouldHaveExpectedTransactionIdLength() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = beginTxWithoutStatement();

        Assertions.assertThat(txIdCapture.getCaptured().getFirst())
                .hasSize(ServerSettings.transaction_id_length.defaultValue());

        commitCaptured(txIdCapture);
    }

    private Capture<String> beginTxWithoutStatement() throws IOException, InterruptedException {
        var startTx = testClient.beginTxJsonl();
        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(startTx)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveTimers())
                .hasNoRemainingEvents();
        return txIdCapture;
    }

    private void commitCaptured(Capture<String> txIdCapture) throws IOException, InterruptedException {
        var commitResponse = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commitResponse)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.doesNotHaveTransaction().doesNotHaveTimers())
                .hasNoRemainingEvents();
    }

    private void commitCapturedDeletedTransaction(Capture<String> txIdCapture)
            throws IOException, InterruptedException {
        var shouldNotBeAvailable =
                testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
        QueryResponseJsonlAssertions.assertThat(shouldNotBeAvailable)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(404)
                .receivesError(
                        Status.Request.Invalid,
                        String.format(
                                "Transaction with Id: \"%s\" was not found. It may have timed out and therefore rolled back or the routing header 'neo4j-cluster-affinity' was not provided.",
                                txIdCapture.getCaptured().getFirst()))
                .hasNoRemainingEvents();
    }

    private int currentNodeCount(String label) throws IOException, InterruptedException {
        var resp = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement(format("MATCH (n:%s) RETURN count(n) as count", label))
                .build());

        var capturedNodeCount = new Capture<Integer>();

        QueryResponseJsonlAssertions.assertThat(resp)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("count")
                .receivesRecord(assertions -> assertions.singleElement().matches(o -> {
                    if (o instanceof Integer num) {
                        capturedNodeCount.capture().accept(num);
                        return true;
                    }
                    return false;
                }))
                .receivesSummary();

        return capturedNodeCount.getCaptured().getFirst().intValue();
    }

    public static Stream<Arguments> typedMimes() {
        return Stream.of(
                Arguments.of(QueryContentType.TYPED_V1_0, QueryContentType.TYPED_L_V1_0),
                Arguments.of(QueryContentType.TYPED_V1_1, QueryContentType.TYPED_L_V1_1));
    }
}
