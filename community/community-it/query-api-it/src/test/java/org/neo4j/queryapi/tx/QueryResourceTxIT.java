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

import static java.lang.String.format;
import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.queryapi.QueryResponseAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;

@QueryAPITestExtension(sleepProcedureEnabled = true)
class QueryResourceTxIT {

    private final QueryAPITestClient testClient;

    private final TransactionManager txManager;
    private final String queryEndpoint;

    QueryResourceTxIT(QueryAPITestClient testClient, TransactionManager txManager) {
        this.testClient = testClient;
        this.txManager = txManager;
        this.queryEndpoint = testClient.getEndpoint();
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
    void shouldStartTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var startTx = testClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(startTx).wasSuccessful();
        assertThat(startTx).hasRecord();
        assertThat(startTx).hasTransaction();
        assertThat(startTx).hasTimers();
        testClient.commitTx(startTx.body().txId());
    }

    @Test
    void shouldStartTxWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var startTx = testClient.beginTx(QueryRequest.newBuilder().build());

        assertThat(startTx).wasSuccessful();
        assertThat(startTx).hasTransaction();
        assertThat(startTx).hasNoTimers();
        testClient.commitTx(startTx.body().txId());
    }

    @Test
    void shouldStartTxWithParams() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .parameters(Map.of("i", "0"))
                .build());

        assertThat(res).wasSuccessful();
        assertThat(res).hasRecord();
        assertThat(res).hasTransaction();
        assertThat(res).hasTimers();
        testClient.commitTx(res.body().txId());
    }

    @Test
    void shouldHandleStartTxRuntimeError() throws IOException, InterruptedException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("UNWIND range(5, 0, -1) as N RETURN 3/N")
                .build());

        assertThat(res).hasNoTransaction();
        assertThat(res).hasErrorStatus(202, Status.Statement.ArithmeticError);
    }

    @Test
    void shouldHandleStartTxSyntaxError() throws IOException, InterruptedException {
        var res = testClient.beginTx(
                QueryRequest.newBuilder().statement("DO SOMETHING!").build());

        assertThat(res).hasNoTransaction();
        assertThat(res).hasErrorStatus(400, Status.Statement.SyntaxError);
    }

    @Test
    void shouldContinueTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var continueTx = testClient.runInTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());

        assertThat(continueTx).wasSuccessful();
        assertThat(continueTx).hasRecord();
        assertThat(continueTx).hasTransaction();
        assertThat(continueTx).hasTimers();
        testClient.commitTx(continueTx.body().txId());
    }

    @Test
    void shouldContinueTxWithCreateNode() throws IOException, InterruptedException, QueryApiTestClientException {
        var nodeCount = currentNodeCount("ContinueNode");
        var res = testClient.beginTx();
        var continueTx = testClient.runInTx(
                QueryRequest.newBuilder().statement("CREATE (n:ContinueNode)").build(),
                res.body().txId());

        assertThat(continueTx).wasSuccessful();
        assertThat(continueTx).hasTransaction();
        assertThat(continueTx).hasTimers();
        Assertions.assertThat(currentNodeCount("ContinueNode")).isEqualTo(nodeCount);
        testClient.commitTx(continueTx.body().txId());
        Assertions.assertThat(currentNodeCount("ContinueNode")).isEqualTo(nodeCount + 1);
    }

    @Test
    void shouldContinueWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var continueTx =
                testClient.runInTx(QueryRequest.newBuilder().build(), res.body().txId());

        assertThat(continueTx).wasSuccessful();
        assertThat(continueTx).hasTransaction();
        assertThat(continueTx).hasNoTimers();
        testClient.commitTx(continueTx.body().txId());
    }

    @Test
    void shouldHandleContinueWithRuntimeError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var cont = testClient.runInTx(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(5, 0, -1) as N RETURN 3/N")
                        .build(),
                res.body().txId());

        assertThat(cont).hasNoTransaction();
        assertThat(cont).hasErrorStatus(202, Status.Statement.ArithmeticError);
    }

    @Test
    void shouldHandleContinueWithSyntaxError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();

        var cont = testClient.runInTx(
                QueryRequest.newBuilder().statement("DO SOMETHING!").build(),
                res.body().txId());

        assertThat(cont).hasNoTransaction();
        assertThat(cont).hasErrorStatus(400, Status.Statement.SyntaxError);
    }

    @Test
    void shouldCommitTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var commit = testClient.commitTx(
                QueryRequest.newBuilder().statement("CREATE (n:QueryAPINode)").build(),
                res.body().txId());

        assertThat(commit).wasSuccessful();
        assertThat(commit).hasNoTransaction();
        assertThat(commit).hasBookmark();
        assertThat(commit).hasTimers();

        // verify node created
        var newNodeCheck = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:QueryAPINode) RETURN count(n)")
                .build());
        assertThat(newNodeCheck).hasRecord(1);
    }

    @Test
    void shouldCommitWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(
                QueryRequest.newBuilder().statement("CREATE (n:CommitBlank)").build());
        var commitRes = testClient.commitTx(res.body().txId());

        assertThat(commitRes).wasSuccessful();
        assertThat(commitRes).hasBookmark();
        assertThat(commitRes).hasNoTimers();
        assertThat(commitRes).hasNoTransaction();

        // verify node created
        var newNodeCheck = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:CommitBlank) RETURN count(n)")
                .build());
        assertThat(newNodeCheck).hasRecord(1);
    }

    @Test
    void shouldHandleCommitWithRuntimeError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n:CommitRuntimeError)")
                .build());
        var commitRes = testClient.commitTx(
                QueryRequest.newBuilder()
                        .statement("UNWIND range(5, 0, -1) as N RETURN 3/N")
                        .build(),
                res.body().txId());

        assertThat(commitRes).hasNoTransaction();
        assertThat(commitRes).hasErrorStatus(202, Status.Statement.ArithmeticError);

        // verify node not created
        var newNodeCheck = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:CommitRuntimeError) RETURN count(n)")
                .build());
        assertThat(newNodeCheck).hasRecord(0);
    }

    @Test
    void shouldHandleCommitWithSyntaxError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n:CommitSyntaxError)")
                .build());
        var commitRes = testClient.commitTx(
                QueryRequest.newBuilder().statement("FLAMINGO").build(),
                res.body().txId());

        assertThat(commitRes).hasNoTransaction();
        assertThat(commitRes).hasErrorStatus(400, Status.Statement.SyntaxError);

        // verify node not created
        var newNodeCheck = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("MATCH (n:CommitSyntaxError) RETURN count(n)")
                .build());
        assertThat(newNodeCheck).hasRecord(0);
    }

    @Test
    void shouldRollbackTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var nodeCount = currentNodeCount("QueryAPIRollbackNode");

        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n:QueryAPIRollbackNode)")
                .build());
        var rollback = testClient.rollbackTx(res.body().txId());

        Assertions.assertThat(rollback.statusCode()).isEqualTo(200);

        var shouldNotBeAvailable = testClient.commitTx(res.body().txId());
        assertThat(shouldNotBeAvailable).wasNotFound();
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
        var res = testClient.beginTx();
        var failure = testClient.runInTx(
                QueryRequest.newBuilder().statement("Garbage").build(),
                res.body().txId());

        assertThat(failure).hasErrorStatus(400, Status.Statement.SyntaxError);

        var commit = testClient.runInTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());

        assertThat(commit).wasNotFound();
    }

    @Test
    void shouldNotAllowCommitAfterError() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var failure = testClient.runInTx(
                QueryRequest.newBuilder().statement("Garbage").build(),
                res.body().txId());

        assertThat(failure).hasErrorStatus(400, Status.Statement.SyntaxError);

        var commit = testClient.commitTx(
                QueryRequest.newBuilder().statement("RETURN 1").build(),
                res.body().txId());

        assertThat(commit).wasNotFound();
    }

    @ParameterizedTest
    @MethodSource("typedMimes")
    void shouldRespondWithTypedFormat(QueryContentType format)
            throws IOException, InterruptedException, QueryApiTestClientException {
        var typedClient = new QueryAPITestClient(queryEndpoint, format);

        var param = new LinkedHashMap<String, Object>();
        param.put("$type", "Integer");
        param.put("_value", "1");

        var typedReq = QueryRequest.newBuilder()
                .statement("RETURN $p")
                .parameters(Map.of("p", param))
                .build();
        var res = typedClient.beginTx();
        assertThat(res).wasSuccessful();

        var continueTx = typedClient.runInTx(typedReq, res.body().txId());

        assertThat(continueTx).wasSuccessful().hasTypedRecord();

        var commit = typedClient.commitTx(typedReq, res.body().txId());

        assertThat(commit).wasSuccessful().hasTypedRecord();
    }

    @ParameterizedTest
    @MethodSource("typedMimes")
    void shouldHandleBlankTypedTx(QueryContentType format)
            throws IOException, InterruptedException, QueryApiTestClientException {
        var typedClient = new QueryAPITestClient(queryEndpoint, format);

        var res = typedClient.beginTx();
        assertThat(res).wasSuccessful();

        var continueTx = typedClient.runInTx(res.body().txId());

        assertThat(continueTx).wasSuccessful();

        var commit = typedClient.commitTx(res.body().txId());

        assertThat(commit).wasSuccessful();
    }

    @Test
    void shouldHaveExpectedTransactionIdLength() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        assertThat(res).hasTransaction();
        Assertions.assertThat(res.body().txId()).hasSize(ServerSettings.transaction_id_length.defaultValue());
        testClient.commitTx(res.body().txId());
    }

    @ParameterizedTest
    @MethodSource("queryRequestElements")
    void shouldHandleRequestFieldsInAnyOrder(List<String> elements)
            throws IOException, InterruptedException, QueryApiTestClientException {
        var response = testClient.sendRawBeginTx("{ %s }".formatted(String.join(",", elements)));

        QueryResponseAssertions.assertThat(response).wasSuccessful();

        testClient.rollbackTx(response.body().txId());
    }

    private int currentNodeCount(String label) throws IOException, InterruptedException {
        return testClient
                .autoCommit(QueryRequest.newBuilder()
                        .statement(format("MATCH (n:%s) RETURN count(n)", label))
                        .build())
                .body()
                .data()
                .get("values")
                .get(0)
                .get(0)
                .asInt();
    }

    public static Stream<Arguments> typedMimes() {
        return Stream.of(QueryContentType.TYPED, QueryContentType.TYPED_V1_0).map(Arguments::of);
    }

    static Stream<Arguments> queryRequestElements() {
        var includeCounters = """
                "includeCounters": true""";
        var parameters = """
                    "parameters": {
                      "value": 1
                    }\
                """;
        var statement = """
                "statement": "RETURN $value AS one\"""";
        return Stream.of(
                        List.of(includeCounters, parameters, statement),
                        List.of(includeCounters, statement, parameters),
                        List.of(statement, includeCounters, parameters),
                        List.of(statement, parameters, includeCounters),
                        List.of(parameters, statement, includeCounters),
                        List.of(parameters, includeCounters, statement))
                .map(Arguments::of);
    }
}
