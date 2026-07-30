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
import java.util.EnumSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
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
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceTxIT {

    private static QueryAPITestClient testClient;
    private static DatabaseManagementService dbms;
    private static TransactionManager txManager;
    private static String queryEndpoint;

    @BeforeAll
    static void beforeAll() throws ProcedureException {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(BoltConnectorInternalSettings.local_channel_address, QueryResourceTxIT.class.getSimpleName())
                .setConfig(BoltConnector.enabled, true)
                .setConfig(BoltConnectorInternalSettings.enable_local_connector, true)
                .setConfig(ServerSettings.http_enabled_modules, EnumSet.allOf(ConfigurableServerModules.class))
                .impermanent()
                .build();

        resolveDependency(dbms, GlobalProcedures.class).register(sleepProcedure());
        txManager = resolveDependency(dbms, TransactionManager.class);
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        queryEndpoint = "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
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
    void shouldStartTx() throws IOException, InterruptedException, QueryApiTestClientException {
        var startTx = testClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(startTx).wasSuccessful();
        assertThat(startTx).hasRecord();
        assertThat(startTx).hasTransaction();
        testClient.commitTx(startTx.body().txId());
    }

    @Test
    void shouldStartTxWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var startTx = testClient.beginTx(QueryRequest.newBuilder().build());

        assertThat(startTx).wasSuccessful();
        assertThat(startTx).hasTransaction();
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
        testClient.commitTx(continueTx.body().txId());
    }

    @Test
    void shouldContinueWithoutStatement() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx();
        var continueTx =
                testClient.runInTx(QueryRequest.newBuilder().build(), res.body().txId());

        assertThat(continueTx).wasSuccessful();
        assertThat(continueTx).hasTransaction();
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
        var res = testClient.beginTx();
        var rollback = testClient.rollbackTx(res.body().txId());

        Assertions.assertThat(rollback.statusCode()).isEqualTo(200);

        var shouldNotBeAvailable = testClient.commitTx(res.body().txId());
        assertThat(shouldNotBeAvailable).wasNotFound();
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

    public static Stream<Arguments> typedMimes() {
        return Stream.of(QueryContentType.TYPED, QueryContentType.TYPED_V1_0).map(Arguments::of);
    }
}
