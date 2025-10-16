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
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.internal.kernel.api.exceptions.ProcedureException;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.api.procedure.GlobalProcedures;
import org.neo4j.kernel.database.Database;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.configuration.ConfigurableServerModules;
import org.neo4j.server.configuration.ServerSettings;
import org.neo4j.server.queryapi.request.AccessMode;
import org.neo4j.server.queryapi.tx.TransactionManager;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

public class QueryResourceTxConfigIT {

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
                        QueryResourceTxConfigIT.class.getSimpleName())
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

    @BeforeEach
    void beforeEach() {
        txManager.removeAllTransactions();
    }

    @AfterEach
    void afterEach() {
        Assertions.assertThat(txManager.openTransactionCount()).isEqualTo(0);
    }

    @Test
    void shouldErrorForWrongAccessMode() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(
                QueryRequest.newBuilder().accessMode(AccessMode.READ).build());
        var write = testClient.runInTx(
                QueryRequest.newBuilder().statement("CREATE (n)").build(),
                res.body().txId());

        assertThat(write).hasErrorStatus(400, Status.Statement.AccessMode);
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
    void shouldReturnBookmarks() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTx(
                QueryRequest.newBuilder().statement("RETURN 1").build());
        var commit = testClient.commitTx(res.body().txId());

        assertThat(commit).wasSuccessful();
        assertThat(commit).hasBookmark();
    }

    @Test
    void shouldReturnUpdatedBookmark() throws IOException, InterruptedException, QueryApiTestClientException {
        var firstBookmark = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        var res = testClient.beginTx();
        var commit = testClient.commitTx(
                QueryRequest.newBuilder().statement("CREATE (n)").build(),
                res.body().txId());

        assertThat(commit).wasSuccessful();
        assertThat(commit).hasBookmark();
        Assertions.assertThat(firstBookmark.body().bookmarks())
                .isNotEqualTo(commit.body().bookmarks());
    }

    @Test
    void shouldAcceptBookmarksAsInput() throws IOException, InterruptedException, QueryApiTestClientException {
        var initialBookmark = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        var waiting = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(initialBookmark.body().bookmarks())
                .build());
        var commit = testClient.commitTx(waiting.body().txId());

        assertThat(commit).wasSuccessful();
        Assertions.assertThat(initialBookmark.body().bookmarks())
                .isNotEqualTo(commit.body().bookmarks());
    }

    @Test
    void shouldAcceptMultipleBookmarksAsInput() throws IOException, InterruptedException, QueryApiTestClientException {
        var initialBookmarkA = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());
        var initialBookmarkB = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        var waiting = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(Stream.concat(
                                initialBookmarkA.body().bookmarks().stream(),
                                initialBookmarkB.body().bookmarks().stream())
                        .toList())
                .build());
        var commit = testClient.commitTx(waiting.body().txId());

        assertThat(commit).wasSuccessful();
        Assertions.assertThat(initialBookmarkA.body().bookmarks())
                .isNotEqualTo(commit.body().bookmarks());
        Assertions.assertThat(initialBookmarkB.body().bookmarks())
                .isNotEqualTo(commit.body().bookmarks());
    }

    @Test
    void shouldTimeoutWaitingForUnreachableBookmark()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        QueryApiTestUtil.resolveDependency(dbms, Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        QueryApiTestUtil.getLastClosedTransactionId(dbms) + 1)),
                List.of()));

        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());

        assertThat(res).hasErrorStatus(400, Status.Transaction.BookmarkTimeout);
    }

    @Test
    void shouldWaitForUpdatedBookmark() throws IOException, InterruptedException, QueryApiTestClientException {
        var lastTxId = QueryApiTestUtil.getLastClosedTransactionId(dbms);
        var nextTxId = lastTxId + 1;
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        QueryApiTestUtil.resolveDependency(dbms, Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        nextTxId)),
                List.of()));

        var res = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());

        assertThat(res).hasErrorStatus(400, Status.Transaction.BookmarkTimeout);

        // move the bookmark forward one tx
        testClient.autoCommit(QueryRequest.newBuilder().statement("CREATE (n)").build());

        var working = testClient.beginTx(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());
        assertThat(working).wasSuccessful();

        var commit = testClient.commitTx(working.body().txId());
        assertThat(commit).wasSuccessful();
        Assertions.assertThat(commit.body().bookmarks()).isNotEqualTo(List.of(expectedBookmark));
    }

    @Test
    void shouldReturnQueryStats() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder()
                .statement("RETURN 1")
                .includeCounters()
                .build();

        var res = testClient.beginTx(returnReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(returnReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(returnReq, commitBegin.body().txId());

        assertThat(res).hasQueryStatistics();
        assertThat(continueRes).hasQueryStatistics();
        assertThat(commitRes).hasQueryStatistics();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldNotReturnQueryStatsByDefault() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();

        var res = testClient.beginTx(returnReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(returnReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(returnReq, commitBegin.body().txId());

        assertThat(res).hasNoQueryStatistics();
        assertThat(continueRes).hasNoQueryStatistics();
        assertThat(commitRes).hasNoQueryStatistics();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldReturnLabelDoesNotExistNotification()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var unknownLabelReq = QueryRequest.newBuilder()
                .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                .build();

        var res = testClient.beginTx(unknownLabelReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes =
                testClient.runInTx(unknownLabelReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(unknownLabelReq, commitBegin.body().txId());

        assertThat(res)
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT);
        assertThat(continueRes)
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT);
        assertThat(commitRes)
                .hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT);

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldNotReturnNotificationsIfNonePresent()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();

        var res = testClient.beginTx(returnReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(returnReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(returnReq, commitBegin.body().txId());

        assertThat(res).hasNoNotifications();
        assertThat(continueRes).hasNoNotifications();
        assertThat(commitRes).hasNoNotifications();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldReturnQueryPlan() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("EXPLAIN RETURN 1").build();

        var res = testClient.beginTx(returnReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(returnReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(returnReq, commitBegin.body().txId());

        assertThat(res).hasQueryPlan();
        assertThat(continueRes).hasQueryPlan();
        assertThat(commitRes).hasQueryPlan();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldNotReturnQueryPlanByDefault() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();

        var res = testClient.beginTx(returnReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(returnReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(returnReq, commitBegin.body().txId());

        assertThat(res).hasNoQueryPlan();
        assertThat(continueRes).hasNoQueryPlan();
        assertThat(commitRes).hasNoQueryPlan();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldReturnProfiledQueryPlan() throws IOException, InterruptedException, QueryApiTestClientException {
        var profileReq = QueryRequest.newBuilder().statement("PROFILE RETURN 1").build();

        var res = testClient.beginTx(profileReq);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(profileReq, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(profileReq, commitBegin.body().txId());

        assertThat(res).hasProfiledQueryPlan();
        assertThat(continueRes).hasProfiledQueryPlan();
        assertThat(commitRes).hasProfiledQueryPlan();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    @Test
    void shouldNotReturnProfiledQueryPlanByDefault()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var request = QueryRequest.newBuilder().statement("RETURN 1").build();

        var res = testClient.beginTx(request);

        var continueBeginRes = testClient.beginTx();
        var continueRes = testClient.runInTx(request, continueBeginRes.body().txId());

        var commitBegin = testClient.beginTx();
        var commitRes = testClient.commitTx(request, commitBegin.body().txId());

        assertThat(res).hasNoProfiledQueryPlan();
        assertThat(continueRes).hasNoProfiledQueryPlan();
        assertThat(commitRes).hasNoProfiledQueryPlan();

        assertThat(testClient.commitTx(res.body().txId())).wasSuccessful();
        assertThat(testClient.commitTx(continueBeginRes.body().txId())).wasSuccessful();
    }

    void shouldRejectConfigOnSubsequentRequests() throws IOException, InterruptedException {
        // todo this is probably a good idea. We dont want to confuse users that configuring mid tx is possible.
    }
}
