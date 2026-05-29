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
package org.neo4j.queryapi;

import static java.util.List.of;
import static org.neo4j.queryapi.QueryApiTestUtil.setupLogging;
import static org.neo4j.queryapi.QueryResponseAssertions.assertThat;
import static org.neo4j.queryapi.testclient.QueryRequest.returnOne;
import static org.neo4j.server.queryapi.response.format.Fieldnames.VALUES_KEY;

import java.io.IOException;
import java.time.Duration;
import java.util.List;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.configuration.connectors.BoltConnector;
import org.neo4j.configuration.connectors.BoltConnectorInternalSettings;
import org.neo4j.configuration.connectors.ConnectorPortRegister;
import org.neo4j.configuration.connectors.ConnectorType;
import org.neo4j.configuration.connectors.HttpConnector;
import org.neo4j.configuration.helpers.SocketAddress;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;

class QueryResourceIT {

    private static DatabaseManagementService dbms;
    private static QueryAPITestClient testClient;

    @BeforeAll
    static void beforeAll() {
        setupLogging();
        var builder = new TestDatabaseManagementServiceBuilder();
        dbms = builder.setConfig(HttpConnector.enabled, true)
                .setConfig(HttpConnector.listen_address, new SocketAddress("localhost", 0))
                .setConfig(BoltConnectorInternalSettings.local_channel_address, QueryResourceIT.class.getSimpleName())
                .setConfig(GraphDatabaseSettings.bookmark_ready_timeout, Duration.ofSeconds(1))
                .setConfig(BoltConnector.enabled, true)
                .impermanent()
                .build();
        var portRegister = QueryApiTestUtil.resolveDependency(dbms, ConnectorPortRegister.class);
        var queryEndpoint =
                "http://" + portRegister.getLocalAddress(ConnectorType.HTTP) + "/db/{databaseName}/query/v2";
        testClient = new QueryAPITestClient(queryEndpoint);
    }

    @AfterAll
    static void teardown() {
        dbms.shutdown();
    }

    @Test
    void shouldExecuteSimpleQuery() throws IOException, InterruptedException {
        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(response).wasSuccessful();
        assertThat(response).hasRecord(1);
    }

    @Test
    void shouldReturnMultipleRecords() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("UNWIND [1,2] as i RETURN i")
                .build());

        assertThat(response).wasSuccessful().hasRecords(1, 2);
    }

    @Test
    void shouldReturnMultipleRecordMultiFields() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("UNWIND [1,2] as i RETURN i, 'bob'")
                .build());

        assertThat(response).wasSuccessful().hasRecords(of(of(1, "bob"), of(2, "bob")));
    }

    @Test
    void shouldReturnBookmarks() throws IOException, InterruptedException {
        var response = testClient.autoCommit(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(response).wasSuccessful().hasRecord().hasBookmark();
    }

    @Test
    void shouldReturnUpdatedBookmark() throws IOException, InterruptedException {
        var responseA = testClient.autoCommit(returnOne());

        assertThat(responseA).wasSuccessful();

        var initialBookmarks = responseA.body().bookmarks();

        var responseB = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(initialBookmarks)
                .build());

        assertThat(responseB).wasSuccessful().hasBookmark();

        Assertions.assertThat(responseB.body().bookmarks()).isNotEqualTo(initialBookmarks);
    }

    @Test
    void shouldAcceptBookmarksAsInput() throws IOException, InterruptedException {
        var responseA = testClient.autoCommit(returnOne());

        assertThat(responseA).wasSuccessful().hasBookmark();

        var responseB = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(responseA.body().bookmarks())
                .build());

        assertThat(responseB).wasSuccessful().hasBookmark();
    }

    @Test
    void shouldAcceptMultipleBookmarksAsInput() throws IOException, InterruptedException {
        var responseA = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());
        var responseB = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        var bmA = responseA.body().bookmarks().get(0);
        var bmB = responseB.body().bookmarks().get(0);

        var combinedBmResponse = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(of(bmA, bmB))
                .build());

        assertThat(combinedBmResponse).wasSuccessful().hasBookmark();

        var newBm = combinedBmResponse.body().bookmarks().get(0);

        Assertions.assertThat(newBm).isNotEqualTo(bmA);
        Assertions.assertThat(newBm).isNotEqualTo(bmB);
    }

    @Test
    void shouldTimeoutWaitingForUnreachableBookmark() throws IOException, InterruptedException {
        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        QueryApiTestUtil.resolveDependency(dbms, Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        QueryApiTestUtil.getLastClosedTransactionId(dbms) + 1)),
                List.of()));

        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        assertThat(response).hasErrorStatus(400, Status.Transaction.BookmarkTimeout);
    }

    @Test
    void shouldWaitForUpdatedBookmark() throws IOException, InterruptedException {
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

        var responseA = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        // initial request times out
        assertThat(responseA).hasErrorStatus(400, Status.Transaction.BookmarkTimeout);

        var createNodeRequest = testClient.autoCommit(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        assertThat(createNodeRequest).wasSuccessful();

        var responseB = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        assertThat(responseB).wasSuccessful().hasBookmark(expectedBookmark);
    }

    @Test
    void callInTransactions() throws Exception {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i")
                .build());

        assertThat(response).wasSuccessful().hasRecords(4, 2, 1, 0);
    }

    @Test
    void shouldHandleErrorAfterStreamingStarts() throws IOException, InterruptedException {
        var response = testClient.autoCommit(QueryRequest.newBuilder()
                .statement("UNWIND range(10_000, 0, -1) AS n RETURN 10_000/n AS n")
                .build());

        QueryResponseAssertions.assertThat(response)
                .hasErrorStatus(202, Status.Statement.ArithmeticError)
                .hasFieldNames("n");

        var parsedJson = response.body().data();

        // All valid values goes through
        Assertions.assertThat(parsedJson.get(VALUES_KEY).size()).isEqualTo(10000);
    }
}
