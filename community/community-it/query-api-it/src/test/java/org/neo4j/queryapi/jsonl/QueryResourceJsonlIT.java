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
package org.neo4j.queryapi.jsonl;

import static java.util.List.of;
import static org.neo4j.queryapi.QueryResponseJsonlAssertions.assertThat;
import static org.neo4j.queryapi.testclient.QueryRequest.returnOne;

import java.io.IOException;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.TransactionType;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L},
        bookmarkReadyTimeoutInSeconds = 1)
class QueryResourceJsonlIT {

    private final DatabaseManagementService dbms;
    private final QueryAPITestClient testClient;

    QueryResourceJsonlIT(DatabaseManagementService dbms, QueryAPITestClient testClient) {
        this.dbms = dbms;
        this.testClient = testClient;
    }

    @Test
    void shouldExecuteSimpleQuery() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnMultipleRecords() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("UNWIND [1,2] as i RETURN i")
                .build());

        assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("i")
                .receivesRecord(1)
                .receivesRecord(2)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnMultipleRecordMultiFields() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("UNWIND [1,2] as i RETURN i, 'bob'")
                .build());

        assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("i", "'bob'")
                .receivesRecord(1, "bob")
                .receivesRecord(2, "bob")
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnBookmarks() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasBookmarks)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnUpdatedBookmark() throws IOException, InterruptedException {
        var responseA = testClient.autoCommitJsonl(returnOne());
        var bookmarkCapture = new Capture<List<String>>();

        assertThat(responseA)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        var initialBookmarks = bookmarkCapture.getCaptured().getFirst();

        var responseB = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(initialBookmarks)
                .build());

        assertThat(responseB)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarksNotEqualTo(initialBookmarks))
                .hasNoRemainingEvents();
    }

    @Test
    void shouldAcceptBookmarksAsInput() throws IOException, InterruptedException {
        var bookmarkCapture = new Capture<List<String>>();
        var responseA = testClient.autoCommitJsonl(returnOne());

        assertThat(responseA)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        var responseB = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(bookmarkCapture.getCaptured().getFirst())
                .build());

        assertThat(responseB)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasBookmarks)
                .hasNoRemainingEvents();
        ;
    }

    @Test
    void shouldAcceptMultipleBookmarksAsInput() throws IOException, InterruptedException {
        var bookmarkCapture = new Capture<List<String>>();

        var responseA = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());
        var responseB = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        assertThat(responseA)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        assertThat(responseB)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        var initialBookmarks =
                bookmarkCapture.getCaptured().stream().flatMap(List::stream).toList();
        var combinedBmResponse = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(initialBookmarks)
                .build());

        assertThat(combinedBmResponse)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesSummary(summaryAssertions ->
                        summaryAssertions.hasBookmarksDoesNotContainAnyElementsOf(initialBookmarks))
                .hasNoRemainingEvents();
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

        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        assertThat(response)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Transaction.BookmarkTimeout)
                .hasNoRemainingEvents();
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

        var responseA = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        // initial request times out
        assertThat(responseA)
                .hasContentType(QueryContentType.UNTYPED_L)
                .hasStatus(400)
                .receivesError(Status.Transaction.BookmarkTimeout)
                .hasNoRemainingEvents();
        ;

        var createNodeRequest = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        assertThat(createNodeRequest)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader()
                .receivesSummary()
                .hasNoRemainingEvents();

        var responseB = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .bookmarks(of(expectedBookmark))
                .build());

        assertThat(responseB)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(summaryAssertions -> summaryAssertions.hasBookmarksEqualTo(List.of(expectedBookmark)))
                .hasNoRemainingEvents();
    }

    @Test
    void callInTransactions() throws Exception {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i")
                .build());

        assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("i")
                .receivesRecord(4)
                .receivesRecord(2)
                .receivesRecord(1)
                .receivesRecord(0)
                .receivesSummary()
                .hasNoRemainingEvents();
    }

    @Test
    void shouldHandleErrorAfterStreamingStarts() throws IOException, InterruptedException {
        var response = testClient.autoCommitJsonl(QueryRequest.newBuilder()
                .statement("UNWIND range(10_000, 0, -1) AS n RETURN 10_000/n AS n")
                .build());

        assertThat(response)
                .isTransferEncodingChunked()
                .hasContentType(QueryContentType.UNTYPED_L)
                .wasSuccessful()
                .receivesHeader("n")
                .receivesNRecords(10000)
                .receivesError(Status.Statement.ArithmeticError)
                .hasNoRemainingEvents();
    }

    @ParameterizedTest
    @MethodSource("queryTypes")
    void shouldReturnQueryType(TransactionType transactionType, String statement, String expectedQueryType)
            throws IOException, InterruptedException {
        var response = testClient.executeQueryJsonl(
                transactionType, QueryRequest.newBuilder().statement(statement).build());

        QueryResponseJsonlAssertions.assertThat(response)
                .wasSuccessful()
                .receivesNHeaders(1)
                .receivesNRecords(expectedQueryType.startsWith("r") ? 1 : 0)
                .receivesSummary(queryAssertions -> queryAssertions.hasQueryTypeEqualTo(expectedQueryType));
    }

    static Stream<Arguments> queryTypes() {
        return Stream.of(TransactionType.values())
                .flatMap(type -> Stream.of(
                        Arguments.of(type, "RETURN 1", "r"),
                        Arguments.of(type, "CREATE ()", "w"),
                        Arguments.of(type, "CREATE (p:Person{name: 'Vozinha'}) RETURN p", "rw"),
                        Arguments.of(
                                type,
                                "CREATE CONSTRAINT constraint_name_%d FOR (n:Label) REQUIRE n.property_%d IS UNIQUE"
                                        .formatted(type.ordinal(), type.ordinal()),
                                "s")));
    }
}
