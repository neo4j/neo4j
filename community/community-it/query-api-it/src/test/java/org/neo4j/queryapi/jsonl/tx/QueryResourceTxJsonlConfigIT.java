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

import java.io.IOException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.notifications.NotificationCodeWithDescription;
import org.neo4j.queryapi.QueryApiTestUtil;
import org.neo4j.queryapi.QueryResponseJsonlAssertions;
import org.neo4j.queryapi.annotation.QueryAPITestExtension;
import org.neo4j.queryapi.assertions.Capture;
import org.neo4j.queryapi.testclient.QueryAPITestClient;
import org.neo4j.queryapi.testclient.QueryApiTestClientException;
import org.neo4j.queryapi.testclient.QueryContentType;
import org.neo4j.queryapi.testclient.QueryRequest;
import org.neo4j.server.queryapi.request.AccessMode;
import org.neo4j.server.queryapi.tx.TransactionManager;

@QueryAPITestExtension(
        contentType = QueryContentType.UNTYPED,
        acceptedContentTypes = {QueryContentType.UNTYPED_L},
        bookmarkReadyTimeoutInSeconds = 1,
        sleepProcedureEnabled = true)
class QueryResourceTxJsonlConfigIT {

    private final QueryAPITestClient testClient;
    private final DatabaseManagementService dbms;
    private final TransactionManager txManager;

    QueryResourceTxJsonlConfigIT(
            DatabaseManagementService dbms, TransactionManager txManager, QueryAPITestClient testClient) {
        this.dbms = dbms;
        this.txManager = txManager;
        this.testClient = testClient;
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
        var txIdCapture = new Capture<String>();
        var res = testClient.beginTxJsonl(
                QueryRequest.newBuilder().accessMode(AccessMode.READ).build());

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var write = testClient.runInTxJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(write)
                .hasStatus(400)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Statement.AccessMode)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldStartTxWithParams() throws IOException, InterruptedException, QueryApiTestClientException {
        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("RETURN 1")
                .parameters(Map.of("i", "0"))
                .build());

        var txIdCapture = new Capture<String>();

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
    }

    @Test
    void shouldReturnBookmarks() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var res = testClient.beginTxJsonl(
                QueryRequest.newBuilder().statement("RETURN 1").build());

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesRecord(1)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasBookmarks)
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnUpdatedBookmark() throws IOException, InterruptedException, QueryApiTestClientException {
        var bookmarkCapture = new Capture<List<String>>();
        var txIdCapture = new Capture<String>();

        var firstBookmark = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        QueryResponseJsonlAssertions.assertThat(firstBookmark)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()));

        var res = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build(),
                txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        Assertions.assertThat(bookmarkCapture.getCaptured().getFirst())
                .isNotEqualTo(bookmarkCapture.getCaptured().getLast());
    }

    @Test
    void shouldAcceptBookmarksAsInput() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var bookmarkCapture = new Capture<List<String>>();

        var initialBookmark = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        QueryResponseJsonlAssertions.assertThat(initialBookmark)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        var waiting = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(bookmarkCapture.getCaptured().getFirst())
                .build());

        QueryResponseJsonlAssertions.assertThat(waiting)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader()
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        Assertions.assertThat(bookmarkCapture.getCaptured().getFirst())
                .isNotEqualTo(bookmarkCapture.getCaptured().getLast());
    }

    @Test
    void shouldAcceptMultipleBookmarksAsInput() throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var bookmarkCapture = new Capture<List<String>>();

        var initialBookmarkA = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());
        var initialBookmarkB = testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        QueryResponseJsonlAssertions.assertThat(initialBookmarkA)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(initialBookmarkB)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        var waiting = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(bookmarkCapture.getCaptured().stream()
                        .flatMap(Collection::stream)
                        .toList())
                .build());

        QueryResponseJsonlAssertions.assertThat(waiting)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader()
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());

        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(bookmarkCapture.capture()))
                .hasNoRemainingEvents();

        Assertions.assertThat(bookmarkCapture.getCaptured().getFirst())
                .isNotEqualTo(bookmarkCapture.getCaptured().getLast());
        Assertions.assertThat(bookmarkCapture.getCaptured().get(1))
                .isNotEqualTo(bookmarkCapture.getCaptured().getLast());
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

        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasStatus(400)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Transaction.BookmarkTimeout)
                .hasNoRemainingEvents();
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

        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());

        QueryResponseJsonlAssertions.assertThat(res)
                .hasStatus(400)
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesError(Status.Transaction.BookmarkTimeout)
                .hasNoRemainingEvents();

        // move the bookmark forward one tx
        testClient.autoCommitJsonl(
                QueryRequest.newBuilder().statement("CREATE (n)").build());

        var working = testClient.beginTxJsonl(QueryRequest.newBuilder()
                .statement("CREATE (n)")
                .bookmarks(List.of(expectedBookmark))
                .build());

        QueryResponseJsonlAssertions.assertThat(working)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader()
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commit = testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst());
        QueryResponseJsonlAssertions.assertThat(commit)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(that -> that.hasBookmarks(
                        bookmarks -> Assertions.assertThat(bookmarks).isNotEqualTo(List.of(expectedBookmark))))
                .hasNoRemainingEvents();
    }

    @Test
    void shouldReturnQueryStats() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder()
                .statement("RETURN 1")
                .includeCounters()
                .build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(returnReq);

        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(that -> that.hasCounters(
                                counters -> Assertions.assertThat(counters).isNotNull())
                        .hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();

        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveCounters())
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(returnReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(that -> that.hasCounters(
                        counters -> Assertions.assertThat(counters).isNotNull()))
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();
        var commitRes =
                testClient.commitTxJsonl(returnReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(that -> that.hasCounters(
                        counters -> Assertions.assertThat(counters).isNotNull()))
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst()))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldNotReturnQueryStatsByDefault() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();

        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(returnReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveCounters())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(returnReq, txIdCapture.getCaptured().getFirst());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveCounters)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(returnReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveCounters)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().getFirst()))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldReturnLabelDoesNotExistNotification()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var txIdCapture = new Capture<String>();
        var unknownLabelReq = QueryRequest.newBuilder()
                .statement("MATCH (n:thisLabelDoesNotExist), (m:thisLabelDoesNotExist) return m, n")
                .build();

        var res = testClient.beginTxJsonl(unknownLabelReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("m", "n")
                .receivesNRecords(0)
                .receivesSummary(that -> that.hasNotifications(
                                NotificationCodeWithDescription.MISSING_LABEL,
                                NotificationCodeWithDescription.MISSING_LABEL,
                                NotificationCodeWithDescription.CARTESIAN_PRODUCT)
                        .hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes = testClient.runInTxJsonl(
                unknownLabelReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(0)
                .receivesSummary(that -> that.hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT))
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes = testClient.commitTxJsonl(
                unknownLabelReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(0)
                .receivesSummary(that -> that.hasNotifications(
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.MISSING_LABEL,
                        NotificationCodeWithDescription.CARTESIAN_PRODUCT))
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldNotReturnNotificationsIfNonePresent()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(returnReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveNotifications())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(returnReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveNotifications)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(returnReq, txIdCapture.getCaptured().getLast());

        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveNotifications)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldReturnQueryPlan() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("EXPLAIN RETURN 1").build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(returnReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasQueryPlan())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(returnReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasQueryPlan)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(returnReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasQueryPlan)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldNotReturnQueryPlanByDefault() throws IOException, InterruptedException, QueryApiTestClientException {
        var returnReq = QueryRequest.newBuilder().statement("RETURN 1").build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(returnReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveQueryPlan())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesNRecords(0)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(returnReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveQueryPlan)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesNRecords(0)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveQueryPlan())
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(returnReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveQueryPlan)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldReturnProfiledQueryPlan() throws IOException, InterruptedException, QueryApiTestClientException {
        var profileReq = QueryRequest.newBuilder().statement("PROFILE RETURN 1").build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(profileReq);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).hasProfiledQueryPlan())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(profileReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasProfiledQueryPlan)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesNRecords(0)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(profileReq, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::hasProfiledQueryPlan)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    @Test
    void shouldNotReturnProfiledQueryPlanByDefault()
            throws IOException, InterruptedException, QueryApiTestClientException {
        var request = QueryRequest.newBuilder().statement("RETURN 1").build();
        var txIdCapture = new Capture<String>();

        var res = testClient.beginTxJsonl(request);
        QueryResponseJsonlAssertions.assertThat(res)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(
                        that -> that.hasTransaction(txIdCapture.capture()).doesNotHaveProfiledQueryPlan())
                .hasNoRemainingEvents();

        var continueBeginRes = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(continueBeginRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var continueRes =
                testClient.runInTxJsonl(request, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(continueRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesNHeaders(1)
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveProfiledQueryPlan)
                .hasNoRemainingEvents();

        var commitBegin = testClient.beginTxJsonl();
        QueryResponseJsonlAssertions.assertThat(commitBegin)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader(QueryResponseJsonlAssertions.HeaderAssertions::doesNotHaveFields)
                .receivesSummary(that -> that.hasTransaction(txIdCapture.capture()))
                .hasNoRemainingEvents();

        var commitRes =
                testClient.commitTxJsonl(request, txIdCapture.getCaptured().getLast());
        QueryResponseJsonlAssertions.assertThat(commitRes)
                .wasSuccessful()
                .hasContentType(QueryContentType.UNTYPED_L)
                .receivesHeader("1")
                .receivesNRecords(1)
                .receivesSummary(QueryResponseJsonlAssertions.SummaryAssertions::doesNotHaveProfiledQueryPlan)
                .hasNoRemainingEvents();

        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(0)))
                .wasSuccessful();
        QueryResponseJsonlAssertions.assertThat(
                        testClient.commitTxJsonl(txIdCapture.getCaptured().get(1)))
                .wasSuccessful();
    }

    void shouldRejectConfigOnSubsequentRequests() throws IOException, InterruptedException {
        // todo this is probably a good idea. We dont want to confuse users that configuring mid tx is possible.
    }
}
