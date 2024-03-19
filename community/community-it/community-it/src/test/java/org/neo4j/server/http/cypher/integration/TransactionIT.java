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
package org.neo4j.server.http.cypher.integration;

import static java.lang.Math.max;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.WebContainerTestUtils.withCSVFile;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasOneErrorOf;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.validRFCTimestamp;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.web.HttpHeaderUtils.ACCESS_MODE_HEADER;
import static org.neo4j.server.web.HttpHeaderUtils.BOOKMARKS_HEADER;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

import com.fasterxml.jackson.databind.JsonNode;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.Socket;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.bolt.tx.TransactionManager;
import org.neo4j.fabric.bolt.QueryRouterBookmark;
import org.neo4j.fabric.bookmark.BookmarkFormat;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.database.Database;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.web.XForwardUtil;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

public class TransactionIT extends AbstractRestFunctionalTestBase {
    private ExecutorService executors;

    private TransactionManager transactionManager;

    @BeforeEach
    public void setUp() {
        executors = Executors.newFixedThreadPool(max(3, Runtime.getRuntime().availableProcessors()));
        transactionManager = resolveDependency(TransactionManager.class);
    }

    @AfterEach
    public void afterEach() {
        // verify TransactionManager's state is reset after each
        assertThat(transactionManager.getTransactionCount()).isEqualTo(0);
        executors.shutdown();
    }

    @Test
    public void begin__execute__commit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");
        assertThat(commitResource).matches(format("http://localhost:\\d+/%s/\\d+/commit", TX_ENDPOINT));
        assertThat(begin.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // execute
        Response execute = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));
        assertThat(execute.status()).isEqualTo(200);
        assertThat(execute.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void begin__execute__rollback() {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        // execute
        POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        // rollback
        Response commit = DELETE(begin.location());

        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction);
    }

    @Test
    public void begin__execute_and_commit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");
        assertThat(commitResource).isEqualTo(begin.location() + "/commit");

        // execute and commit
        Response commit = POST(commitResource, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        assertThat(commit).satisfies(containsNoErrors());
        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void begin_and_execute__commit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = POST(TX_ENDPOINT, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        String commitResource = begin.stringFromContent("commit");

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void begin_and_execute__commit_with_badly_escaped_statement() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM "
                + "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" "
                + "} ] }";

        // begin and execute
        // given statement is badly escaped and it is a client error, thus tx is rolled back at once
        Response begin = POST(TX_ENDPOINT, quotedJson(json));

        String commitResource = begin.stringFromContent("commit");

        // commit fails because tx was rolled back on the previous step
        Response commit = POST(commitResource);

        assertThat(begin.status()).isEqualTo(201);
        assertThat(begin).satisfies(hasErrors(Status.Request.InvalidFormat));

        assertThat(commit.status()).isEqualTo(404);
        assertThat(commit).satisfies(hasErrors(Status.Transaction.TransactionNotFound));

        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction);
    }

    @Test
    public void begin__execute__commit__execute() throws Exception {

        // begin
        Response begin = POST(TX_ENDPOINT);
        String commitResource = begin.stringFromContent("commit");

        // execute
        POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        // commit
        POST(commitResource);

        // execute
        Response execute2 = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        assertThat(execute2.status()).isEqualTo(404);
        assertThat(execute2).satisfies(hasErrors(Status.Transaction.TransactionNotFound));
    }

    @Test
    public void begin_and_execute_and_commit() {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute and commit
        Response begin =
                POST(transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(containsNoErrors());
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void begin_and_execute_and_commit_with_badly_escaped_statement() {

        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM "
                + "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" "
                + "} ] }";
        // begin and execute and commit
        Response begin = POST(transactionCommitUri(), quotedJson(json));

        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(hasErrors(Status.Request.InvalidFormat));
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction);
    }

    @Test
    public void begin__execute_multiple__commit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");

        // execute
        POST(
                begin.location(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' }, " + "{ 'statement': 'CREATE (n)' } ] }"));

        // commit
        Response commit = POST(commitResource);
        assertThat(commit).satisfies(containsNoErrors());
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 2);
    }

    @Test
    public void begin__execute__execute__commit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");

        // execute
        POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        // execute
        POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        // commit
        POST(commitResource);

        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 2);
    }

    @Test
    public void begin_create_two_nodes_delete_one() throws Exception {
        /*
         * This issue was reported from the community. It resulted in a refactoring of the interaction
         * between TxManager and TransactionContexts.
         */

        // GIVEN
        long nodesInDatabaseBeforeTransaction = countNodes();
        Response response1 = POST(
                transactionCommitUri(),
                rawPayload("{ \"statements\" : [{\"statement\" : \"CREATE (n0:DecibelEntity :AlbumGroup{DecibelID : "
                        + "'34a2201b-f4a9-420f-87ae-00a9c691cc5c', Title : 'Dance With Me', "
                        + "ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', "
                        + "OriginalReleaseDate : '2013-01-08', IsCanon : 'False'}) return id(n0)\"}, "
                        + "{\"statement\" : \"CREATE (n1:DecibelEntity :AlbumRelease{DecibelID : "
                        + "'9ed529fa-7c19-11e2-be78-bcaec5bea3c3', Title : 'Dance With Me', "
                        + "ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', LabelName : 'Barsuk "
                        + "Records', "
                        + "FormatNames : 'File', TrackCount : '3', MediaCount : '1', Duration : '460.000000', "
                        + "ReleaseDate : '2013-01-08', ReleaseYear : '2013', ReleaseRegion : 'USA', "
                        + "Cline : 'Barsuk Records', Pline : 'Barsuk Records', CYear : '2013', PYear : '2013', "
                        + "ParentalAdvisory : 'False', IsLimitedEdition : 'False'}) return id(n1)\"}]}"));
        assertEquals(200, response1.status());
        JsonNode everything = jsonNode(response1.rawContent());
        JsonNode result = everything.get("results").get(0);
        long id = result.get("data").get(0).get("row").get(0).asLong();

        // WHEN
        Response response2 = POST(
                transactionCommitUri(),
                rawPayload("{ \"statements\" : [{\"statement\":\"match (n) where id(n) = " + id
                        + " delete n return n\"}]}"));
        assertEquals(200, response2.status());

        // THEN
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void begin__rollback__commit() throws Exception {

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);
        String commitResource = begin.stringFromContent("commit");

        // terminate
        Response interrupt = DELETE(begin.location());
        assertThat(interrupt.status()).isEqualTo(200);

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(404);
    }

    @Test
    public void begin__rollback__execute() {

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        // terminate
        Response interrupt = DELETE(begin.location());
        assertThat(interrupt.status()).isEqualTo(200);

        // execute
        Response execute = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        assertThat(execute.status()).isEqualTo(404);
    }

    @Test
    @Timeout(30)
    public void begin__execute__rollback_concurrently() throws Exception {

        // begin
        final Response begin = POST(TX_ENDPOINT);
        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        Label sharedLockLabel = Label.label("sharedLock");
        POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n:" + sharedLockLabel + ")' } ] }"));

        CountDownLatch nodeLockLatch = new CountDownLatch(1);
        CountDownLatch nodeReleaseLatch = new CountDownLatch(1);

        Future<?> lockerFuture =
                executors.submit(() -> lockNodeWithLabel(sharedLockLabel, nodeLockLatch, nodeReleaseLatch));
        nodeLockLatch.await();

        // execute
        final String executeResource = begin.location();
        final String statement = "MATCH (n:" + sharedLockLabel + ") DELETE n RETURN count(n)";

        final Future<Response> executeFuture = executors.submit(() -> {
            HTTP.Builder requestBuilder = HTTP.withBaseUri(container().getBaseUri());
            Response response = requestBuilder.POST(
                    executeResource, quotedJson("{ 'statements': [ { 'statement': '" + statement + "' } ] }"));
            assertThat(response.status()).isEqualTo(200);
            return response;
        });

        // terminate
        final Future<Response> interruptFuture = executors.submit(() -> {
            waitForStatementExecution(statement);

            Response response = DELETE(executeResource);
            assertThat(response.status()).as(response.toString()).isEqualTo(200);
            nodeReleaseLatch.countDown();
            return response;
        });

        interruptFuture.get();
        lockerFuture.get();
        Response execute = executeFuture.get();
        assertThat(execute)
                .satisfies(hasOneErrorOf(Status.Transaction.Terminated, Status.Transaction.LockClientStopped));

        Response execute2 = POST(executeResource, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));
        assertThat(execute2.status()).isEqualTo(404);
        assertThat(execute2).satisfies(hasErrors(Status.Transaction.TransactionNotFound));
    }

    @Test
    public void status_codes_should_appear_in_response() {

        Response response =
                POST(transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'RETURN $n' } ] }"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response).satisfies(hasErrors(Status.Statement.ParameterMissing));
    }

    @Test
    public void should_return_location_correctly_in_response() throws JsonParseException {

        // begin
        var begin = POST(TX_ENDPOINT);
        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        // run
        var txId = extractTxId(begin);
        var response = POST(
                format("%s/%s", TX_ENDPOINT, txId), quotedJson("{ 'statements': [ { 'statement': 'RETURN 1' } ] }"));
        System.out.println(response);
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.get("commit").toString()).contains(TX_ENDPOINT);

        // commit
        var commit = POST(format("%s/%s/commit", TX_ENDPOINT, txId));
        System.out.println(commit);
        assertThat(commit.status()).isEqualTo(200);
        assertThat(commit.get("commit").toString()).contains(TX_ENDPOINT);
    }

    @Test
    @Timeout(30)
    public void executing_single_statement_in_new_transaction_and_failing_to_read_the_output_should_interrupt()
            throws Exception {

        // given
        long initialNodes = countNodes();
        DatabaseTransactionStats txMonitor = ((GraphDatabaseAPI) graphdb())
                .getDependencyResolver()
                .resolveDependency(DatabaseTransactionStats.class);
        long initialRollBacks = txMonitor.getNumberOfRolledBackTransactions();

        // when sending a request and aborting in the middle of receiving the result
        Socket socket = new Socket("localhost", getLocalHttpPort());
        PrintStream out = new PrintStream(socket.getOutputStream());

        String output = quotedJson(
                        "{ 'statements': [ { 'statement': 'UNWIND range(0, 9999) AS i CREATE (n {i: i}) RETURN n' } ] "
                                + "}")
                .get();
        out.printf("POST /%s/commit HTTP/1.1\r\n", TX_ENDPOINT);
        out.print("Host: localhost:7474\r\n");
        out.print("Content-type: application/json; charset=utf-8\r\n");
        out.print("Content-length: " + output.getBytes().length + "\r\n");
        out.print("\r\n");
        out.print(output);
        out.print("\r\n");

        InputStream inputStream = socket.getInputStream();
        Reader reader = new InputStreamReader(inputStream);

        // Read the first 300 bytes and then close the socket which should trigger a rollback
        int numRead = 0;
        while (numRead < 300) {
            numRead += reader.read(new char[300]);
        }
        socket.close();

        assertEquals(initialNodes, countNodes());

        // Wait for the transaction to finish. If this gets stuck, the test will timeout in 30s
        while (txMonitor.getNumberOfActiveTransactions() > 0) {
            Thread.sleep(100);
        }

        assertEquals(1, txMonitor.getNumberOfRolledBackTransactions() - initialRollBacks);
    }

    @Test
    public void should_include_graph_format_when_requested() throws Exception {

        long initialData = countNodes("Foo");

        // given
        POST(transactionCommitUri(), singleStatement("CREATE (n:Foo:Bar)"));

        // when
        Response response = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN n', 'resultDataContents':['row',"
                        + "'graph'] } ] }"));

        // then
        assertThat(response.status()).isEqualTo(200);
        JsonNode data = response.get("results").get(0).get("data");
        assertTrue(data.isArray(), "data is a list");
        assertEquals(initialData + 1, data.size(), "one entry");
        JsonNode entry = data.get(0);
        assertTrue(entry.has("row"), "entry has row");
        assertTrue(entry.has("graph"), "entry has graph");
        JsonNode nodes = entry.get("graph").get("nodes");
        JsonNode rels = entry.get("graph").get("relationships");
        assertTrue(nodes.isArray(), "nodes is a list");
        assertTrue(rels.isArray(), "relationships is a list");
        assertEquals(1, nodes.size(), "one node");
        assertEquals(0, rels.size(), "no relationships");
        Set<String> labels = new HashSet<>();
        for (JsonNode node : nodes.get(0).get("labels")) {
            labels.add(node.asText());
        }
        assertTrue(labels.size() > 0, "some labels");
    }

    @Test
    public void should_serialize_collect_correctly() throws Exception {

        // given
        POST(transactionCommitUri(), singleStatement("CREATE (n:Foo)"));

        // when
        Response response = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN COLLECT(n)' } ] }"));

        // then
        assertThat(response.status()).isEqualTo(200);

        JsonNode data = response.get("results").get(0);
        assertThat(data.get("columns").get(0).asText()).isEqualTo("COLLECT(n)");
        assertThat(data.get("data").get(0).get("row").size()).isEqualTo(1);
        assertThat(data.get("data").get(0).get("row").get(0).get(0).size()).isEqualTo(0);

        assertThat(response.get("errors").size()).isEqualTo(0);
    }

    @Test
    public void shouldSerializeMapsCorrectlyInRowsFormat() throws Exception {

        Response response = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'RETURN {one:{two:[true, {three: 42}]}}' } ] }"));

        // then
        assertThat(response.status()).isEqualTo(200);

        JsonNode data = response.get("results").get(0);
        JsonNode row = data.get("data").get(0).get("row");
        assertThat(row.size()).isEqualTo(1);
        JsonNode firstCell = row.get(0);
        assertThat(firstCell.get("one").get("two").size()).isEqualTo(2);
        assertThat(firstCell.get("one").get("two").get(0).asBoolean()).isEqualTo(true);
        assertThat(firstCell.get("one").get("two").get(1).get("three").asInt()).isEqualTo(42);

        assertThat(response.get("errors").size()).isEqualTo(0);
    }

    @Test
    public void begin_and_execute_call_in_tx_and_commit() throws Exception {

        int nodes = 11;
        int batch = 2;
        withCSVFile(nodes, url -> {
            Response response;
            long nodesInDatabaseBeforeTransaction;
            long txIdBefore;
            int times = 0;
            do {
                nodesInDatabaseBeforeTransaction = countNodes();
                txIdBefore = resolveDependency(TransactionIdStore.class).getLastClosedTransactionId();

                // begin and execute and commit
                // language=json
                var query = String.format(
                        """
                        {
                          "statements": [
                            {
                              "statement": "LOAD CSV FROM \\"%s\\" AS line CALL { WITH line CREATE() } IN TRANSACTIONS OF %s ROWS"
                            }
                          ]
                        }
                        """,
                        url, batch);

                response = POST(transactionCommitUri(), quotedJson(query));
                times++;

            } while (response.get("errors").iterator().hasNext() && (times < 5));

            long txIdAfter = resolveDependency(TransactionIdStore.class).getLastClosedTransactionId();

            assertThat(response).as("Last response is: " + response).satisfies(containsNoErrors());
            assertThat(response.status()).isEqualTo(200);
            assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + nodes);
            assertThat(txIdAfter).isEqualTo(txIdBefore + ((nodes / batch) + 1));
        });
    }

    @Test
    public void begin_and_execute_call_in_tx_that_returns_data_and_commit() throws Exception {

        int nodes = 11;
        int batchSize = 2;

        // language=json
        var query =
                """
                {
                  "statements": [
                    {
                      "statement": "LOAD CSV FROM \\"%s\\" AS line CALL { WITH line CREATE (n {id1: 23}) RETURN n } IN TRANSACTIONS OF %s ROWS RETURN n"
                    }
                  ]
                }
                """;

        // warm up the CALL {} IN TX
        withCSVFile(nodes, url -> POST(transactionCommitUri(), quotedJson(String.format(query, url, batchSize))));

        withCSVFile(nodes, url -> {
            long nodesInDatabaseBeforeTransaction = countNodes();
            long txIdBefore = resolveDependency(TransactionIdStore.class).getLastClosedTransactionId();

            // begin and execute and commit
            Response response = POST(transactionCommitUri(), quotedJson(String.format(query, url, batchSize)));
            long txIdAfter = resolveDependency(TransactionIdStore.class).getLastClosedTransactionId();

            assertThat(response.status()).isEqualTo(200);

            assertThat(response).satisfies(containsNoErrors());

            JsonNode columns = response.get("results").get(0).get("columns");
            assertThat(columns.toString()).isEqualTo("[\"n\"]");
            assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + nodes);
            long expectedTxCount = (nodes / batchSize) + 1;

            assertThat(txIdAfter - txIdBefore).isEqualTo(expectedTxCount);
        });
    }

    @Test
    public void begin_and_execute_multiple_call_in_tx_last_and_commit() throws Exception {

        // language=json
        var query =
                """
                {
                  "statements": [
                    {
                      "statement": "CREATE ()"
                    },
                    {
                      "statement": "LOAD CSV FROM \\"%s\\" AS line CALL { WITH line CREATE (n {id1: 23}) RETURN n } IN TRANSACTIONS RETURN n"
                    }
                  ]
                }
                """;

        withCSVFile(1, url -> {
            // begin and execute and commit
            Response response = POST(transactionCommitUri(), quotedJson(String.format(query, url)));

            // assertThat(response).satisfies(hasErrors(Status.Statement.ExecutionFailed));
            assertThat(response.get("errors").get(0).get("message").asText())
                    .startsWith("Expected transaction state to be empty when calling transactional subquery");
        });
    }

    @Test
    public void begin_and_execute_call_in_tx_followed_by_another_statement_and_commit() throws Exception {

        // language=json
        var query =
                """
                {
                  "statements": [
                    {
                      "statement": "LOAD CSV FROM \\"%s\\" AS line CALL { WITH line CREATE (n {id1: 23}) RETURN n } IN TRANSACTIONS RETURN n"
                    },
                    {
                      "statement": "RETURN 1"
                    }
                  ]
                }
                                """;

        withCSVFile(1, url -> {
            // begin and execute and commit
            Response response = POST(transactionCommitUri(), quotedJson(String.format(query, url)));

            assertThat(response.status()).isEqualTo(200);
        });
    }

    @Test
    public void begin_and_execute_invalid_query_and_commit() {

        // begin and execute and commit
        Response response =
                POST(transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'MATCH n RETURN m' } ] }"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response).satisfies(hasErrors(Status.Statement.SyntaxError));
    }

    @Test
    public void shouldSerializeMapsCorrectlyInRestFormat() throws Exception {

        Response response = POST(
                transactionCommitUri(),
                quotedJson(
                        "{ 'statements': [ { 'statement': 'RETURN {one:{two:[true, {three: 42}]}}', 'resultDataContents':['rest'] } ] }"));

        // then
        assertThat(response.status()).isEqualTo(200);

        JsonNode data = response.get("results").get(0);
        JsonNode rest = data.get("data").get(0).get("rest");
        assertThat(rest.size()).isEqualTo(1);
        JsonNode firstCell = rest.get(0);
        assertThat(firstCell.get("one").get("two").size()).isEqualTo(2);
        assertThat(firstCell.get("one").get("two").get(0).asBoolean()).isEqualTo(true);
        assertThat(firstCell.get("one").get("two").get(1).get("three").asInt()).isEqualTo(42);

        assertThat(response.get("errors").size()).isEqualTo(0);
    }

    @Test
    public void shouldHandleMapParametersCorrectly() throws Exception {

        Response response = POST(
                transactionCommitUri(),
                quotedJson(
                        "{ 'statements': [ { 'statement': "
                                + "'WITH $map AS map RETURN map[0]', 'parameters':{'map':[{'index':0,'name':'a'},{'index':1,'name':'b'}]} } ] }"));

        // then
        assertThat(response.status()).isEqualTo(200);

        JsonNode data = response.get("results").get(0);
        JsonNode row = data.get("data").get(0).get("row");
        assertThat(row.size()).isEqualTo(1);

        assertThat(row.get(0).get("index").asInt()).isEqualTo(0);
        assertThat(row.get(0).get("name").asText()).isEqualTo("a");

        assertThat(response.get("errors").size()).isEqualTo(0);
    }

    @Test
    public void restFormatNodesShouldHaveSensibleUris() throws Exception {

        // given
        final String hostname = "localhost";

        // when
        Response rs = POST(
                transactionCommitUri(),
                quotedJson(
                        "{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', 'resultDataContents':['rest'] } ] }"));

        // then
        JsonNode restNode =
                rs.get("results").get(0).get("data").get(0).get("rest").get(0);

        assertPath(restNode.get("labels"), "/node/\\d+/labels", hostname);
        assertPath(restNode.get("outgoing_relationships"), "/node/\\d+/relationships/out", hostname);
        assertPath(restNode.get("traverse"), "/node/\\d+/traverse/\\{returnType\\}", hostname);
        assertPath(
                restNode.get("all_typed_relationships"),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}",
                hostname);
        assertPath(restNode.get("self"), "/node/\\d+", hostname);
        assertPath(restNode.get("property"), "/node/\\d+/properties/\\{key\\}", hostname);
        assertPath(restNode.get("properties"), "/node/\\d+/properties", hostname);
        assertPath(
                restNode.get("outgoing_typed_relationships"),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}",
                hostname);
        assertPath(restNode.get("incoming_relationships"), "/node/\\d+/relationships/in", hostname);
        assertPath(restNode.get("create_relationship"), "/node/\\d+/relationships", hostname);
        assertPath(
                restNode.get("paged_traverse"),
                "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," + "leaseTime\\}",
                hostname);
        assertPath(restNode.get("all_relationships"), "/node/\\d+/relationships/all", hostname);
        assertPath(
                restNode.get("incoming_typed_relationships"),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}",
                hostname);
    }

    @Test
    public void restFormattedNodesShouldHaveSensibleUrisWhenUsingXForwardHeader() throws Exception {

        // given
        final String hostname = "dummy.example.org";

        // when
        Response rs = http.withHeaders(XForwardUtil.X_FORWARD_HOST_HEADER_KEY, hostname)
                .POST(
                        transactionCommitUri(),
                        quotedJson("{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', "
                                + "'resultDataContents':['rest'] } ] }"));

        // then
        JsonNode restNode =
                rs.get("results").get(0).get("data").get(0).get("rest").get(0);

        assertPath(restNode.get("labels"), "/node/\\d+/labels", hostname);
        assertPath(restNode.get("outgoing_relationships"), "/node/\\d+/relationships/out", hostname);
        assertPath(restNode.get("traverse"), "/node/\\d+/traverse/\\{returnType\\}", hostname);
        assertPath(
                restNode.get("all_typed_relationships"),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}",
                hostname);
        assertPath(restNode.get("self"), "/node/\\d+", hostname);
        assertPath(restNode.get("property"), "/node/\\d+/properties/\\{key\\}", hostname);
        assertPath(restNode.get("properties"), "/node/\\d+/properties", hostname);
        assertPath(
                restNode.get("outgoing_typed_relationships"),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}",
                hostname);
        assertPath(restNode.get("incoming_relationships"), "/node/\\d+/relationships/in", hostname);
        assertPath(restNode.get("create_relationship"), "/node/\\d+/relationships", hostname);
        assertPath(
                restNode.get("paged_traverse"),
                "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," + "leaseTime\\}",
                hostname);
        assertPath(restNode.get("all_relationships"), "/node/\\d+/relationships/all", hostname);
        assertPath(
                restNode.get("incoming_typed_relationships"),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}",
                hostname);
    }

    @Test
    public void correctStatusCodeWhenUsingHintWithoutAnyIndex() {

        // begin and execute and commit
        Response begin = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': "
                        + "'MATCH (n:Test) USING INDEX n:Test(foo) WHERE n.foo = 42 RETURN n.foo' } ] }"));
        assertThat(begin).satisfies(hasErrors(Status.Request.Schema.IndexNotFound));
    }

    @Test
    public void transaction_not_in_response_on_failure() throws Exception {

        // begin
        Response begin = POST(TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");

        // execute valid statement
        Response valid = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'RETURN 42' } ] }"));
        assertThat(valid.status()).isEqualTo(200);
        assertThat(valid.get("transaction")).isNotNull();

        // execute invalid statement
        Response invalid = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'RETRUN 42' } ] }"));
        assertThat(invalid.status()).isEqualTo(200);
        // transaction has been closed and rolled back
        assertThat(invalid.get("transaction")).isNull();

        // commit
        Response commit = POST(commitResource);

        // no transaction open anymore, we have failed
        assertThat(commit.status()).isEqualTo(404);
    }

    @Test
    public void shouldWorkWhenHittingTheASTCacheInCypher() throws JsonParseException {

        // give a cached plan
        Response response =
                POST(transactionCommitUri(), singleStatement("MATCH (group:Group {name: \\\"AAA\\\"}) RETURN *"));

        assertThat(response.status()).isEqualTo(200);
        assertThat(response.get("errors").size()).isEqualTo(0);

        // when we hit the ast cache
        response = POST(transactionCommitUri(), singleStatement("MATCH (group:Group {name: \\\"BBB\\\"}) RETURN *"));

        // then no errors (in particular no NPE)
        assertThat(response.status()).isEqualTo(200);
        assertThat(response.get("errors").size()).isEqualTo(0);
    }

    @Test
    public void writeSettingsBeginAndCommit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = POST(
                TX_ENDPOINT,
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                Map.of(ACCESS_MODE_HEADER, "WRITE"));

        String commitResource = begin.stringFromContent("commit");

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void shouldErrorWhenWriteAttemptedWithReadSetting() throws Exception {

        // begin and execute and commit
        Response begin = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                Map.of(ACCESS_MODE_HEADER, "READ"));

        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(hasErrors(Status.Statement.AccessMode));
    }

    @Test
    public void readSettingsBeginAndCommit() throws Exception {

        // begin and execute
        Response begin = POST(
                TX_ENDPOINT,
                quotedJson("{'statements': [ { 'statement': 'MATCH (n) RETURN n' } ] }"),
                Map.of(ACCESS_MODE_HEADER, "READ"));

        String commitResource = begin.stringFromContent("commit");

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
    }

    @Test
    public void beginWithSettingsOnlyAndThenExecuteCommit() throws Exception {

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST(TX_ENDPOINT, quotedJson(""), Map.of(ACCESS_MODE_HEADER, "WRITE"));

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");
        assertThat(commitResource).matches(format("http://localhost:\\d+/%s/\\d+/commit", TX_ENDPOINT));
        assertThat(begin.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // execute
        Response execute = POST(begin.location(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));
        assertThat(execute.status()).isEqualTo(200);
        assertThat(execute.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
        assertThat(countNodes()).isEqualTo(nodesInDatabaseBeforeTransaction + 1);
    }

    @Test
    public void shouldIgnoreAccessModeHeaderOnSecondRequest() throws Exception {

        // begin
        Response begin = POST(TX_ENDPOINT, quotedJson(""), Map.of(ACCESS_MODE_HEADER, "READ"));

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");
        assertThat(commitResource).matches(format("http://localhost:\\d+/%s/\\d+/commit", TX_ENDPOINT));
        assertThat(begin.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // execute
        Response execute = POST(
                begin.location(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                Map.of(ACCESS_MODE_HEADER, "WRITE"));
        assertThat(execute.status()).isEqualTo(200);
        assertThat(execute).satisfies(hasErrors(Status.Statement.AccessMode));
    }

    @Test
    public void shouldErrorWithInvalidAccessModeHeader() {
        // begin
        Response begin = POST(TX_ENDPOINT, quotedJson(""), Map.of(ACCESS_MODE_HEADER, "INVALID!"));
        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(hasErrors(Status.Request.InvalidFormat));
    }

    @ParameterizedTest
    @ValueSource(strings = {"boooookyMark", "64, 12", "FB:boom!"})
    public void shouldRejectInvalidBookmarks(String bookmarkInput) throws JsonParseException {

        // begin
        Response begin = POST(TX_ENDPOINT, quotedJson(""), Map.of(BOOKMARKS_HEADER, bookmarkInput));

        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(hasErrors(Status.Request.InvalidFormat));
        assertThat(begin.get("errors").get(0).get("message").asText())
                .startsWith("Invalid bookmarks header. " + "`bookmarks` must be an array of non-empty string values.");
    }

    @Test
    public void shouldReturnUpdatedBookmarkAfterSingleRequestTransaction() throws JsonParseException {
        var initialBookmark = POST(
                        transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"))
                .get("lastBookmarks")
                .get(0)
                .asText();

        var beginWithBookmark = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(initialBookmark));

        assertThat(beginWithBookmark.status()).isEqualTo(200);
        assertThat(beginWithBookmark).satisfies(containsNoErrors());
        assertThat(beginWithBookmark.get("lastBookmarks").asText()).isNotEqualTo(initialBookmark);
    }

    @Test
    public void shouldAcceptMultipleBookmarks() throws JsonParseException {
        var initialBookmarkA = POST(
                        transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"))
                .get("lastBookmarks")
                .get(0)
                .asText();
        var initialBookmarkB = POST(
                        transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"))
                .get("lastBookmarks")
                .get(0)
                .asText();

        var beginWithBookmark = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(initialBookmarkA, initialBookmarkB));

        assertThat(beginWithBookmark.status()).isEqualTo(200);
        assertThat(beginWithBookmark).satisfies(containsNoErrors());
        assertThat(beginWithBookmark.get("lastBookmarks").get(0).asText()).isNotEqualTo(initialBookmarkA);
        assertThat(beginWithBookmark.get("lastBookmarks").get(0).asText()).isNotEqualTo(initialBookmarkB);
    }

    @Test
    public void shouldReturnUpdatedBookmarkAfterExplicitTransaction() throws JsonParseException {
        var initialBookmark = POST(
                        transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"))
                .get("lastBookmarks")
                .get(0)
                .asText();

        // begin
        Response begin = POST(TX_ENDPOINT, quotedJson(""), bookmarkHeader(initialBookmark));

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");
        assertThat(commitResource).isEqualTo(begin.location() + "/commit");

        // execute and commit
        Response commit = POST(commitResource, quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        assertThat(commit).satisfies(containsNoErrors());
        assertThat(commit.status()).isEqualTo(200);
        assertThat(commit.get("lastBookmarks").get(0).asText()).isNotEqualTo(initialBookmark);
    }

    @Test
    void shouldFailForUnreachableBookmark() {
        var lastClosedTransactionId = getLastClosedTransactionId();

        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        resolveDependency(Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        lastClosedTransactionId + 1)),
                List.of()));

        Response begin = POST(
                TX_ENDPOINT,
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(expectedBookmark));

        assertThat(begin.status()).isEqualTo(201);
        assertThat(begin).satisfies(hasErrors(Status.Transaction.BookmarkTimeout));
    }

    @Test
    void bookmarksAreIgnoredOnMidTransactionRequests() throws JsonParseException {
        var bookmark = POST(transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"))
                .get("lastBookmarks")
                .get(0)
                .asText();

        // begin
        Response begin = POST(TX_ENDPOINT);

        assertThat(begin.status()).isEqualTo(201);
        assertHasTxLocation(begin, TX_ENDPOINT);

        String commitResource = begin.stringFromContent("commit");

        // execute
        Response execute = POST(
                begin.location(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(bookmark));
        assertThat(execute.status()).isEqualTo(200);
        assertThat(execute.get("transaction").get("expires").asText()).satisfies(validRFCTimestamp());

        // commit
        Response commit = POST(commitResource);

        assertThat(commit.status()).isEqualTo(200);
        assertThat(commit.get("lastBookmarks").get(0).asText()).isNotBlank();
    }

    @Test
    public void shouldWaitForUpdatedBookmark() {
        var lastClosedTransactionId = getLastClosedTransactionId();

        var expectedBookmark = BookmarkFormat.serialize(new QueryRouterBookmark(
                List.of(new QueryRouterBookmark.InternalGraphState(
                        resolveDependency(Database.class)
                                .getNamedDatabaseId()
                                .databaseId()
                                .uuid(),
                        lastClosedTransactionId + 1)),
                List.of()));

        var begin = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(expectedBookmark));

        assertThat(begin.status()).isEqualTo(200);
        assertThat(begin).satisfies(hasErrors(Status.Transaction.BookmarkTimeout));

        // move the state forward one so bookmark becomes reachable
        POST(transactionCommitUri(), quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"));

        Response begin2 = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"),
                bookmarkHeader(expectedBookmark));

        assertThat(begin2.status()).isEqualTo(200);
        assertThat(begin2).satisfies(containsNoErrors());
    }

    @Test
    public void begin_and_execute_multiple_statements_with_partial_rollback() throws Exception {
        var nodesAtStart = countNodes();

        // language=json
        var query =
                """
                {
                  "statements": [
                    {
                      "statement": "UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i"
                    },
                    {
                      "statement": "CREATE ()"
                    },
                    {
                      "statement": "CREATE ()"
                    },
                    {
                      "statement": "BROKEN"
                    }
                  ]
                }
                                """;

        HTTP.Response response = POST(transactionCommitUri(), quotedJson(query));

        // Expect statements outside CALL IN TX not to be rolled back but other statements to be rolled back
        assertThat(response).satisfies(hasErrors(Status.Statement.SyntaxError));
        assertThat(countNodes()).isEqualTo(nodesAtStart + 4);
    }

    @Test
    public void begin_and_execute_multiple_statements_successfully() throws Exception {
        var nodesAtStart = countNodes();

        // language=json
        var query =
                """
                {
                  "statements": [
                    {
                      "statement": "UNWIND [4, 2, 1, 0] AS i CALL { WITH i CREATE ()} IN TRANSACTIONS OF 2 ROWS RETURN i"
                    },
                    {
                      "statement": "CREATE ()"
                    },
                    {
                      "statement": "CREATE ()"
                    }
                  ]
                }
                """;

        HTTP.Response response = POST(transactionCommitUri(), quotedJson(query.replace("\n", "")));

        // Expect statements outside CALL IN TX not to be rolled back but other statements to be rolled back
        assertThat(response).satisfies(containsNoErrors());
        assertThat(countNodes()).isEqualTo(nodesAtStart + 6);
    }

    private String transactionCommitUri() {
        return format("%s/commit", TX_ENDPOINT);
    }

    private String databaseName() {
        return TX_ENDPOINT.split("/")[1]; // either data or neo4j
    }

    private void assertPath(JsonNode jsonURIString, String path, String hostname) {
        var databaseName = databaseName();
        var expected = String.format("http://%s:\\d+/db/%s%s", hostname, databaseName, path);
        assertTrue(
                jsonURIString.asText().matches(expected),
                String.format("Expected a uri matching '%s', but got '%s'.", expected, jsonURIString.asText()));
    }

    private static HTTP.RawPayload singleStatement(String statement) {
        return rawPayload("{\"statements\":[{\"statement\":\"" + statement + "\"}]}");
    }

    private long countNodes(String... labels) {
        Set<Label> givenLabels = new HashSet<>(labels.length);
        for (String label : labels) {
            givenLabels.add(Label.label(label));
        }

        final GraphDatabaseService graphdb = graphdb();
        try (Transaction transaction = graphdb.beginTx()) {
            long count = 0;
            try (ResourceIterable<Node> allNodes = transaction.getAllNodes()) {
                for (Node node : allNodes) {
                    Set<Label> nodeLabels = Iterables.asSet(node.getLabels());
                    if (nodeLabels.containsAll(givenLabels)) {
                        count++;
                    }
                }
            }
            transaction.commit();
            return count;
        }
    }

    private void lockNodeWithLabel(
            Label sharedLockLabel, CountDownLatch nodeLockLatch, CountDownLatch nodeReleaseLatch) {
        GraphDatabaseService db = graphdb();
        try (Transaction tx = db.beginTx();
                ResourceIterator<Node> nodes = tx.findNodes(sharedLockLabel)) {
            Node node = nodes.next();
            node.setProperty("a", "b");
            nodeLockLatch.countDown();
            nodeReleaseLatch.await();
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private static void waitForStatementExecution(String statement) {
        var server = container();
        var databaseService = server.getDefaultDatabase();
        var kernelTransactions = databaseService.getDependencyResolver().resolveDependency(KernelTransactions.class);
        while (!isStatementExecuting(kernelTransactions, statement)) {
            Thread.yield();
        }
    }

    private static boolean isStatementExecuting(KernelTransactions kernelTransactions, String statement) {
        return kernelTransactions.activeTransactions().stream()
                .flatMap(k -> k.executingQuery().stream())
                .anyMatch(executingQuery -> statement.equals(executingQuery.rawQueryText()));
    }

    private static Map<String, String> bookmarkHeader(String... bookmark) {
        var sb = new StringBuilder();

        sb.append("[");
        for (var iter = Arrays.stream(bookmark).iterator(); iter.hasNext(); ) {
            sb.append("\"");
            sb.append(iter.next());
            sb.append("\"");
            if (iter.hasNext()) {
                sb.append(",");
            }
        }
        sb.append("]");

        return Map.of(BOOKMARKS_HEADER, sb.toString());
    }

    public long getLastClosedTransactionId() {
        var txIdStore = resolveDependency(TransactionIdStore.class);
        return txIdStore.getLastClosedTransactionId();
    }
}
