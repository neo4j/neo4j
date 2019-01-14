/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.server.rest.transactional.integration;

import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.Socket;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.helpers.collection.Iterables;
import org.neo4j.kernel.api.KernelTransactionHandle;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.transaction.TransactionStats;
import org.neo4j.kernel.impl.transaction.log.TransactionIdStore;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.ServerTestUtils;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.web.XForwardUtil;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static java.lang.Math.max;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.notNullValue;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.isValidRFCTimestamp;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.matches;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class TransactionIT extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( server().baseUri() );
    private ExecutorService executors;

    @Before
    public void setUp()
    {
        executors = Executors.newFixedThreadPool( max( 3, Runtime.getRuntime().availableProcessors() ) );
    }

    @After
    public void tearDown()
    {
        executors.shutdown();
    }

    @Test
    public void begin__execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource, matches( "http://localhost:\\d+/db/data/transaction/\\d+/commit" ) );
        assertThat( begin.get( "transaction" ).get( "expires" ).asText(), isValidRFCTimestamp() );

        // execute
        Response execute =
                http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertThat( execute.status(), equalTo( 200 ) );
        assertThat( execute.get( "transaction" ).get( "expires" ).asText(), isValidRFCTimestamp() );

        // commit
        Response commit = http.POST( commitResource );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin__execute__rollback()
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // rollback
        Response commit = http.DELETE( begin.location() );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin__execute_and_commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource, equalTo( begin.location() + "/commit" ) );

        // execute and commit
        Response commit = http.POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }"
        ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin_and_execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = http.POST( "db/data/transaction",
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit
        Response commit = http.POST( commitResource );

        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin_and_execute__commit_with_badly_escaped_statement() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM " +
                      "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" " +
                      "} ] }";

        // begin and execute
        // given statement is badly escaped and it is a client error, thus tx is rolled back at once
        Response begin = http.POST( "db/data/transaction", quotedJson( json ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit fails because tx was rolled back on the previous step
        Response commit = http.POST( commitResource );

        assertThat( begin.status(), equalTo( 201 ) );
        assertThat( begin, hasErrors( Status.Request.InvalidFormat ) );

        assertThat( commit.status(), equalTo( 404 ) );
        assertThat( commit, hasErrors( Status.Transaction.TransactionNotFound ) );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin__execute__commit__execute() throws Exception
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );
        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // commit
        http.POST( commitResource );

        // execute
        Response execute2 = http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ]" +
                                                                     " }" ) );

        assertThat( execute2.status(), equalTo( 404 ) );
        assertThat( execute2, hasErrors( Status.Transaction.TransactionNotFound ) );
    }

    @Test
    public void begin_and_execute_and_commit()
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute and commit
        Response begin = http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( begin.status(), equalTo( 200 ) );
        assertThat( begin, containsNoErrors() );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void returned_rest_urls_must_be_useable() throws Exception
    {
        // begin and execute and commit "resultDataContents":["REST"]
        HTTP.RawPayload payload = quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n {a: 1}) return n', " +
                                              "'resultDataContents' : ['REST'] } ] }" );
        Response begin = http.POST( "db/data/transaction/commit", payload );

        assertThat( begin.status(), equalTo( 200 ) );
        JsonNode results = begin.get( "results" );
        JsonNode result = results.get( 0 );
        JsonNode data = result.get( "data" );
        JsonNode firstDataSegment = data.get( 0 );
        JsonNode restData = firstDataSegment.get( "rest" );
        JsonNode firstRestSegment = restData.get( 0 );
        String propertiesUri = firstRestSegment.get( "properties" ).asText();

        Response propertiesResponse = http.GET( propertiesUri );
        assertThat( propertiesResponse.status(), is( 200 ) );
    }

    @Test
    public void begin_and_execute_and_commit_with_badly_escaped_statement()
    {
        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM " +
                      "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" " +
                      "} ] }";
        // begin and execute and commit
        Response begin = http.POST( "db/data/transaction/commit", quotedJson( json ) );

        assertThat( begin.status(), equalTo( 200 ) );
        assertThat( begin, hasErrors( Status.Request.InvalidFormat ) );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction ) );
    }

    @Test
    public void begin_and_execute_periodic_commit_and_commit() throws Exception
    {
        int nodes = 11;
        int batch = 2;
        ServerTestUtils.withCSVFile( nodes, url ->
        {
            Response response;
            long nodesInDatabaseBeforeTransaction;
            long txIdBefore;
            int times = 0;
            do
            {
                nodesInDatabaseBeforeTransaction = countNodes();
                txIdBefore = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

                // begin and execute and commit

                response = http.POST(
                        "db/data/transaction/commit",
                        quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT " + batch + " LOAD CSV FROM " +
                                "\\\"" + url + "\\\" AS line CREATE ()' } ] }" )
                );
                times++;
            }
            while ( response.get( "errors" ).iterator().hasNext() && (times < 5) );

            long txIdAfter = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            assertThat( "Last response is: " + response, response, containsNoErrors() );
            assertThat( response.status(), equalTo( 200 ) );
            assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + nodes ) );
            assertThat( txIdAfter, equalTo( txIdBefore + ((nodes / batch) + 1) ) );
        } );
    }

    @Test
    public void begin_and_execute_periodic_commit_that_returns_data_and_commit() throws Exception
    {
        int nodes = 11;
        int batch = 2;
        ServerTestUtils.withCSVFile( nodes, url ->
        {
            long nodesInDatabaseBeforeTransaction = countNodes();
            long txIdBefore = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            // begin and execute and commit
            Response response = http.POST(
                    "db/data/transaction/commit",
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT " + batch + " LOAD CSV FROM " +
                            "\\\"" + url + "\\\" AS line CREATE (n {id: 23}) RETURN n' } ] }" )
            );
            long txIdAfter = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            assertThat( response.status(), equalTo( 200 ) );

            assertThat( response, containsNoErrors() );

            JsonNode columns = response.get( "results" ).get( 0 ).get( "columns" );
            assertThat( columns.toString(), equalTo( "[\"n\"]" ) );
            assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + nodes ) );
            assertThat( txIdAfter, equalTo( txIdBefore + ((nodes / batch) + 1) ) );
        } );
    }

    @Test
    public void begin_and_execute_cypher_23_periodic_commit_that_returns_data_and_commit() throws Exception
    {
        // to get rid off the property key id creation in the actual test
        try ( Transaction tx = graphdb().beginTx() )
        {
            Node node = graphdb().createNode();
            node.setProperty( "id", 42 );
        }

        int nodes = 11;
        int batch = 2;
        ServerTestUtils.withCSVFile( nodes, url ->
        {
            long nodesInDatabaseBeforeTransaction = countNodes();
            long txIdBefore = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            // begin and execute and commit
            Response response = http.POST(
                    "db/data/transaction/commit",
                    quotedJson( "{ 'statements': [ { 'statement': 'CYPHER 2.3 USING PERIODIC COMMIT " + batch +
                            " LOAD CSV FROM" + " \\\"" + url + "\\\" AS line CREATE (n {id: 23}) RETURN n' } ] }" )
            );
            long txIdAfter = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            assertThat( response.status(), equalTo( 200 ) );

            assertThat( response, containsNoErrors() );

            JsonNode columns = response.get( "results" ).get( 0 ).get( "columns" );
            assertThat( columns.toString(), equalTo( "[\"n\"]" ) );
            assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + nodes ) );
            assertThat( txIdAfter, equalTo( txIdBefore + ((nodes / batch) + 1) ) );
        } );
    }

    @Test
    public void begin_and_execute_periodic_commit_followed_by_another_statement_and_commit() throws Exception
    {
        ServerTestUtils.withCSVFile( 1, url ->
        {
            // begin and execute and commit
            Response response = http.POST(
                    "db/data/transaction/commit",
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT LOAD CSV FROM \\\"" +
                                url +
                                "\\\" AS line CREATE (n {id: 23}) RETURN n' }, { 'statement': 'RETURN 1' } ] }" )
            );

            assertThat( response.status(), equalTo( 200 ) );
            assertThat( response, hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @Test
    public void begin_and_execute_invalid_query_and_commit()
    {
        // begin and execute and commit
        Response response = http.POST(
                "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'MATCH n RETURN m' } ] }" )
        );

        assertThat( response.status(), equalTo( 200 ) );
        assertThat( response, hasErrors( Status.Statement.SyntaxError ) );
    }

    @Test
    public void begin_and_execute_multiple_periodic_commit_last_and_commit() throws Exception
    {
        ServerTestUtils.withCSVFile( 1, url ->
        {
            // begin and execute and commit
            Response response = http.POST(
                    "db/data/transaction/commit",
                    quotedJson( "{ 'statements': [ { 'statement': 'CREATE ()' }, " +
                                "{ 'statement': 'USING PERIODIC COMMIT LOAD CSV FROM \\\"" + url + "\\\" AS line " +
                                "CREATE ()' } ] }" )
            );

            assertThat( response, hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @Test
    public void begin__execute__execute_and_periodic_commit() throws Exception
    {
        ServerTestUtils.withCSVFile( 1, url ->
        {
            // begin
            Response begin = http.POST( "db/data/transaction" );

            // execute
            http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE ()' } ] }" ) );

            // execute
            Response response = http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'USING" +
                                                                         " PERIODIC COMMIT LOAD CSV FROM \\\"" +
                                                                         url + "\\\" AS line CREATE ()' } ] }" ) );

            assertThat( response, hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @Test
    public void begin_and_execute_periodic_commit__commit() throws Exception
    {
        ServerTestUtils.withCSVFile( 1, url ->
        {
            // begin and execute
            Response begin = http.POST(
                    "db/data/transaction",
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT LOAD CSV FROM \\\"" +
                                url + "\\\" AS line CREATE ()' } ] }" )
            );

            assertThat( begin, hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @Test
    public void begin__execute_multiple__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "db/data/transaction" );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' }, " +
                                                 "{ 'statement': 'CREATE (n)' } ] }" ) );

        // commit
        Response commit = http.POST( commitResource );
        assertThat( commit, containsNoErrors() );
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 2 ) );
    }

    @Test
    public void begin__execute__execute__commit() throws Exception
    {
        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = http.POST( "db/data/transaction" );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ]" +
                                                 " }" ) );

        // execute
        http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ]" +
                                                 " }" ) );

        // commit
        http.POST( commitResource );

        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 2 ) );
    }

    @Test
    public void begin_create_two_nodes_delete_one() throws Exception
    {
        /*
         * This issue was reported from the community. It resulted in a refactoring of the interaction
         * between TxManager and TransactionContexts.
         */

        // GIVEN
        long nodesInDatabaseBeforeTransaction = countNodes();
        Response response = http.POST( "db/data/transaction/commit",
                rawPayload( "{ \"statements\" : [{\"statement\" : \"CREATE (n0:DecibelEntity :AlbumGroup{DecibelID : " +
                            "'34a2201b-f4a9-420f-87ae-00a9c691cc5c', Title : 'Dance With Me', " +
                            "ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', " +
                            "OriginalReleaseDate : '2013-01-08', IsCanon : 'False'}) return id(n0)\"}, " +
                            "{\"statement\" : \"CREATE (n1:DecibelEntity :AlbumRelease{DecibelID : " +
                            "'9ed529fa-7c19-11e2-be78-bcaec5bea3c3', Title : 'Dance With Me', " +
                            "ArtistString : 'Ra Ra Riot', MainArtistAlias : 'Ra Ra Riot', LabelName : 'Barsuk " +
                            "Records', " +
                            "FormatNames : 'File', TrackCount : '3', MediaCount : '1', Duration : '460.000000', " +
                            "ReleaseDate : '2013-01-08', ReleaseYear : '2013', ReleaseRegion : 'USA', " +
                            "Cline : 'Barsuk Records', Pline : 'Barsuk Records', CYear : '2013', PYear : '2013', " +
                            "ParentalAdvisory : 'False', IsLimitedEdition : 'False'}) return id(n1)\"}]}" ) );
        assertEquals( 200, response.status() );
        JsonNode everything = jsonNode( response.rawContent() );
        JsonNode result = everything.get( "results" ).get( 0 );
        long id = result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).getLongValue();

        // WHEN
        http.POST( "db/data/cypher", rawPayload( "{\"query\":\"match (n) where id(n) = " + id + " delete n\"}" ) );

        // THEN
        assertThat( countNodes(), equalTo( nodesInDatabaseBeforeTransaction + 1 ) );
    }

    @Test
    public void begin__rollback__commit() throws Exception
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );
        String commitResource = begin.stringFromContent( "commit" );

        // terminate
        Response interrupt = http.DELETE( begin.location() );
        assertThat( interrupt.status(), equalTo( 200 ) );

        // commit
        Response commit = http.POST( commitResource );

        assertThat( commit.status(), equalTo( 404 ) );
    }

    @Test
    public void begin__rollback__execute()
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );

        // terminate
        Response interrupt = http.DELETE( begin.location() );
        assertThat( interrupt.status(), equalTo( 200 ) );

        // execute
        Response execute =
                http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( execute.status(), equalTo( 404 ) );
    }

    @Test( timeout = 30_000 )
    public void begin__execute__rollback_concurrently() throws Exception
    {
        // begin
        final Response begin = http.POST( "db/data/transaction" );
        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );

        Label sharedLockLabel = Label.label( "sharedLock" );
        http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n:" + sharedLockLabel + ")' } ] }" ) );

        CountDownLatch nodeLockLatch = new CountDownLatch( 1 );
        CountDownLatch nodeReleaseLatch = new CountDownLatch( 1 );

        Future<?> lockerFuture = executors.submit( () -> lockNodeWithLabel( sharedLockLabel, nodeLockLatch, nodeReleaseLatch ) );
        nodeLockLatch.await();

        // execute
        final String executeResource = begin.location();
        final String statement = "MATCH (n:" + sharedLockLabel + ") DELETE n RETURN count(n)";

        final Future<Response> executeFuture = executors.submit( () ->
        {
            HTTP.Builder requestBuilder = HTTP.withBaseUri( server().baseUri() );
            Response response = requestBuilder.POST( executeResource, quotedJson( "{ 'statements': [ { 'statement': '" +
                                                                        statement + "' } ] }" ) );
            assertThat( response.status(), equalTo( 200 ) );
            return response;
        } );

        // terminate
        final Future<Response> interruptFuture = executors.submit( () ->
        {
            waitForStatementExecution( statement );

            Response response = http.DELETE( executeResource );
            assertThat( response.toString(), response.status(), equalTo( 200 ) );
            nodeReleaseLatch.countDown();
            return response;
        } );

        interruptFuture.get();
        lockerFuture.get();
        Response execute = executeFuture.get();
        assertThat( execute, hasErrors( Status.Statement.ExecutionFailed ) );

        Response execute2 =
                http.POST( executeResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertThat( execute2.status(), equalTo( 404 ) );
        assertThat( execute2, hasErrors( Status.Transaction.TransactionNotFound ) );
    }

    @Test
    public void status_codes_should_appear_in_response()
    {
        Response response = http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': 'RETURN {n}' } ] }" ) );

        assertThat( response.status(), equalTo( 200 ) );
        assertThat( response, hasErrors( Status.Statement.ParameterMissing ) );
    }

    @Test
    public void executing_single_statement_in_new_transaction_and_failing_to_read_the_output_should_interrupt()
            throws Exception
    {
        // given
        long initialNodes = countNodes();
        TransactionStats txMonitor = ((GraphDatabaseAPI) graphdb()).getDependencyResolver().resolveDependency(
                TransactionStats.class );
        long initialTerminations = txMonitor.getNumberOfTerminatedTransactions();

        // when sending a request and aborting in the middle of receiving the result
        Socket socket = new Socket( "localhost", getLocalHttpPort() );
        PrintStream out = new PrintStream( socket.getOutputStream() );

        String output = quotedJson(
                "{ 'statements': [ { 'statement': 'WITH * UNWIND range(0, 9999) AS i CREATE (n {i: i}) RETURN n' } ] " +
                "}" ).get();
        out.print( "POST /db/data/transaction/commit HTTP/1.1\r\n" );
        out.print( "Host: localhost:7474\r\n" );
        out.print( "Content-type: application/json; charset=utf-8\r\n" );
        out.print( "Content-length: " + output.getBytes().length + "\r\n" );
        out.print( "\r\n" );
        out.print( output );
        out.print( "\r\n" );

        InputStream inputStream = socket.getInputStream();
        Reader reader = new InputStreamReader( inputStream );

        int numRead = 0;
        while ( numRead < 300 )
        {
            numRead += reader.read( new char[300] );
        }
        socket.close();

        try ( Transaction ignored = graphdb().beginTx() )
        {
            assertEquals( initialNodes, countNodes() );
        }

        // then soon the transaction should have been terminated
        long endTime = System.currentTimeMillis() + 5000;
        long additionalTerminations = 0;

        while ( true )
        {
            additionalTerminations = txMonitor.getNumberOfTerminatedTransactions() - initialTerminations;

            if ( additionalTerminations > 0 || System.currentTimeMillis() > endTime )
            {
                break;
            }

            Thread.sleep( 100 );
        }

        assertEquals( 1, additionalTerminations );
    }

    @Test
    public void should_include_graph_format_when_requested() throws Exception
    {
        long initialData = countNodes( "Foo" );

        // given
        http.POST( "db/data/transaction/commit", singleStatement( "CREATE (n:Foo:Bar)" ) );

        // when
        Response response = http.POST( "db/data/transaction/commit", quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN n', 'resultDataContents':['row'," +
                "'graph'] } ] }" ) );

        // then
        assertThat( response.status(), equalTo( 200 ) );
        JsonNode data = response.get( "results" ).get( 0 ).get( "data" );
        assertTrue( "data is a list", data.isArray() );
        assertEquals( "one entry", initialData + 1, data.size() );
        JsonNode entry = data.get( 0 );
        assertTrue( "entry has row", entry.has( "row" ) );
        assertTrue( "entry has graph", entry.has( "graph" ) );
        JsonNode nodes = entry.get( "graph" ).get( "nodes" );
        JsonNode rels = entry.get( "graph" ).get( "relationships" );
        assertTrue( "nodes is a list", nodes.isArray() );
        assertTrue( "relationships is a list", rels.isArray() );
        assertEquals( "one node", 1, nodes.size() );
        assertEquals( "no relationships", 0, rels.size() );
        Set<String> labels = new HashSet<>();
        for ( JsonNode node : nodes.get( 0 ).get( "labels" ) )
        {
            labels.add( node.getTextValue() );
        }
        assertEquals( "labels", asSet( "Foo", "Bar" ), labels );
    }

    @Test
    public void should_serialize_collect_correctly() throws Exception
    {
        // given
        http.POST( "db/data/transaction/commit", singleStatement( "CREATE (n:Foo)" ) );

        // when
        Response response = http.POST( "db/data/transaction/commit", quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN COLLECT(n)' } ] }" ) );

        // then
        assertThat( response.status(), equalTo( 200 ) );

        JsonNode data = response.get( "results" ).get( 0 );
        assertThat( data.get( "columns" ).get( 0 ).asText(), equalTo( "COLLECT(n)" ) );
        assertThat( data.get( "data" ).get( 0 ).get( "row" ).size(), equalTo( 1 ) );
        assertThat( data.get( "data" ).get( 0 ).get( "row" ).get( 0 ).get( 0 ).size(), equalTo( 0 ) );

        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldSerializeMapsCorrectlyInRowsFormat() throws Exception
    {
        Response response = http.POST( "db/data/transaction/commit", quotedJson(
                "{ 'statements': [ { 'statement': 'RETURN {one:{two:[true, {three: 42}]}}' } ] }" ) );

        // then
        assertThat( response.status(), equalTo( 200 ) );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode row = data.get( "data" ).get( 0 ).get( "row" );
        assertThat( row.size(), equalTo( 1 ) );
        JsonNode firstCell = row.get( 0 );
        assertThat( firstCell.get( "one" ).get( "two" ).size(), is( 2 ) );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 0 ).asBoolean(), is( true ) );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asInt(), is( 42 ) );

        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldSerializeMapsCorrectlyInRestFormat() throws Exception
    {
        Response response = http.POST( "db/data/transaction/commit", quotedJson( "{ 'statements': [ { 'statement': " +
                                                                                  "'RETURN {one:{two:[true, {three: " +
                                                                                  "42}]}}', " +
                                                                                  "'resultDataContents':['rest'] } ] " +
                                                                                  "}" ) );

        // then
        assertThat( response.status(), equalTo( 200 ) );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode rest = data.get( "data" ).get( 0 ).get( "rest" );
        assertThat( rest.size(), equalTo( 1 ) );
        JsonNode firstCell = rest.get( 0 );
        assertThat( firstCell.get( "one" ).get( "two" ).size(), is( 2 ) );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 0 ).asBoolean(), is( true ) );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asInt(), is( 42 ) );

        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );
    }

    @Test
    public void shouldHandleMapParametersCorrectly() throws Exception
    {
        Response response = http.POST(
                "db/data/transaction/commit",
                quotedJson("{ 'statements': [ { 'statement': " +
                        "'WITH {map} AS map RETURN map[0]', 'parameters':{'map':[{'index':0,'name':'a'},{'index':1,'name':'b'}]} } ] }"));

        // then
        assertThat( response.status(), equalTo( 200 ) );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode row = data.get( "data" ).get( 0 ).get( "row" );
        assertThat( row.size(), equalTo( 1 ) );

        assertThat( row.get(0).get("index").asInt(), equalTo( 0 ) );
        assertThat( row.get(0).get("name").asText(), equalTo( "a" ) );

        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );
    }

    @Test
    public void restFormatNodesShouldHaveSensibleUris() throws Throwable
    {
        // given
        final String hostname = "localhost";
        final String scheme = "http";

        // when
        Response rs = http.POST( "db/data/transaction/commit", quotedJson(
                "{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', 'resultDataContents':['rest'] } ] }"
        ) );

        // then
        JsonNode restNode = rs.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" ).get( 0 );

        assertPath( restNode.get( "labels" ), "/node/\\d+/labels", hostname, scheme );
        assertPath( restNode.get( "outgoing_relationships" ), "/node/\\d+/relationships/out", hostname, scheme );
        assertPath( restNode.get( "traverse" ), "/node/\\d+/traverse/\\{returnType\\}", hostname, scheme );
        assertPath( restNode.get( "all_typed_relationships" ),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}", hostname, scheme );
        assertPath( restNode.get( "self" ), "/node/\\d+", hostname, scheme );
        assertPath( restNode.get( "property" ), "/node/\\d+/properties/\\{key\\}", hostname, scheme );
        assertPath( restNode.get( "properties" ), "/node/\\d+/properties", hostname, scheme );
        assertPath( restNode.get( "outgoing_typed_relationships" ),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}", hostname, scheme );
        assertPath( restNode.get( "incoming_relationships" ), "/node/\\d+/relationships/in", hostname, scheme );
        assertPath( restNode.get( "create_relationship" ), "/node/\\d+/relationships", hostname, scheme );
        assertPath( restNode.get( "paged_traverse" ), "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," +
                                                      "leaseTime\\}", "localhost", scheme );
        assertPath( restNode.get( "all_relationships" ), "/node/\\d+/relationships/all", hostname, scheme );
        assertPath( restNode.get( "incoming_typed_relationships" ),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}", hostname, scheme );
    }

    @Test
    public void restFormattedNodesShouldHaveSensibleUrisWhenUsingXForwardHeader() throws Throwable
    {
        // given
        final String hostname = "dummy.example.org";
        final String scheme = "http";

        // when
        Response rs = http.withHeaders( XForwardUtil.X_FORWARD_HOST_HEADER_KEY, hostname )
                .POST( "db/data/transaction/commit", quotedJson(
                        "{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', " +
                        "'resultDataContents':['rest'] } ] }"
                ) );

        // then
        JsonNode restNode = rs.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" ).get( 0 );

        assertPath( restNode.get( "labels" ), "/node/\\d+/labels", hostname, scheme );
        assertPath( restNode.get( "outgoing_relationships" ), "/node/\\d+/relationships/out", hostname, scheme );
        assertPath( restNode.get( "traverse" ), "/node/\\d+/traverse/\\{returnType\\}", hostname, scheme );
        assertPath( restNode.get( "all_typed_relationships" ),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}", hostname, scheme );
        assertPath( restNode.get( "self" ), "/node/\\d+", hostname, scheme );
        assertPath( restNode.get( "property" ), "/node/\\d+/properties/\\{key\\}", hostname, scheme );
        assertPath( restNode.get( "properties" ), "/node/\\d+/properties", hostname, scheme );
        assertPath( restNode.get( "outgoing_typed_relationships" ),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}", hostname, scheme );
        assertPath( restNode.get( "incoming_relationships" ), "/node/\\d+/relationships/in", hostname, scheme );
        assertPath( restNode.get( "create_relationship" ), "/node/\\d+/relationships", hostname, scheme );
        assertPath( restNode.get( "paged_traverse" ), "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," +
                                                      "leaseTime\\}", hostname, scheme );
        assertPath( restNode.get( "all_relationships" ), "/node/\\d+/relationships/all", hostname, scheme );
        assertPath( restNode.get( "incoming_typed_relationships" ),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}", hostname, scheme );
    }

    @Test
    public void correctStatusCodeWhenUsingHintWithoutAnyIndex()
    {
        // begin and execute and commit
        Response begin = http.POST( "db/data/transaction/commit",
                quotedJson( "{ 'statements': [ { 'statement': " +
                        "'MATCH (n:Test) USING INDEX n:Test(foo) WHERE n.foo = 42 RETURN n.foo' } ] }" ) );
        assertThat( begin, hasErrors( Status.Request.Schema.IndexNotFound ) );
    }

    @Test
    public void transaction_not_in_response_on_failure() throws Exception
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );

        String commitResource = begin.stringFromContent( "commit" );

        // execute valid statement
        Response valid =
                http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 42' } ] }" ) );
        assertThat( valid.status(), equalTo( 200 ) );
        assertThat( valid.get( "transaction"), notNullValue() );

        // execute invalid statement
        Response invalid =
                http.POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'RETRUN 42' } ] }" ) );
        assertThat( invalid.status(), equalTo( 200 ) );
        //transaction has been closed and rolled back
        assertThat( invalid.get( "transaction"), nullValue() );

        // commit
        Response commit = http.POST( commitResource );

        //no transaction open anymore, we have failed
        assertThat( commit.status(), equalTo( 404 ) );
    }

    @Test
    public void shouldWorkWhenHittingTheASTCacheInCypher() throws JsonParseException
    {
        // give a cached plan
        Response response = http.POST( "db/data/transaction/commit",
                singleStatement( "MATCH (group:Group {name: \\\"AAA\\\"}) RETURN *" ) );

        assertThat( response.status(), equalTo( 200 ) );
        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );

        // when we hit the ast cache
        response = http.POST( "db/data/transaction/commit",
                singleStatement( "MATCH (group:Group {name: \\\"BBB\\\"}) RETURN *" ) );

        // then no errors (in particular no NPE)
        assertThat( response.status(), equalTo( 200 ) );
        assertThat( response.get( "errors" ).size(), equalTo( 0 ) );
    }

    private void assertPath( JsonNode jsonURIString, String path, String hostname, final String scheme )
    {
        assertTrue( "Expected a uri matching '" + scheme + "://" + hostname + ":\\d+/db/data" + path + "', " +
                    "but got '" + jsonURIString.asText() + "'.",
                jsonURIString.asText().matches( scheme + "://" + hostname + ":\\d+/db/data" + path ) );
    }

    private HTTP.RawPayload singleStatement( String statement )
    {
        return rawPayload( "{\"statements\":[{\"statement\":\"" + statement + "\"}]}" );
    }

    private long countNodes( String... labels )
    {
        Set<Label> givenLabels = new HashSet<>( labels.length );
        for ( String label : labels )
        {
            givenLabels.add( Label.label( label ) );
        }

        try ( Transaction transaction = graphdb().beginTx() )
        {
            long count = 0;
            for ( Node node : graphdb().getAllNodes() )
            {
                Set<Label> nodeLabels = Iterables.asSet( node.getLabels() );
                if ( nodeLabels.containsAll( givenLabels ) )
                {
                    count++;
                }
            }
            transaction.failure();
            return count;
        }
    }

    private void assertHasTxLocation( Response begin )
    {
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );
    }

    private void lockNodeWithLabel( Label sharedLockLabel, CountDownLatch nodeLockLatch, CountDownLatch nodeReleaseLatch )
    {
        GraphDatabaseService db = graphdb();
        try ( Transaction ignored = db.beginTx() )
        {
            Node node = db.findNodes( sharedLockLabel ).next();
            node.setProperty( "a", "b" );
            nodeLockLatch.countDown();
            nodeReleaseLatch.await();
        }
        catch ( InterruptedException e )
        {
            throw new RuntimeException( e );
        }
    }

    private static void waitForStatementExecution( String statement )
    {
        KernelTransactions kernelTransactions =
                server().getDatabase().getGraph().getDependencyResolver().resolveDependency( KernelTransactions.class );
        while ( !isStatementExecuting( kernelTransactions, statement ) )
        {
            Thread.yield();
        }
    }

    private static boolean isStatementExecuting( KernelTransactions kernelTransactions, String statement )
    {
        return kernelTransactions.activeTransactions().stream()
                .flatMap( KernelTransactionHandle::executingQueries )
                .anyMatch( executingQuery -> statement.equals( executingQuery.queryText() ) );
    }
}
