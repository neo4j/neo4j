/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.server.http.cypher.integration;

import com.fasterxml.jackson.databind.JsonNode;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.PrintStream;
import java.io.Reader;
import java.net.Socket;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.api.KernelTransactions;
import org.neo4j.kernel.impl.transaction.stats.DatabaseTransactionStats;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.ParameterizedTransactionEndpointsTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.server.web.XForwardUtil;
import org.neo4j.storageengine.api.TransactionIdStore;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static java.lang.Math.max;
import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.server.WebContainerTestUtils.withCSVFile;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.validRFCTimestamp;
import static org.neo4j.server.rest.domain.JsonHelper.jsonNode;
import static org.neo4j.server.web.HttpHeaderUtils.ACCESS_MODE_HEADER;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

public class TransactionIT extends ParameterizedTransactionEndpointsTestBase
{
    private ExecutorService executors;
    private String txUri;

    @BeforeEach
    public void setUp()
    {
        executors = Executors.newFixedThreadPool( max( 3, Runtime.getRuntime().availableProcessors() ) );
    }

    @AfterEach
    public void tearDown()
    {
        executors.shutdown();
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute__commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource ).matches( format( "http://localhost:\\d+/%s/\\d+/commit", txUri ) );
        assertThat( begin.get( "transaction" ).get( "expires" ).asText() ).satisfies( validRFCTimestamp() );

        // execute
        Response execute =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertThat( execute.status() ).isEqualTo( 200 );
        assertThat( execute.get( "transaction" ).get( "expires" ).asText() ).satisfies( validRFCTimestamp() );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute__rollback( String txUri )
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        // execute
        POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // rollback
        Response commit = DELETE( begin.location() );

        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute_and_commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource ).isEqualTo( begin.location() + "/commit" );

        // execute and commit
        Response commit = POST( commitResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute__commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = POST( txUri,
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute__commit_with_badly_escaped_statement( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM " +
                      "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" " +
                      "} ] }";

        // begin and execute
        // given statement is badly escaped and it is a client error, thus tx is rolled back at once
        Response begin = POST( txUri, quotedJson( json ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit fails because tx was rolled back on the previous step
        Response commit = POST( commitResource );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertThat( begin ).satisfies( hasErrors( Status.Request.InvalidFormat ) );

        assertThat( commit.status() ).isEqualTo( 404 );
        assertThat( commit ).satisfies( hasErrors( Status.Transaction.TransactionNotFound ) );

        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute__commit__execute( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri );
        String commitResource = begin.stringFromContent( "commit" );

        // execute
        POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // commit
        POST( commitResource );

        // execute
        Response execute2 = POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( execute2.status() ).isEqualTo( 404 );
        assertThat( execute2 ).satisfies( hasErrors( Status.Transaction.TransactionNotFound ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_and_commit( String txUri )
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute and commit
        Response begin = POST( transactionCommitUri(),
                quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( begin.status() ).isEqualTo( 200 );
        assertThat( begin ).satisfies( containsNoErrors() );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_and_commit_with_badly_escaped_statement( String txUri )
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();
        String json = "{ \"statements\": [ { \"statement\": \"LOAD CSV WITH HEADERS FROM " +
                      "\\\"xx file://C:/countries.csvxxx\\\\\" as csvLine MERGE (c:Country { Code: csvLine.Code })\" " +
                      "} ] }";
        // begin and execute and commit
        Response begin = POST( transactionCommitUri(), quotedJson( json ) );

        assertThat( begin.status() ).isEqualTo( 200 );
        assertThat( begin ).satisfies( hasErrors( Status.Request.InvalidFormat ) );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_periodic_commit_and_commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        int nodes = 11;
        int batch = 2;
        withCSVFile( nodes, url ->
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

                response = POST(
                        transactionCommitUri(),
                        quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT " + batch + " LOAD CSV FROM " +
                                "\\\"" + url + "\\\" AS line CREATE ()' } ] }" )
                );
                times++;
            }
            while ( response.get( "errors" ).iterator().hasNext() && (times < 5) );

            long txIdAfter = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            assertThat( response ).as( "Last response is: " + response ).satisfies( containsNoErrors() );
            assertThat( response.status() ).isEqualTo( 200 );
            assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + nodes );
            assertThat( txIdAfter ).isEqualTo( txIdBefore + ((nodes / batch) + 1) );
        } );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_periodic_commit_that_returns_data_and_commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        int nodes = 11;
        int batchSize = 2;

        // warm up the periodic commit
        withCSVFile( nodes, url ->
        {
            Response response = POST(
                    transactionCommitUri(),
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT " + batchSize + " LOAD CSV FROM " +
                            "\\\"" + url + "\\\" AS line CREATE (n {id1: 23}) RETURN n' } ] }" )
            );
        } );

        withCSVFile( nodes, url ->
        {
            long nodesInDatabaseBeforeTransaction = countNodes();
            long txIdBefore = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            // begin and execute and commit
            Response response = POST(
                    transactionCommitUri(),
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT " + batchSize + " LOAD CSV FROM " +
                            "\\\"" + url + "\\\" AS line CREATE (n {id1: 23}) RETURN n' } ] }" )
            );
            long txIdAfter = resolveDependency( TransactionIdStore.class ).getLastClosedTransactionId();

            assertThat( response.status() ).isEqualTo( 200 );

            assertThat( response ).satisfies( containsNoErrors() );

            JsonNode columns = response.get( "results" ).get( 0 ).get( "columns" );
            assertThat( columns.toString() ).isEqualTo( "[\"n\"]" );
            assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + nodes );
            long expectedTxCount = (nodes / batchSize) + 1;

            assertThat( txIdAfter - txIdBefore ).isEqualTo( expectedTxCount );
        } );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_periodic_commit_followed_by_another_statement_and_commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        withCSVFile( 1, url ->
        {
            // begin and execute and commit
            Response response = POST(
                    transactionCommitUri(),
                    quotedJson( "{ 'statements': [ { 'statement': 'USING PERIODIC COMMIT LOAD CSV FROM \\\"" +
                                url +
                                "\\\" AS line CREATE (n {id: 23}) RETURN n' }, { 'statement': 'RETURN 1' } ] }" )
            );

            assertThat( response.status() ).isEqualTo( 200 );
            assertThat( response ).satisfies( hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_invalid_query_and_commit( String txUri )
    {
        this.txUri = txUri;

        // begin and execute and commit
        Response response = POST(
                transactionCommitUri(),
                quotedJson( "{ 'statements': [ { 'statement': 'MATCH n RETURN m' } ] }" )
        );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response ).satisfies( hasErrors( Status.Statement.SyntaxError ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_and_execute_multiple_periodic_commit_last_and_commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        withCSVFile( 1, url ->
        {
            // begin and execute and commit
            Response response = POST(
                    transactionCommitUri(),
                    quotedJson( "{ 'statements': [ { 'statement': 'CREATE ()' }, " +
                                "{ 'statement': 'USING PERIODIC COMMIT LOAD CSV FROM \\\"" + url + "\\\" AS line " +
                                "CREATE ()' } ] }" )
            );

            assertThat( response ).satisfies( hasErrors( Status.Statement.SemanticError ) );
        } );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute_multiple__commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' }, " +
                                                 "{ 'statement': 'CREATE (n)' } ] }" ) );

        // commit
        Response commit = POST( commitResource );
        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 2 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__execute__execute__commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri );

        String commitResource = begin.stringFromContent( "commit" );

        // execute
        POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // execute
        POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        // commit
        POST( commitResource );

        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 2 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin_create_two_nodes_delete_one( String txUri ) throws Exception
    {
        this.txUri = txUri;

        /*
         * This issue was reported from the community. It resulted in a refactoring of the interaction
         * between TxManager and TransactionContexts.
         */

        // GIVEN
        long nodesInDatabaseBeforeTransaction = countNodes();
        Response response1 = POST( transactionCommitUri(),
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
        assertEquals( 200, response1.status() );
        JsonNode everything = jsonNode( response1.rawContent() );
        JsonNode result = everything.get( "results" ).get( 0 );
        long id = result.get( "data" ).get( 0 ).get( "row" ).get( 0 ).asLong();

        // WHEN
        Response response2 = POST( transactionCommitUri(),
                rawPayload( "{ \"statements\" : [{\"statement\":\"match (n) where id(n) = " + id + " delete n\"}]}" ) );
        assertEquals( 200, response2.status() );

        // THEN
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__rollback__commit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );
        String commitResource = begin.stringFromContent( "commit" );

        // terminate
        Response interrupt = DELETE( begin.location() );
        assertThat( interrupt.status() ).isEqualTo( 200 );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 404 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void begin__rollback__execute( String txUri )
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        // terminate
        Response interrupt = DELETE( begin.location() );
        assertThat( interrupt.status() ).isEqualTo( 200 );

        // execute
        Response execute =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );

        assertThat( execute.status() ).isEqualTo( 404 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    @Timeout( 30 )
    public void begin__execute__rollback_concurrently( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        final Response begin = POST( txUri );
        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        Label sharedLockLabel = Label.label( "sharedLock" );
        POST( transactionCommitUri(),
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
            HTTP.Builder requestBuilder = HTTP.withBaseUri( container().getBaseUri() );
            Response response = requestBuilder.POST( executeResource, quotedJson( "{ 'statements': [ { 'statement': '" +
                                                                        statement + "' } ] }" ) );
            assertThat( response.status() ).isEqualTo( 200 );
            return response;
        } );

        // terminate
        final Future<Response> interruptFuture = executors.submit( () ->
        {
            waitForStatementExecution( statement );

            Response response = DELETE( executeResource );
            assertThat( response.status() ).as( response.toString() ).isEqualTo( 200 );
            nodeReleaseLatch.countDown();
            return response;
        } );

        interruptFuture.get();
        lockerFuture.get();
        Response execute = executeFuture.get();
        assertThat( execute ).satisfies( hasErrors( Status.Statement.Statement.ExecutionFailed ) );

        Response execute2 =
                POST( executeResource, quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertThat( execute2.status() ).isEqualTo( 404 );
        assertThat( execute2 ).satisfies( hasErrors( Status.Transaction.TransactionNotFound ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void status_codes_should_appear_in_response( String txUri )
    {
        this.txUri = txUri;

        Response response = POST( transactionCommitUri(),
                quotedJson( "{ 'statements': [ { 'statement': 'RETURN $n' } ] }" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response ).satisfies( hasErrors( Status.Statement.ParameterMissing ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void should_return_location_correctly_in_response( String txUri ) throws JsonParseException
    {
        this.txUri = txUri;

        // begin
        var begin = POST( txUri );
        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        // run
        var txId = extractTxId( begin );
        var response = POST( format( "%s/%s", txUri, txId ),
                quotedJson( "{ 'statements': [ { 'statement': 'RETURN 1' } ] }" ) );
        System.out.println( response );
        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.get( "commit" ).toString() ).contains( txUri );

        // commit
        var commit = POST( format( "%s/%s/commit", txUri, txId ) );
        System.out.println( commit );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( commit.get( "commit" ).toString() ).contains( txUri );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    @Timeout( 30 )
    public void executing_single_statement_in_new_transaction_and_failing_to_read_the_output_should_interrupt( String txUri )
            throws Exception
    {
        this.txUri = txUri;

        // given
        long initialNodes = countNodes();
        DatabaseTransactionStats txMonitor = ((GraphDatabaseAPI) graphdb()).getDependencyResolver().resolveDependency(
                DatabaseTransactionStats.class );
        long initialRollBacks = txMonitor.getNumberOfRolledBackTransactions();

        // when sending a request and aborting in the middle of receiving the result
        Socket socket = new Socket( "localhost", getLocalHttpPort() );
        PrintStream out = new PrintStream( socket.getOutputStream() );

        String output = quotedJson(
                "{ 'statements': [ { 'statement': 'UNWIND range(0, 9999) AS i CREATE (n {i: i}) RETURN n' } ] " +
                "}" ).get();
        out.print( format( "POST /%s/commit HTTP/1.1\r\n", txUri ) );
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

        assertEquals( initialNodes, countNodes() );

        // then soon the transaction should have been terminated
        long endTime = System.currentTimeMillis() + 5000;
        long additionalRollBacks;

        while ( true )
        {
            additionalRollBacks = txMonitor.getNumberOfRolledBackTransactions() - initialRollBacks;

            if ( additionalRollBacks > 0 || System.currentTimeMillis() > endTime )
            {
                break;
            }

            Thread.sleep( 100 );
        }

        assertEquals( 1, additionalRollBacks );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void should_include_graph_format_when_requested( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long initialData = countNodes( "Foo" );

        // given
        POST( transactionCommitUri(), singleStatement( "CREATE (n:Foo:Bar)" ) );

        // when
        Response response = POST( transactionCommitUri(), quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN n', 'resultDataContents':['row'," +
                "'graph'] } ] }" ) );

        // then
        assertThat( response.status() ).isEqualTo( 200 );
        JsonNode data = response.get( "results" ).get( 0 ).get( "data" );
        assertTrue( data.isArray(), "data is a list" );
        assertEquals( initialData + 1, data.size(), "one entry" );
        JsonNode entry = data.get( 0 );
        assertTrue( entry.has( "row" ), "entry has row" );
        assertTrue( entry.has( "graph" ), "entry has graph" );
        JsonNode nodes = entry.get( "graph" ).get( "nodes" );
        JsonNode rels = entry.get( "graph" ).get( "relationships" );
        assertTrue( nodes.isArray(), "nodes is a list" );
        assertTrue( rels.isArray(), "relationships is a list" );
        assertEquals( 1, nodes.size(), "one node" );
        assertEquals( 0, rels.size(), "no relationships" );
        Set<String> labels = new HashSet<>();
        for ( JsonNode node : nodes.get( 0 ).get( "labels" ) )
        {
            labels.add( node.asText() );
        }
        assertTrue( labels.size() > 0, "some labels" );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void should_serialize_collect_correctly( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // given
        POST( transactionCommitUri(), singleStatement( "CREATE (n:Foo)" ) );

        // when
        Response response = POST( transactionCommitUri(), quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n:Foo) RETURN COLLECT(n)' } ] }" ) );

        // then
        assertThat( response.status() ).isEqualTo( 200 );

        JsonNode data = response.get( "results" ).get( 0 );
        assertThat( data.get( "columns" ).get( 0 ).asText() ).isEqualTo( "COLLECT(n)" );
        assertThat( data.get( "data" ).get( 0 ).get( "row" ).size() ).isEqualTo( 1 );
        assertThat( data.get( "data" ).get( 0 ).get( "row" ).get( 0 ).get( 0 ).size() ).isEqualTo( 0 );

        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldSerializeMapsCorrectlyInRowsFormat( String txUri ) throws Exception
    {
        this.txUri = txUri;

        Response response = POST( transactionCommitUri(), quotedJson(
                "{ 'statements': [ { 'statement': 'RETURN {one:{two:[true, {three: 42}]}}' } ] }" ) );

        // then
        assertThat( response.status() ).isEqualTo( 200 );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode row = data.get( "data" ).get( 0 ).get( "row" );
        assertThat( row.size() ).isEqualTo( 1 );
        JsonNode firstCell = row.get( 0 );
        assertThat( firstCell.get( "one" ).get( "two" ).size() ).isEqualTo( 2 );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 0 ).asBoolean() ).isEqualTo( true );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asInt() ).isEqualTo( 42 );

        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldSerializeMapsCorrectlyInRestFormat( String txUri ) throws Exception
    {
        this.txUri = txUri;

        Response response = POST( transactionCommitUri(),
                quotedJson( "{ 'statements': [ { 'statement': 'RETURN {one:{two:[true, {three: 42}]}}', 'resultDataContents':['rest'] } ] }" ) );

        // then
        assertThat( response.status() ).isEqualTo( 200 );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode rest = data.get( "data" ).get( 0 ).get( "rest" );
        assertThat( rest.size() ).isEqualTo( 1 );
        JsonNode firstCell = rest.get( 0 );
        assertThat( firstCell.get( "one" ).get( "two" ).size() ).isEqualTo( 2 );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 0 ).asBoolean() ).isEqualTo( true );
        assertThat( firstCell.get( "one" ).get( "two" ).get( 1 ).get( "three" ).asInt() ).isEqualTo( 42 );

        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldHandleMapParametersCorrectly( String txUri ) throws Exception
    {
        this.txUri = txUri;

        Response response = POST(
                transactionCommitUri(),
                quotedJson("{ 'statements': [ { 'statement': " +
                        "'WITH $map AS map RETURN map[0]', 'parameters':{'map':[{'index':0,'name':'a'},{'index':1,'name':'b'}]} } ] }") );

        // then
        assertThat( response.status() ).isEqualTo( 200 );

        JsonNode data = response.get( "results" ).get( 0 );
        JsonNode row = data.get( "data" ).get( 0 ).get( "row" );
        assertThat( row.size() ).isEqualTo( 1 );

        assertThat( row.get( 0 ).get( "index" ).asInt() ).isEqualTo( 0 );
        assertThat( row.get( 0 ).get( "name" ).asText() ).isEqualTo( "a" );

        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void restFormatNodesShouldHaveSensibleUris( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // given
        final String hostname = "localhost";

        // when
        Response rs = POST( transactionCommitUri(), quotedJson(
                "{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', 'resultDataContents':['rest'] } ] }" ) );

        // then
        JsonNode restNode = rs.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" ).get( 0 );

        assertPath( restNode.get( "labels" ), "/node/\\d+/labels", hostname );
        assertPath( restNode.get( "outgoing_relationships" ), "/node/\\d+/relationships/out", hostname );
        assertPath( restNode.get( "traverse" ), "/node/\\d+/traverse/\\{returnType\\}", hostname );
        assertPath( restNode.get( "all_typed_relationships" ),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}", hostname );
        assertPath( restNode.get( "self" ), "/node/\\d+", hostname );
        assertPath( restNode.get( "property" ), "/node/\\d+/properties/\\{key\\}", hostname );
        assertPath( restNode.get( "properties" ), "/node/\\d+/properties", hostname );
        assertPath( restNode.get( "outgoing_typed_relationships" ),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}", hostname );
        assertPath( restNode.get( "incoming_relationships" ), "/node/\\d+/relationships/in", hostname );
        assertPath( restNode.get( "create_relationship" ), "/node/\\d+/relationships", hostname );
        assertPath( restNode.get( "paged_traverse" ), "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," +
                                                      "leaseTime\\}", hostname );
        assertPath( restNode.get( "all_relationships" ), "/node/\\d+/relationships/all", hostname );
        assertPath( restNode.get( "incoming_typed_relationships" ),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}", hostname );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void restFormattedNodesShouldHaveSensibleUrisWhenUsingXForwardHeader( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // given
        final String hostname = "dummy.example.org";

        // when
        Response rs = http.withHeaders( XForwardUtil.X_FORWARD_HOST_HEADER_KEY, hostname )
                .POST( transactionCommitUri(), quotedJson(
                        "{ 'statements': [ { 'statement': 'CREATE (n:Foo:Bar) RETURN n', " +
                        "'resultDataContents':['rest'] } ] }" ) );

        // then
        JsonNode restNode = rs.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" ).get( 0 );

        assertPath( restNode.get( "labels" ), "/node/\\d+/labels", hostname );
        assertPath( restNode.get( "outgoing_relationships" ), "/node/\\d+/relationships/out", hostname );
        assertPath( restNode.get( "traverse" ), "/node/\\d+/traverse/\\{returnType\\}", hostname );
        assertPath( restNode.get( "all_typed_relationships" ),
                "/node/\\d+/relationships/all/\\{-list\\|&\\|types\\}", hostname );
        assertPath( restNode.get( "self" ), "/node/\\d+", hostname );
        assertPath( restNode.get( "property" ), "/node/\\d+/properties/\\{key\\}", hostname );
        assertPath( restNode.get( "properties" ), "/node/\\d+/properties", hostname );
        assertPath( restNode.get( "outgoing_typed_relationships" ),
                "/node/\\d+/relationships/out/\\{-list\\|&\\|types\\}", hostname );
        assertPath( restNode.get( "incoming_relationships" ), "/node/\\d+/relationships/in", hostname );
        assertPath( restNode.get( "create_relationship" ), "/node/\\d+/relationships", hostname );
        assertPath( restNode.get( "paged_traverse" ), "/node/\\d+/paged/traverse/\\{returnType\\}\\{\\?pageSize," +
                                                      "leaseTime\\}", hostname );
        assertPath( restNode.get( "all_relationships" ), "/node/\\d+/relationships/all", hostname );
        assertPath( restNode.get( "incoming_typed_relationships" ),
                "/node/\\d+/relationships/in/\\{-list\\|&\\|types\\}", hostname );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void correctStatusCodeWhenUsingHintWithoutAnyIndex( String txUri )
    {
        this.txUri = txUri;

        // begin and execute and commit
        Response begin = POST( transactionCommitUri(),
                quotedJson( "{ 'statements': [ { 'statement': " +
                        "'MATCH (n:Test) USING INDEX n:Test(foo) WHERE n.foo = 42 RETURN n.foo' } ] }" ) );
        assertThat( begin ).satisfies( hasErrors( Status.Request.Schema.IndexNotFound ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void transaction_not_in_response_on_failure( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri );

        String commitResource = begin.stringFromContent( "commit" );

        // execute valid statement
        Response valid =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'RETURN 42' } ] }" ) );
        assertThat( valid.status() ).isEqualTo( 200 );
        assertThat( valid.get( "transaction" ) ).isNotNull();

        // execute invalid statement
        Response invalid =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'RETRUN 42' } ] }" ) );
        assertThat( invalid.status() ).isEqualTo( 200 );
        //transaction has been closed and rolled back
        assertThat( invalid.get( "transaction" ) ).isNull();

        // commit
        Response commit = POST( commitResource );

        //no transaction open anymore, we have failed
        assertThat( commit.status() ).isEqualTo( 404 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldWorkWhenHittingTheASTCacheInCypher( String txUri ) throws JsonParseException
    {
        this.txUri = txUri;

        // give a cached plan
        Response response = POST( transactionCommitUri(),
                singleStatement( "MATCH (group:Group {name: \\\"AAA\\\"}) RETURN *" ) );

        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );

        // when we hit the ast cache
        response = POST( transactionCommitUri(),
                singleStatement( "MATCH (group:Group {name: \\\"BBB\\\"}) RETURN *" ) );

        // then no errors (in particular no NPE)
        assertThat( response.status() ).isEqualTo( 200 );
        assertThat( response.get( "errors" ).size() ).isEqualTo( 0 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void writeSettingsBeginAndCommit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin and execute
        Response begin = POST( txUri,
                               quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ),
                               Map.of( ACCESS_MODE_HEADER, "WRITE") );

        String commitResource = begin.stringFromContent( "commit" );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldErrorWhenWriteAttemptedWithReadSetting( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin and execute and commit
        Response begin = POST( transactionCommitUri(),
                               quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ),
                               Map.of( ACCESS_MODE_HEADER, "READ" ) );

        assertThat( begin.status() ).isEqualTo( 200 );
        assertThat( begin ).satisfies( hasErrors( Status.Statement.Request.Invalid ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void readSettingsBeginAndCommit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin and execute
        Response begin = POST( txUri,
                               quotedJson( "{'statements': [ { 'statement': 'MATCH (n) RETURN n' } ] }" ),
                               Map.of( ACCESS_MODE_HEADER, "READ" ) );

        String commitResource = begin.stringFromContent( "commit" );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 200 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void beginWithSettingsOnlyAndThenExecuteCommit( String txUri ) throws Exception
    {
        this.txUri = txUri;

        long nodesInDatabaseBeforeTransaction = countNodes();

        // begin
        Response begin = POST( txUri, quotedJson( "" ), Map.of( ACCESS_MODE_HEADER, "WRITE" ) );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource ).matches( format( "http://localhost:\\d+/%s/\\d+/commit", txUri ) );
        assertThat( begin.get( "transaction" ).get( "expires" ).asText() ).satisfies( validRFCTimestamp() );

        // execute
        Response execute =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ) );
        assertThat( execute.status() ).isEqualTo( 200 );
        assertThat( execute.get( "transaction" ).get( "expires" ).asText() ).satisfies( validRFCTimestamp() );

        // commit
        Response commit = POST( commitResource );

        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( countNodes() ).isEqualTo( nodesInDatabaseBeforeTransaction + 1 );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldIgnoreAccessModeHeaderOnSecondRequest( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri,
                               quotedJson( "" ),
                               Map.of( ACCESS_MODE_HEADER, "READ" ) );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin, txUri );

        String commitResource = begin.stringFromContent( "commit" );
        assertThat( commitResource ).matches( format( "http://localhost:\\d+/%s/\\d+/commit", txUri ) );
        assertThat( begin.get( "transaction" ).get( "expires" ).asText() ).satisfies( validRFCTimestamp() );

        // execute
        Response execute =
                POST( begin.location(), quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n)' } ] }" ),
                      Map.of( ACCESS_MODE_HEADER, "WRITE" ) );
        assertThat( execute.status() ).isEqualTo( 200 );
        assertThat( execute ).satisfies( hasErrors( Status.Request.Invalid ) );
    }

    @ParameterizedTest
    @MethodSource( "argumentsProvider" )
    public void shouldErrorWithInvalidAccessModeHeader( String txUri ) throws Exception
    {
        this.txUri = txUri;

        // begin
        Response begin = POST( txUri,
                               quotedJson( "" ),
                               Map.of( ACCESS_MODE_HEADER, "INVALID!" ) );

        assertThat( begin.status() ).isEqualTo( 200 );
        assertThat( begin ).satisfies( hasErrors( Status.Request.InvalidFormat ) );
    }

    private String transactionCommitUri()
    {
        return format( "%s/commit", txUri );
    }

    private String databaseName()
    {
        return txUri.split( "/" )[1]; // either data or neo4j
    }

    private void assertPath( JsonNode jsonURIString, String path, String hostname )
    {
        var databaseName = databaseName();
        var expected = String.format( "http://%s:\\d+/db/%s%s", hostname, databaseName, path );
        assertTrue( jsonURIString.asText().matches( expected ),
                String.format( "Expected a uri matching '%s', but got '%s'.", expected, jsonURIString.asText() ) );
    }

    private static HTTP.RawPayload singleStatement( String statement )
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

        final GraphDatabaseService graphdb = graphdb();
        try ( Transaction transaction = graphdb.beginTx() )
        {
            long count = 0;
            for ( Node node : transaction.getAllNodes() )
            {
                Set<Label> nodeLabels = Iterables.asSet( node.getLabels() );
                if ( nodeLabels.containsAll( givenLabels ) )
                {
                    count++;
                }
            }
            transaction.commit();
            return count;
        }
    }

    private void lockNodeWithLabel( Label sharedLockLabel, CountDownLatch nodeLockLatch, CountDownLatch nodeReleaseLatch )
    {
        GraphDatabaseService db = graphdb();
        try ( Transaction tx = db.beginTx();
              ResourceIterator<Node> nodes = tx.findNodes( sharedLockLabel ) )
        {
            Node node = nodes.next();
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
        var server = container();
        var databaseService = server.getDefaultDatabase();
        var kernelTransactions = databaseService.getDependencyResolver().resolveDependency( KernelTransactions.class );
        while ( !isStatementExecuting( kernelTransactions, statement ) )
        {
            Thread.yield();
        }
    }

    private static boolean isStatementExecuting( KernelTransactions kernelTransactions, String statement )
    {
        return kernelTransactions.activeTransactions().stream()
                .flatMap( k -> k.executingQuery().stream() )
                .anyMatch( executingQuery -> statement.equals( executingQuery.rawQueryText() ) );
    }
}
