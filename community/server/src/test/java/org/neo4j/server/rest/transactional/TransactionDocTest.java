/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.server.rest.transactional;

import java.text.ParseException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.repr.util.RFC1123;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.server.HTTP;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.*;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;

public class TransactionDocTest extends AbstractRestFunctionalTestBase
{
    /**
     * Begin a transaction
     *
     * You begin a new transaction by posting zero or more Cypher statements
     * to the transaction endpoint. The server will respond with the result of
     * your statements, as well as the location of your open transaction.
     */
    @Test
    @Documented
    public void begin_a_transaction() throws PropertyValueException
    {
        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 201 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n {props}) RETURN n', " +
                        "'parameters': { 'props': { 'name': 'My Node' } } } ] }" ) )
                .post( getDataUri() + "transaction" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        Map<String, Object> node = resultCell( result, 0, 0 );
        assertThat( (String) node.get( "name" ), equalTo( "My Node" ) );
    }

    /**
     * Execute statements in an open transaction
     *
     * Given that you have an open transaction, you can make a number of requests, each of which executes additional
     * statements, and keeps the transaction open by resetting the transaction timeout.
     */
    @Test
    @Documented
    public void execute_statements_in_an_open_transaction() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction" ).location();

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE n RETURN n' } ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
    }

    /**
     * Execute statements in an open transaction in REST format for the return.
     *
     * Given that you have an open transaction, you can make a number of requests, each of which executes additional
     * statements, and keeps the transaction open by resetting the transaction timeout. Specifying the `REST` format will
     * give back full Neo4j Rest API representations of the Neo4j Nodes, Relationships and Paths, if returned.
     */
    @Test
    @Documented
    public void execute_statements_in_an_open_transaction_using_REST() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction" ).location();

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE n RETURN n','resultDataContents':['REST'] } ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        ArrayList rest = (ArrayList) ((Map)((ArrayList)((Map)((ArrayList)result.get("results")).get(0)) .get("data")).get(0)).get("rest");
        String selfUri = ((String)((Map)rest.get(0)).get("self"));
        assertTrue(selfUri.startsWith(getDatabaseUri()));
        assertNoErrors( result );
    }

    /**
     * Reset transaction timeout of an open transaction
     *
     * Every orphaned transaction is automatically expired after a period of inactivity.  This may be prevented
     * by resetting the transaction timeout.
     *
     * The timeout may be reset by sending a keep-alive request to the server that executes an empty list of statements.
     * This request will reset the transaction timeout and return the new time at which the transaction will
     * expire as an RFC1123 formatted timestamp value in the ``transaction'' section of the response.
     */
    @Test
    @Documented
    public void reset_transaction_timeout_of_an_open_transaction()
            throws PropertyValueException, ParseException, InterruptedException
    {
        // Given
        HTTP.Response initialResponse = POST( getDataUri() + "transaction" );
        String location = initialResponse.location();
        long initialExpirationTime = expirationTime( jsonToMap( initialResponse.rawContent() ) );

        // This generous wait time is necessary to compensate for limited resolution of RFC 1123 timestamps
        // and the fact that the system clock is allowed to run "backwards" between threads
        // (cf. http://stackoverflow.com/questions/2978598)
        //
        Thread.sleep( 3000 );

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ ] }" ) )
                .post( location );


        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        long newExpirationTime = expirationTime( result );

        assertTrue( "Expiration time was not increased", newExpirationTime > initialExpirationTime );
    }

    /**
     * Commit an open transaction
     *
     * Given you have an open transaction, you can send a commit request. Optionally, you submit additional statements
     * along with the request that will be executed before committing the transaction.
     */
    @Test
    @Documented
    public void commit_an_open_transaction() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction" ).location();

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE n RETURN id(n)' } ] }" ) )
                .post( location + "/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        assertThat( GET( getNodeUri( id ) ).status(), is( 200 ) );
    }

    /**
     * Begin and commit a transaction in one request
     *
     * If there is no need to keep a transaction open across multiple HTTP requests, you can begin a transaction,
     * execute statements, and commit with just a single HTTP request.
     */
    @Test
    @Documented
    public void begin_and_commit_a_transaction_in_one_request() throws PropertyValueException
    {
        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE n RETURN id(n)' } ] }" ) )
                .post( getDataUri() + "transaction/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        assertThat( GET( getNodeUri( id ) ).status(), is( 200 ) );
    }

    /**
     * Return results in graph format
     *
     * If you want to understand the graph structure of nodes and relationships returned by your query,
     * you can specify the "graph" results data format. For example, this is useful when you want to visualise the
     * graph structure. The format collates all the nodes and relationships from all columns of the result,
     * and also flattens collections of nodes and relationships, including paths.
     */
    @Test
    @Documented
    public void return_results_in_graph_format() throws PropertyValueException
    {
        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'statements':[{'statement':" +
                        "'CREATE ( bike:Bike { weight: 10 } )" +
                        "CREATE ( frontWheel:Wheel { spokes: 3 } )" +
                        "CREATE ( backWheel:Wheel { spokes: 32 } )" +
                        "CREATE p1 = bike -[:HAS { position: 1 } ]-> frontWheel " +
                        "CREATE p2 = bike -[:HAS { position: 2 } ]-> backWheel " +
                        "RETURN bike, p1, p2', " +
                        "'resultDataContents': ['row','graph']}] }" ) )
                        .post( getDataUri() + "transaction/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Map<String, List<Object>> row = graphRow( result, 0 );
        assertEquals( 3, row.get( "nodes" ).size() );
        assertEquals( 2, row.get( "relationships" ).size() );
    }

    /**
     * Rollback an open transaction
     *
     * Given that you have an open transaction, you can send a roll back request. The server will roll back the
     * transaction.
     */
    @Test
    @Documented
    public void rollback_an_open_transaction() throws PropertyValueException
    {
        // Given
        HTTP.Response firstReq = POST( getDataUri() + "transaction",
                HTTP.RawPayload.quotedJson( "{ 'statements': [ { 'statement': 'CREATE n RETURN id(n)' } ] }" ) );
        String location = firstReq.location();

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .delete( location + "" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( firstReq, 0, 0 );
        assertThat( GET( getNodeUri( id ) ).status(), is( 404 ) );
    }

    /**
     * Handling errors
     *
     * The result of any request against the transaction endpoint is streamed back to the client.
     * Therefore the server does not know whether the request will be successful or not when it sends the HTTP status
     * code.
     *
     * Because of this, all requests against the transactional endpoint will return 200 or 201 status code, regardless
     * of whether statements were successfully executed. At the end of the response payload, the server includes a list
     * of errors that occurred while executing statements. If this list is empty, the request completed successfully.
     *
     * If any errors occur while executing statements, the server will roll back the transaction.
     *
     * In this example, we send the server an invalid statement to demonstrate error handling.
     * 
     * For more information on the status codes, see <<status-codes>>.
     */
    @Test
    @Documented
    public void handling_errors() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction" ).location();

        // Document
        ResponseEntity response = gen.get()
                .noGraph()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'This is not a valid Cypher Statement.' } ] }" ) )
                .post( location + "/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertErrors( result, Status.Statement.InvalidSyntax );
    }

    private void assertNoErrors( Map<String, Object> response )
    {
        assertErrors( response );
    }

    private void assertErrors( Map<String, Object> response, Status... expectedErrors )
    {
        @SuppressWarnings("unchecked")
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        Iterator<Status> expected = iterator( expectedErrors );

        while ( expected.hasNext() )
        {
            assertTrue( errors.hasNext() );
            assertThat( (String)errors.next().get( "code" ), equalTo( expected.next().code().serialize() ) );
        }
        if ( errors.hasNext() )
        {
            Map<String, Object> error = errors.next();
            fail( "Expected no more errors, but got " + error.get( "code" ) + " - '" + error.get( "message" ) + "'." );
        }
    }

    private <T> T resultCell( HTTP.Response response, int row, int column )
    {
        return resultCell( response.<Map<String, Object>>content(), row, column );
    }

    @SuppressWarnings("unchecked")
    private <T> T resultCell( Map<String, Object> response, int row, int column )
    {
        Map<String, Object> result = ((List<Map<String, Object>>) response.get( "results" )).get( 0 );
        List<Map<String,List>> data = (List<Map<String,List>>) result.get( "data" );
        return (T) data.get( row ).get( "row" ).get( column );
    }

    @SuppressWarnings("unchecked")
    private Map<String, List<Object>> graphRow( Map<String, Object> response, int row )
    {
        Map<String, Object> result = ((List<Map<String, Object>>) response.get( "results" )).get( 0 );
        List<Map<String,List>> data = (List<Map<String,List>>) result.get( "data" );
        return (Map<String,List<Object>>) data.get( row ).get( "graph" );
    }

    private String quotedJson( String singleQuoted )
    {
        return singleQuoted.replaceAll( "'", "\"" );
    }

    private long expirationTime( Map<String, Object> entity ) throws ParseException
    {
        String timestampString = (String) ( (Map<?, ?>) entity.get( "transaction" ) ).get( "expires" );
        return RFC1123.parseTimestamp( timestampString ).getTime();
    }
}
