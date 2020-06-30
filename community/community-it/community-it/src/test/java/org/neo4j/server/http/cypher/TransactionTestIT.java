/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.server.http.cypher;

import org.junit.jupiter.api.Test;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.neo4j.annotations.documented.Documented;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.RESTRequestGenerator.ResponseEntity;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.test.server.HTTP.POST;

public class TransactionTestIT extends AbstractRestFunctionalTestBase
{
    @Test
    @Documented( "Begin a transaction\n" +
                 "\n" +
                 "You begin a new transaction by posting zero or more Cypher statements\n" +
                 "to the transaction endpoint. The server will respond with the result of\n" +
                 "your statements, as well as the location of your open transaction." )
    public void begin_a_transaction() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 201 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n $props) RETURN n', " +
                        "'parameters': { 'props': { 'name': 'My Node' } } } ] }" ) )
                .post( txUri() );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        Map<String, Object> node = resultCell( result, 0, 0 );
        assertThat( node.get( "name" ) ).isEqualTo( "My Node" );
    }

    @Test
    @Documented( "Execute statements in an open transaction\n" +
                 "\n" +
                 "Given that you have an open transaction, you can make a number of requests, each of which executes additional\n" +
                 "statements, and keeps the transaction open by resetting the transaction timeout." )
    public void execute_statements_in_an_open_transaction() throws JsonParseException
    {
        // Given
        String location = POST( txUri() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN n' } ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertThat( result ).containsKey( "transaction" );
        assertNoErrors( result );
    }

    @Test
    @Documented( "Execute statements in an open transaction in REST format for the return.\n" +
                 "\n" +
                 "Given that you have an open transaction, you can make a number of requests, each of which executes additional\n" +
                 "statements, and keeps the transaction open by resetting the transaction timeout. Specifying the `REST` format will\n" +
                 "give back full Neo4j Rest API representations of the Neo4j Nodes, Relationships and Paths, if returned." )
    public void execute_statements_in_an_open_transaction_using_REST() throws JsonParseException
    {
        // Given
        String location = POST( txUri() ).location();
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN n','resultDataContents':['REST'] } ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        List rest = (ArrayList) ((Map)((ArrayList)((Map)((ArrayList)result.get("results")).get(0)) .get("data")).get(0)).get("rest");
        String selfUri = (String) ((Map)rest.get(0)).get("self");
        assertTrue(selfUri.startsWith( dbUri()));
        assertNoErrors( result );
    }

    @Test
    @Documented( "Reset transaction timeout of an open transaction\n" +
                 "\n" +
                 "Every orphaned transaction is automatically expired after a period of inactivity.  This may be prevented\n" +
                 "by resetting the transaction timeout.\n" +
                 "\n" +
                 "The timeout may be reset by sending a keep-alive request to the server that executes an empty list of statements.\n" +
                 "This request will reset the transaction timeout and return the new time at which the transaction will\n" +
                 "expire as an RFC1123 formatted timestamp value in the ``transaction'' section of the response." )
    public void reset_transaction_timeout_of_an_open_transaction()
            throws JsonParseException, InterruptedException
    {
        // Given
        HTTP.Response initialResponse = POST( txUri() );
        String location = initialResponse.location();
        long initialExpirationTime = expirationTime( jsonToMap( initialResponse.rawContent() ) );

        // This generous wait time is necessary to compensate for limited resolution of RFC 1123 timestamps
        // and the fact that the system clock is allowed to run "backwards" between threads
        // (cf. http://stackoverflow.com/questions/2978598)
        //
        Thread.sleep( 3000 );

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        long newExpirationTime = expirationTime( result );

        assertTrue( newExpirationTime > initialExpirationTime, "Expiration time was not increased" );
    }

    @Test
    @Documented( "Commit an open transaction\n" +
                 "\n" +
                 "Given you have an open transaction, you can send a commit request. Optionally, you submit additional statements\n" +
                 "along with the request that will be executed before committing the transaction." )
    public void commit_an_open_transaction() throws JsonParseException
    {
        // Given
        String location = POST( txUri() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN id(n)' } ] }" ) )
                .post( txCommitUri() );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        verifyNodeExists( id );
    }

    @Test
    @Documented( "Begin and commit a transaction in one request\n" +
                 "\n" +
                 "If there is no need to keep a transaction open across multiple HTTP requests, you can begin a transaction,\n" +
                 "execute statements, and commit with just a single HTTP request." )
    public void begin_and_commit_a_transaction_in_one_request() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN id(n)' } ] }" ) )
                .post( txCommitUri() );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        verifyNodeExists( id );
    }

    @Test
    @Documented( "Execute multiple statements\n" +
                 "\n" +
                 "You can send multiple Cypher statements in the same request.\n" +
                 "The response will contain the result of each statement." )
    public void execute_multiple_statements() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get().expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN id(n)' }, "
                        + "{ 'statement': 'CREATE (n $props) RETURN n', "
                        + "'parameters': { 'props': { 'name': 'My Node' } } } ] }" ) )
                .post( txCommitUri() );

        // Then
        Map<String,Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        Integer id = resultCell( result, 0, 0 );
        verifyNodeExists( id );
        assertThat( response.entity() ).contains( "My Node" );
    }

    @Test
    @Documented( "Return results in graph format\n" +
                 "\n" +
                 "If you want to understand the graph structure of nodes and relationships returned by your query,\n" +
                 "you can specify the \"graph\" results data format. For example, this is useful when you want to visualise the\n" +
                 "graph structure. The format collates all the nodes and relationships from all columns of the result,\n" +
                 "and also flattens collections of nodes and relationships, including paths." )
    public void return_results_in_graph_format() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{'statements':[{'statement':" +
                        "'CREATE ( bike:Bike { weight: 10 } ) " +
                        "CREATE ( frontWheel:Wheel { spokes: 3 } ) " +
                        "CREATE ( backWheel:Wheel { spokes: 32 } ) " +
                        "CREATE p1 = (bike)-[:HAS { position: 1 } ]->(frontWheel) " +
                        "CREATE p2 = (bike)-[:HAS { position: 2 } ]->(backWheel) " +
                        "RETURN bike, p1, p2', " +
                        "'resultDataContents': ['row','graph']}] }" ) )
                        .post( txCommitUri() );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Map<String, List<Object>> row = graphRow( result, 0 );
        assertEquals( 3, row.get( "nodes" ).size() );
        assertEquals( 2, row.get( "relationships" ).size() );
    }

    @Test
    @Documented( "Rollback an open transaction\n" +
                 "\n" +
                 "Given that you have an open transaction, you can send a rollback request. The server will rollback the\n" +
                 "transaction. Any further statements trying to run in this transaction will fail immediately." )
    public void rollback_an_open_transaction() throws JsonParseException
    {
        // Given
        HTTP.Response firstReq = POST( txUri(),
                HTTP.RawPayload.quotedJson( "{ 'statements': [ { 'statement': 'CREATE (n) RETURN id(n)' } ] }" ) );
        String location = firstReq.location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .delete( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( firstReq, 0, 0 );
        verifyNodeDoesNotExist( id );
    }

    @Test
    @Documented( "Handling errors\n" +
                 "\n" +
                 "The result of any request against the transaction endpoint is streamed back to the client.\n" +
                 "Therefore the server does not know whether the request will be successful or not when it sends the HTTP status\n" +
                 "code.\n" +
                 "\n" +
                 "Because of this, all requests against the transactional endpoint will return 200 or 201 status code, regardless\n" +
                 "of whether statements were successfully executed. At the end of the response payload, the server includes a list\n" +
                 "of errors that occurred while executing statements. If this list is empty, the request completed successfully.\n" +
                 "\n" +
                 "If any errors occur while executing statements, the server will roll back the transaction.\n" +
                 "\n" +
                 "In this example, we send the server an invalid statement to demonstrate error handling.\n" +
                 " \n" +
                 "For more information on the status codes, see <<status-codes>>." )
    public void handling_errors() throws JsonParseException
    {
        // Given
        String location = POST( txUri() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'This is not a valid Cypher Statement.' } ] }" ) )
                .post( txCommitUri() );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertErrors( result, Status.Statement.SyntaxError );
    }

    @Test
    @Documented( "Handling errors in an open transaction\n" +
                "\n" +
                "Whenever there is an error in a request the server will rollback the transaction.\n" +
                  "By inspecting the response for the presence/absence of the `transaction` key you can tell if the " +
                 "transaction is still open" )
    public void errors_in_open_transaction() throws JsonParseException
    {
        // Given
        String location = POST( txUri() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson( "{ 'statements': [ { 'statement': 'This is not a valid Cypher Statement.' } ] }" ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertThat( result ).doesNotContainKey( "transaction" );
    }

    @Test
    @Documented( "Include query statistics\n" +
                 "\n" +
                 "By setting `includeStats` to `true` for a statement, query statistics will be returned for it." )
    public void include_query_statistics() throws JsonParseException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( quotedJson(
                        "{ 'statements': [ { 'statement': 'CREATE (n) RETURN id(n)', 'includeStats': true } ] }" ) )
                .post( txCommitUri() );

        // Then
        Map<String,Object> entity = jsonToMap( response.entity() );
        assertNoErrors( entity );
        Map<String,Object> firstResult = ((List<Map<String,Object>>) entity.get( "results" )).get( 0 );

        assertThat( firstResult ).containsKey( "stats" );
        Map<String,Object> stats = (Map<String,Object>) firstResult.get( "stats" );
        assertThat( stats.get( "nodes_created" ) ).isEqualTo( 1 );
        assertThat( stats.get( "contains_updates" ) ).isEqualTo( true );
        assertThat( stats.get( "contains_system_updates" ) ).isEqualTo( false );
        assertThat( stats.get( "system_updates" ) ).isEqualTo( 0 );
    }

    private void assertNoErrors( Map<String, Object> response )
    {
        assertErrors( response );
    }

    private void assertErrors( Map<String, Object> response, Status... expectedErrors )
    {
        @SuppressWarnings( "unchecked" )
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        Iterator<Status> expected = iterator( expectedErrors );

        while ( expected.hasNext() )
        {
            assertTrue( errors.hasNext() );
            assertThat( errors.next().get( "code" ) ).isEqualTo( expected.next().code().serialize() );
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

    @SuppressWarnings( "unchecked" )
    private <T> T resultCell( Map<String, Object> response, int row, int column )
    {
        Map<String, Object> result = ((List<Map<String, Object>>) response.get( "results" )).get( 0 );
        List<Map<String,List>> data = (List<Map<String,List>>) result.get( "data" );
        return (T) data.get( row ).get( "row" ).get( column );
    }

    @SuppressWarnings( "unchecked" )
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

    private long expirationTime( Map<String, Object> entity )
    {
        String timestampString = (String) ( (Map<?, ?>) entity.get( "transaction" ) ).get( "expires" );
        return ZonedDateTime.parse( timestampString, DateTimeFormatter.RFC_1123_DATE_TIME ).toEpochSecond();
    }

    private void verifyNodeExists( long nodeId )
    {
        ResponseEntity response = getNodeById( nodeId );
        // if at least one node is returned, there will be "node" in the metadata part od the the row
        assertThat( response.entity() ).contains( "node" );
    }

    private void verifyNodeDoesNotExist( long nodeId )
    {
        ResponseEntity response = getNodeById( nodeId );
        assertThat( response.entity() ).doesNotContain( "node" );
    }

    private ResponseEntity getNodeById( long nodeId )
    {
        return gen.get().expectedStatus( 200 ).payload( quotedJson(
                "{ 'statements': [ { 'statement': 'MATCH (n) WHERE ID(n) = $nodeId RETURN n' , " + "'parameters': { 'nodeId': " + nodeId + " } } ] }" ) ).post(
                txCommitUri() );
    }
}
