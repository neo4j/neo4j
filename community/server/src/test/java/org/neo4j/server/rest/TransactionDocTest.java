/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.server.rest;

import static junit.framework.Assert.fail;
import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;
import static org.neo4j.helpers.collection.IteratorUtil.set;
import static org.neo4j.helpers.collection.MapUtil.map;
import static org.neo4j.server.rest.RESTDocsGenerator.ResponseEntity;
import static org.neo4j.server.rest.domain.JsonHelper.createJsonFrom;
import static org.neo4j.server.rest.domain.JsonHelper.jsonToMap;
import static org.neo4j.test.server.HTTP.GET;
import static org.neo4j.test.server.HTTP.POST;
import static org.neo4j.test.server.HTTP.RawPayload.rawPayload;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.junit.Test;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.server.rest.transactional.error.Neo4jError;
import org.neo4j.server.rest.web.PropertyValueException;
import org.neo4j.test.server.HTTP;

public class TransactionDocTest extends AbstractRestFunctionalTestBase
{

    /**
     * Create a transaction
     * <p/>
     * You create a new transaction by posting zero or more cypher statements
     * to the transaction endpoint. The server will respond with the result of
     * your statements, as well as the location of your running transaction.
     */
    @Test
    @Documented
    public void starting_a_transaction() throws PropertyValueException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 201 )
                .payload( createJsonFrom( set( map(
                        "statement", "CREATE (n {props}) RETURN n",
                        "parameters", map( "props", map( "name", "My Node" ) ) ) ) ) )
                .post( getDataUri() + "transaction" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
        Map<String, Object> node = resultCell( result, 0, 0 );
        assertThat( (String) node.get( "name" ), equalTo( "My Node" ) );
    }

    /**
     * Execute statements in running transaction
     * <p/>
     * Given that you have a running transaction, you can post any number of statements to it.
     */
    @Test
    @Documented
    public void execute_statements_in_running_transaction() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction", set() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( createJsonFrom( set( map( "statement", "CREATE n RETURN n" ) ) ) )
                .post( location );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );
    }

    /**
     * Commit a running transaction
     */
    @Test
    @Documented
    public void commit_a_running_transaction() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction", set() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( createJsonFrom( set( map( "statement", "CREATE n RETURN id(n)" ) ) ) )
                .post( location + "/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        assertThat( GET( getNodeUri( id ) ).status(), is( 200 ) );
    }

    /**
     * Create and commit a transaction in one request
     * <p/>
     * This is similar to how the old cypher endpoint behaves.
     */
    @Test
    @Documented
    public void create_and_commit_a_transaction_in_one_request() throws PropertyValueException
    {
        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( createJsonFrom( set( map( "statement", "CREATE n RETURN id(n)" ) ) ) )
                .post( getDataUri() + "transaction/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertNoErrors( result );

        Integer id = resultCell( result, 0, 0 );
        assertThat( GET( getNodeUri( id ) ).status(), is( 200 ) );
    }

    /**
     * Rollback a running transaction
     */
    @Test
    @Documented
    public void rollback_a_running_transaction() throws PropertyValueException
    {
        // Given
        HTTP.Response firstReq = POST( getDataUri() + "transaction", set( map( "statement",
                "CREATE n RETURN id(n)" ) ) );
        String location = firstReq.location();

        // Document
        ResponseEntity response = gen.get()
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
     * <p/>
     * The result of any operation against the transaction resource is streamed back to the client,
     * which means that the server does not know ahead of time if the request will be successful or not.
     * <p/>
     * Because of that, all operations against the transactional resource returns 200 or 201 statuses, always.
     * In order to verify that a request was successful, at the end of each result will be a field called
     * "errors". If this is empty, the operation completed successfully.
     * <p/>
     * If it is not empty, any related transaction will have been rolled back.
     * <p/>
     * In this example, we send the server an invalid statement.
     */
    @Test
    @Documented
    public void handling_errors() throws PropertyValueException
    {
        // Given
        String location = POST( getDataUri() + "transaction", set() ).location();

        // Document
        ResponseEntity response = gen.get()
                .expectedStatus( 200 )
                .payload( createJsonFrom( set( map( "statement", "This is not a valid Cypher Statement." ) ) ) )
                .post( location + "/commit" );

        // Then
        Map<String, Object> result = jsonToMap( response.entity() );
        assertErrors( result, Neo4jError.Code.UNKNOWN_STATEMENT_ERROR );
    }

    //
    // -- Integration tests that are not part of the documentation
    //

    @Test
    public void invalidRequestShouldContainErrorAndHaveNoEffect() throws Exception
    {
        // Given I've started a transaction
        HTTP.Response response = POST( getDataUri() + "transaction", set( map( "statement",
                "CREATE n RETURN id(n)" ) ) );
        Integer nodeId = resultCell( response, 0, 0 );
        String txLocation = response.location();

        // When
        response = POST( txLocation + "/commit", set( map( "statement", "CREATE ;;' RETURN id(n)" ) ) );

        // Then
        assertThat( GET( getNodeUri( nodeId ) ).status(), is( 404 ) );
        assertThat( response.status(), is( 200 ) ); // <-- Because error will happen after streaming starts
        assertErrors( response, Neo4jError.Code.UNKNOWN_STATEMENT_ERROR );
    }

    @Test
    public void invalidJsonInRequestShouldContainErrorAndHaveNoEffect() throws Exception
    {
        // Given I've started a transaction
        HTTP.Response response = POST( getDataUri() + "transaction", set( map( "statement",
                "CREATE n RETURN id(n)" ) ) );
        Integer nodeId = resultCell( response, 0, 0 );
        String txLocation = response.location();

        // When
        response = POST( txLocation + "/commit", rawPayload( "[{asd,::}]" ) );

        // Then
        assertThat( GET( getNodeUri( nodeId ) ).status(), is( 404 ) );
        assertThat( response.status(), is( 200 ) ); // <-- Because error will happen after streaming starts
        assertErrors( response, Neo4jError.Code.INVALID_REQUEST );
    }

    private void assertNoErrors( Map<String, Object> response )
    {
        assertErrors( response );
    }

    private void assertErrors( HTTP.Response response, Neo4jError.Code... expectedErrors )
    {
        assertErrors( response.<Map<String, Object>>content(), expectedErrors );
    }

    private void assertErrors( Map<String, Object> response, Neo4jError.Code... expectedErrors )
    {
        Iterator<Map<String, Object>> errors = ((List<Map<String, Object>>) response.get( "errors" )).iterator();
        Iterator<Neo4jError.Code> expected = iterator( expectedErrors );

        while ( expected.hasNext() )
        {
            assertTrue( errors.hasNext() );
            assertThat( Long.valueOf( (Integer) errors.next().get( "code" ) ), equalTo( expected.next().getCode() ) );
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
        List<List> data = (List<List>) result.get( "data" );
        return (T) data.get( row ).get( column );
    }

}
