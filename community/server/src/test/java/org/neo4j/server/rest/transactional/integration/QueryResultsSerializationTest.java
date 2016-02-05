/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.server.rest.transactional.integration;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import org.neo4j.graphdb.Result;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.fail;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsDeletedNodes;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsDeletedRelationships;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsNoDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.matches;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsNoDeletedEntities;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class QueryResultsSerializationTest extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( "http://localhost:7474" );

    private String commitResource;

    @Before
    public void setUp()
    {
        // begin
        Response begin = http.POST( "/db/data/transaction" );

        assertThat( begin.status(), equalTo( 201 ) );
        assertHasTxLocation( begin );
        try
        {
            commitResource = begin.stringFromContent( "commit" );
        }
        catch ( JsonParseException e )
        {
            fail( "Exception caught when setting up test: " + e.getMessage() );
        }
        assertThat( commitResource, equalTo( begin.location() + "/commit" ) );
    }

    @After
    public void tearDown()
    {
        // empty the database
        graphdb().execute( "MATCH (n) DETACH DELETE n" );
    }

    @Test
    public void shouldBeAbleToReturnDeletedEntitiesGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsDeletedRelationships( 1 ) );
        assertThat( commit, graphContainsDeletedNodes( 2 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldNotMarkNormalEntitiesAsDeletedGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsNoDeletedEntities() );
        assertThat( commit.status(), equalTo( 200 ) );
    }

    @Test
    public void shouldNotMarkNormalEntitiesAsDeletedRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsNoDeletedEntities() );
        assertThat( commit.status(), equalTo( 200 ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedEntitiesRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsDeletedEntities( 3 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesGraph()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsDeletedNodes( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesRow()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsDeletedEntities( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsDeletedEntities( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeGraph()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeRow()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeGraph()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeRow()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s)-[r:R]->(e) DELETE r RETURN r.p" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s)-[r:R]->(e) DELETE r RETURN r.p" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnTypeOfDeletedRelationshipGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s)-[r:R]->(e) DELETE r RETURN type(r)" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldFailIfTryingToReturnTypeOfDeletedRelationshipRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s)-[r:R]->(e) DELETE r RETURN type(r)" ) );

        assertThat( commit, hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    private HTTP.RawPayload queryAsJsonGraph( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'graph' ] } ] }" );
    }

    private HTTP.RawPayload queryAsJsonRow( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'row' ] } ] }" );
    }

    private long nodesInDatabase()
    {
        Result r = graphdb().execute( "MATCH (n) RETURN count(n) AS c" );
        return (Long) r.columnAs( "c" ).next();
    }

    private void assertHasTxLocation( Response begin )
    {
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );
    }
}
