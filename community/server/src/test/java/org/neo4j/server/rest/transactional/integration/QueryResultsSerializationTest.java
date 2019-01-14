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
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.spatial.Point;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.impl.factory.GraphDatabaseFacade;
import org.neo4j.kernel.impl.store.GeometryType;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.containsNoErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsDeletedNodes;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsDeletedRelationships;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.graphContainsNoDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.hasErrors;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.matches;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.restContainsDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.restContainsNoDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsDeletedEntities;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsDeletedEntitiesInPath;
import static org.neo4j.server.rest.transactional.integration.TransactionMatchers.rowContainsNoDeletedEntities;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;
import static org.neo4j.values.storable.CoordinateReferenceSystem.WGS84;
import static org.neo4j.values.storable.Values.pointValue;

public class QueryResultsSerializationTest extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( server().baseUri() );

    private String commitResource;

    @Before
    public void setUp()
    {
        // begin
        Response begin = http.POST( "db/data/transaction" );

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
    public void shouldBeAbleToReturnDeletedEntitiesRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, restContainsDeletedEntities( 3 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
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
        assertThat( commit, rowContainsDeletedEntities( 2, 1 ) );
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
    public void shouldNotMarkNormalEntitiesAsDeletedRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, restContainsNoDeletedEntities() );
        assertThat( commit.status(), equalTo( 200 ) );
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
        assertThat( commit, rowContainsDeletedEntities( 1, 0 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesRest()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, restContainsDeletedEntities( 1 ) );
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
        assertThat( commit, rowContainsDeletedEntities( 0, 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, restContainsDeletedEntities( 1 ) );
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
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeRest()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

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
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeRest()
    {
        // given
        graphdb().execute( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

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
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:MARKER {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s)-[r:MARKER]->(e) DELETE r RETURN r.p" ) );

        assertThat( "Error raw response: " + commit.rawContent(), commit,
                hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 2L ) );
    }

    @Test
    public void returningADeletedPathGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsDeletedNodes( 2 ) );
        assertThat( commit, graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void returningAPartiallyDeletedPathGraph()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, graphContainsDeletedNodes( 1 ) );
        assertThat( commit, graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void returningADeletedPathRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsDeletedEntitiesInPath( 2, 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void returningAPartiallyDeletedPathRow()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit, rowContainsDeletedEntitiesInPath( 1, 1 ) );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void returningADeletedPathRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void returningAPartiallyDeletedPathRest()
    {
        // given
        graphdb().execute( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( nodesInDatabase(), equalTo( 1L ) );
    }

    @Test
    public void nestedShouldWorkGraph()
    {
        // given
        graphdb().execute( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( commit, graphContainsDeletedNodes( 1 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void nestedShouldWorkRest()
    {
        // given
        graphdb().execute( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( commit, restContainsNestedDeleted() );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void nestedShouldWorkRow()
    {
        // given
        graphdb().execute( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit, containsNoErrors() );
        assertThat( commit.status(), equalTo( 200 ) );
        assertThat( commit, rowContainsDeletedEntities( 2, 0 ) );
        assertThat( nodesInDatabase(), equalTo( 0L ) );
    }

    @Test
    public void shouldHandleTemporalArrays() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "date", new ZonedDateTime[]{date} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "row" ).get( 0 )
                .get( "date" ).get( 0 );

        assertEquals( "\"1980-03-11T00:00+01:00[Europe/Stockholm]\"", row.toString() );
    }

    @Test
    public void shouldHandleDurationArrays() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        Duration duration = Duration.ofSeconds( 73 );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "duration", new Duration[]{duration} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "row" ).get( 0 )
                .get( "duration" ).get( 0 );

        assertEquals( "\"PT1M13S\"", row.toString() );
    }

    @Test
    public void shouldHandleTemporalUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "date", date );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "date" );
        assertEquals( "\"1980-03-11T00:00+01:00[Europe/Stockholm]\"", row.toString() );
    }

    @Test
    public void shouldHandleDurationUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "duration", duration );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "duration" );
        assertEquals( "\"PT1M13S\"", row.toString() );
    }

    @Test
    public void shouldHandleTemporalArraysUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "dates", new ZonedDateTime[]{date} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "dates" ).get(0);
        assertEquals( "\"1980-03-11T00:00+01:00[Europe/Stockholm]\"", row.toString() );
    }

    @Test
    public void shouldHandleDurationArraysUsingRestResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "durations", new Duration[]{duration} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "rest" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "rest" )
                .get( 0 ).get( "data" ).get( "durations" ).get( 0 );
        assertEquals( "\"PT1M13S\"", row.toString() );
    }

    @Test
    public void shouldHandleTemporalUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "date", date );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );
        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get("nodes").get( 0 ).get( "properties" ).get( "date" );
        assertEquals( "\"1980-03-11T00:00+01:00[Europe/Stockholm]\"", row.toString() );
    }

    @Test
    public void shouldHandleDurationUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "duration", duration );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get("nodes").get( 0 ).get( "properties" ).get( "duration" );
        assertEquals( "\"PT1M13S\"", row.toString() );
    }

    @Test
    public void shouldHandleTemporalArraysUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "dates", new ZonedDateTime[]{date} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get( "nodes" ).get( 0 ).get( "properties" ).get( "dates" ).get( 0 );
        assertEquals( "\"1980-03-11T00:00+01:00[Europe/Stockholm]\"", row.toString() );
    }

    @Test
    public void shouldHandleDurationArraysUsingGraphResultDataContent() throws Exception
    {
        //Given
        GraphDatabaseFacade db = server().getDatabase().getGraph();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = db.createNode( label( "N" ) );
            node.setProperty( "durations", new Duration[]{duration} );
            tx.success();
        }

        //When
        HTTP.Response response = runQuery( "MATCH (n:N) RETURN n", "graph" );

        //Then
        assertEquals( 200, response.status() );
        assertNoErrors( response );

        JsonNode row = response.get( "results" ).get( 0 ).get( "data" ).get( 0 ).get( "graph" )
                .get("nodes").get( 0 ).get( "properties" ).get( "durations" ).get( 0 );
        assertEquals( "\"PT1M13S\"", row.toString() );
    }

    private HTTP.RawPayload queryAsJsonGraph( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'graph' ] } ] }" );
    }

    private HTTP.RawPayload queryAsJsonRest( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'rest' ] } ] }" );
    }

    private HTTP.RawPayload queryAsJsonRow( String query )
    {
        return quotedJson( "{ 'statements': [ { 'statement': '" + query + "', 'resultDataContents': [ 'row' ] } ] }" );
    }

    private long nodesInDatabase()
    {
        Result r = graphdb().execute( "MATCH (n) RETURN count(n) AS c" );
        Long nodes = (Long) r.columnAs( "c" ).next();
        r.close();
        return nodes;
    }

    private void assertHasTxLocation( Response begin )
    {
        assertThat( begin.location(), matches( "http://localhost:\\d+/db/data/transaction/\\d+" ) );
    }

    /**
     * This matcher is hardcoded to check for a list containing one deleted node and one map with a
     * deleted node mapped to the key `someKey`.
     */
    private static Matcher<? super Response> restContainsNestedDeleted()
    {
        return new TypeSafeMatcher<Response>()
        {
            @Override
            protected boolean matchesSafely( HTTP.Response response )
            {
                try
                {
                    JsonNode list = TransactionMatchers.getJsonNodeWithName( response, "rest" ).iterator().next();

                    assertThat( list.get( 0 ).get( "metadata" ).get( "deleted" ).asBoolean(), equalTo( Boolean.TRUE ) );
                    assertThat( list.get( 1 ).get( "someKey" ).get( "metadata" ).get( "deleted" ).asBoolean(),
                            equalTo( Boolean.TRUE ) );

                    return true;
                }
                catch ( JsonParseException e )
                {
                    return false;
                }
            }

            @Override
            public void describeTo( Description description )
            {
            }
        };
    }
}
