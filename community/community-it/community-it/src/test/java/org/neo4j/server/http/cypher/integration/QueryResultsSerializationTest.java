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
package org.neo4j.server.http.cypher.integration;

import com.fasterxml.jackson.databind.JsonNode;
import org.assertj.core.api.Condition;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.time.ZoneId;
import java.time.ZonedDateTime;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Result;
import org.neo4j.graphdb.Transaction;
import org.neo4j.kernel.api.exceptions.Status;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.server.rest.AbstractRestFunctionalTestBase;
import org.neo4j.server.rest.domain.JsonParseException;
import org.neo4j.test.server.HTTP;
import org.neo4j.test.server.HTTP.Response;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.containsNoErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.getJsonNodeWithName;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.graphContainsDeletedNodes;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.graphContainsDeletedRelationships;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.graphContainsNoDeletedEntities;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.hasErrors;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.restContainsDeletedEntities;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.restContainsNoDeletedEntities;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsDeletedEntities;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsDeletedEntitiesInPath;
import static org.neo4j.server.http.cypher.integration.TransactionConditions.rowContainsNoDeletedEntities;
import static org.neo4j.test.server.HTTP.RawPayload.quotedJson;

public class QueryResultsSerializationTest extends AbstractRestFunctionalTestBase
{
    private final HTTP.Builder http = HTTP.withBaseUri( container().getBaseUri() );

    private String commitResource;

    @BeforeEach
    public void setUp()
    {
        // begin
        Response begin = http.POST( txUri() );

        assertThat( begin.status() ).isEqualTo( 201 );
        assertHasTxLocation( begin );
        try
        {
            commitResource = begin.stringFromContent( "commit" );
        }
        catch ( JsonParseException e )
        {
            fail( "Exception caught when setting up test: " + e.getMessage() );
        }
        assertThat( commitResource ).isEqualTo( begin.location() + "/commit" );
    }

    @AfterEach
    public void tearDown()
    {
        // empty the database
        executeTransactionally( "MATCH (n) DETACH DELETE n" );
    }

    @Test
    public void shouldBeAbleToReturnDeletedEntitiesGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsDeletedRelationships( 1 ) );
        assertThat( commit ).satisfies( graphContainsDeletedNodes( 2 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedEntitiesRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( restContainsDeletedEntities( 3 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedEntitiesRow()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s:Start)-[r:R]->(e:End) DELETE s, r, e RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsDeletedEntities( 2, 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldNotMarkNormalEntitiesAsDeletedGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsNoDeletedEntities() );
        assertThat( commit.status() ).isEqualTo( 200 );
    }

    @Test
    public void shouldNotMarkNormalEntitiesAsDeletedRow()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsNoDeletedEntities() );
        assertThat( commit.status() ).isEqualTo( 200 );
    }

    @Test
    public void shouldNotMarkNormalEntitiesAsDeletedRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s:Start)-[r:R]->(e:End) RETURN *" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( restContainsNoDeletedEntities() );
        assertThat( commit.status() ).isEqualTo( 200 );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesGraph()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsDeletedNodes( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesRow()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsDeletedEntities( 1, 0 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedNodesRest()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN n" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( restContainsDeletedEntities( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsRow()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsDeletedEntities( 0, 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void shouldBeAbleToReturnDeletedRelationshipsRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s)-[r:R]->(e) DELETE r RETURN r" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( restContainsDeletedEntities( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeGraph()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeRow()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedNodeRest()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete {p: 'a property'})" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN n.p" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeGraph()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeRow()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnLabelsOfDeletedNodeRest()
    {
        // given
        executeTransactionally( "CREATE (:NodeToDelete)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n:NodeToDelete) DELETE n RETURN labels(n)" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (s)-[r:R]->(e) DELETE r RETURN r.p" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipRow()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (s)-[r:R]->(e) DELETE r RETURN r.p" ) );

        assertThat( commit ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void shouldFailIfTryingToReturnPropsOfDeletedRelationshipRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:MARKER {p: 'a property'}]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (s)-[r:MARKER]->(e) DELETE r RETURN r.p" ) );

        assertThat( commit ).as( "Error raw response: " + commit.rawContent() ).satisfies( hasErrors( Status.Statement.EntityNotFound ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 2L );
    }

    @Test
    public void returningADeletedPathGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsDeletedNodes( 2 ) );
        assertThat( commit ).satisfies( graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void returningAPartiallyDeletedPathGraph()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( graphContainsDeletedNodes( 1 ) );
        assertThat( commit ).satisfies( graphContainsDeletedRelationships( 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void returningADeletedPathRow()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsDeletedEntitiesInPath( 2, 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void returningAPartiallyDeletedPathRow()
    {
        // given
        String query = "CREATE (:Start)-[:R]->(:End)";
        executeTransactionally( query );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit ).satisfies( rowContainsDeletedEntitiesInPath( 1, 1 ) );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    private void executeTransactionally( String query )
    {
        GraphDatabaseService database = graphdb();
        try ( Transaction transaction = database.beginTx() )
        {
            transaction.execute( query );
            transaction.commit();
        }
    }

    @Test
    public void returningADeletedPathRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH p=(s)-[r:R]->(e) DELETE p RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void returningAPartiallyDeletedPathRest()
    {
        // given
        executeTransactionally( "CREATE (:Start)-[:R]->(:End)" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH p=(s)-[r:R]->(e) DELETE s,r RETURN p" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( nodesInDatabase() ).isEqualTo( 1L );
    }

    @Test
    public void nestedShouldWorkGraph()
    {
        // given
        executeTransactionally( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonGraph( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( commit ).satisfies( graphContainsDeletedNodes( 1 ) );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void nestedShouldWorkRest()
    {
        // given
        executeTransactionally( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRest( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( commit ).satisfies( restContainsNestedDeleted() );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void nestedShouldWorkRow()
    {
        // given
        executeTransactionally( "CREATE ()" );

        // execute and commit
        Response commit = http.POST( commitResource,
                queryAsJsonRow( "MATCH (n) DELETE (n) RETURN [n, {someKey: n}]" ) );

        assertThat( commit ).satisfies( containsNoErrors() );
        assertThat( commit.status() ).isEqualTo( 200 );
        assertThat( commit ).satisfies( rowContainsDeletedEntities( 2, 0 ) );
        assertThat( nodesInDatabase() ).isEqualTo( 0L );
    }

    @Test
    public void shouldHandleTemporalArrays() throws Exception
    {
        //Given
        var db = getDefaultDatabase();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "date", new ZonedDateTime[]{date} );
            tx.commit();
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
        var db = getDefaultDatabase();
        Duration duration = Duration.ofSeconds( 73 );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "duration", new Duration[]{duration} );
            tx.commit();
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
        var db = getDefaultDatabase();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "date", date );
            tx.commit();
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
        var db = getDefaultDatabase();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "duration", duration );
            tx.commit();
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
        var db = getDefaultDatabase();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "dates", new ZonedDateTime[]{date} );
            tx.commit();
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
        var db = getDefaultDatabase();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "durations", new Duration[]{duration} );
            tx.commit();
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
        var db = getDefaultDatabase();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "date", date );
            tx.commit();
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
        var db = getDefaultDatabase();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "duration", duration );
            tx.commit();
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
        var db = getDefaultDatabase();
        ZonedDateTime date = ZonedDateTime.of( 1980, 3, 11, 0, 0,
                0, 0, ZoneId.of( "Europe/Stockholm" ) );
        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "dates", new ZonedDateTime[]{date} );
            tx.commit();
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
        var db = getDefaultDatabase();
        Duration duration = Duration.ofSeconds( 73 );

        try ( Transaction tx = db.beginTx() )
        {
            Node node = tx.createNode( label( "N" ) );
            node.setProperty( "durations", new Duration[]{duration} );
            tx.commit();
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
        GraphDatabaseService database = graphdb();
        try ( Transaction transaction = database.beginTx() )
        {
            try ( Result r = transaction.execute( "MATCH (n) RETURN count(n) AS c" ) )
            {
                return (Long) r.columnAs( "c" ).next();
            }
        }
    }

    private GraphDatabaseAPI getDefaultDatabase()
    {
        return container().getDefaultDatabase();
    }

    /**
     * This condition is hardcoded to check for a list containing one deleted node and one map with a
     * deleted node mapped to the key `someKey`.
     */
    private static Condition<? super Response> restContainsNestedDeleted()
    {
        return new Condition<>( response ->
        {
            try
            {
                JsonNode list = getJsonNodeWithName( response, "rest" ).iterator().next();

                assertThat( list.get( 0 ).get( "metadata" ).get( "deleted" ).asBoolean() ).isEqualTo( Boolean.TRUE );
                assertThat( list.get( 1 ).get( "someKey" ).get( "metadata" ).get( "deleted" ).asBoolean() ).isEqualTo( Boolean.TRUE );

                return true;
            }
            catch ( JsonParseException e )
            {
                return false;
            }
        }, "Contains deleted data." );
    }
}
