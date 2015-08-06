/*
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
package org.neo4j.ndp.messaging.v1.infrastructure;

import org.junit.Test;

import org.neo4j.graphdb.Path;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

import static org.neo4j.ndp.messaging.v1.example.Support.nodes;
import static org.neo4j.ndp.messaging.v1.example.Support.relationships;
import static org.neo4j.ndp.messaging.v1.example.Nodes.ALICE;
import static org.neo4j.ndp.messaging.v1.example.Nodes.BOB;
import static org.neo4j.ndp.messaging.v1.example.Nodes.CAROL;
import static org.neo4j.ndp.messaging.v1.example.Nodes.DAVE;
import static org.neo4j.ndp.messaging.v1.example.Paths.PATH_WITH_LENGTH_ONE;
import static org.neo4j.ndp.messaging.v1.example.Paths.PATH_WITH_LENGTH_TWO;
import static org.neo4j.ndp.messaging.v1.example.Paths.PATH_WITH_LENGTH_ZERO;
import static org.neo4j.ndp.messaging.v1.example.Paths.PATH_WITH_LOOP;
import static org.neo4j.ndp.messaging.v1.example.Paths.PATH_WITH_NODES_VISITED_MULTIPLE_TIMES;
import static org.neo4j.ndp.messaging.v1.example.Paths
        .PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION;
import static org.neo4j.ndp.messaging.v1.example.Paths
        .PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_KNOWS_BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.ALICE_LIKES_CAROL;
import static org.neo4j.ndp.messaging.v1.example.Relationships.CAROL_DISLIKES_BOB;
import static org.neo4j.ndp.messaging.v1.example.Relationships.CAROL_MARRIED_TO_DAVE;
import static org.neo4j.ndp.messaging.v1.example.Relationships.DAVE_WORKS_FOR_DAVE;

public class ValuePathTest
{

    @Test
    public void canConstructPathWithLengthZero()
    {
        // Given A
        Path path = PATH_WITH_LENGTH_ZERO;

        // Then
        assertThat( path.length(), equalTo( 0 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( ALICE ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE ) ) );
        assertThat( path.lastRelationship(), nullValue() );
        assertThat( relationships( path ), equalTo( relationships() ) );
    }

    @Test
    public void canConstructPathWithLengthOne()
    {
        // Given A->B
        Path path = PATH_WITH_LENGTH_ONE;

        // Then
        assertThat( path.length(), equalTo( 1 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( BOB ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE, BOB ) ) );
        assertThat( path.lastRelationship(), equalTo( ALICE_KNOWS_BOB ) );
        assertThat( relationships( path ), equalTo( relationships( ALICE_KNOWS_BOB ) ) );
    }

    @Test
    public void canConstructPathWithLengthTwo()
    {
        // Given A->C->D
        Path path = PATH_WITH_LENGTH_TWO;

        // Then
        assertThat( path.length(), equalTo( 2 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( DAVE ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE, CAROL, DAVE ) ) );
        assertThat( path.lastRelationship(), equalTo( CAROL_MARRIED_TO_DAVE ) );
        assertThat( relationships( path ), equalTo(
                relationships( ALICE_LIKES_CAROL, CAROL_MARRIED_TO_DAVE ) ) );
    }

    @Test
    public void canConstructPathWithRelationshipTraversedAgainstItsDirection()
    {
        // Given A->B<-C->D
        Path path = PATH_WITH_RELATIONSHIP_TRAVERSED_AGAINST_ITS_DIRECTION;

        // Then
        assertThat( path.length(), equalTo( 3 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( DAVE ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE, BOB, CAROL, DAVE ) ) );
        assertThat( path.lastRelationship(), equalTo( CAROL_MARRIED_TO_DAVE ) );
        assertThat( relationships( path ), equalTo(
                relationships( ALICE_KNOWS_BOB, CAROL_DISLIKES_BOB, CAROL_MARRIED_TO_DAVE ) ) );
    }

    @Test
    public void canConstructPathWithNodesVisitedMultipleTimes()
    {
        // Given A->B<-A->C->B<-C
        Path path = PATH_WITH_NODES_VISITED_MULTIPLE_TIMES;

        // Then
        assertThat( path.length(), equalTo( 5 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( CAROL ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE, BOB, ALICE, CAROL, BOB, CAROL ) ) );
        assertThat( path.lastRelationship(), equalTo( CAROL_DISLIKES_BOB ) );
        assertThat( relationships( path ), equalTo(
                relationships( ALICE_KNOWS_BOB, ALICE_KNOWS_BOB, ALICE_LIKES_CAROL,
                        CAROL_DISLIKES_BOB, CAROL_DISLIKES_BOB ) ) );
    }

    @Test
    public void canConstructPathWithRelationshipTraversedMultipleTimesInSameDirection()
    {
        // Given A->C->B<-A->C->D
        Path path = PATH_WITH_RELATIONSHIP_TRAVERSED_MULTIPLE_TIMES_IN_SAME_DIRECTION;

        // Then
        assertThat( path.length(), equalTo( 5 ) );
        assertThat( path.startNode(), equalTo( ALICE ) );
        assertThat( path.endNode(), equalTo( DAVE ) );
        assertThat( nodes( path ), equalTo( nodes( ALICE, CAROL, BOB, ALICE, CAROL, DAVE ) ) );
        assertThat( path.lastRelationship(), equalTo( CAROL_MARRIED_TO_DAVE ) );
        assertThat( relationships( path ), equalTo(
                relationships( ALICE_LIKES_CAROL, CAROL_DISLIKES_BOB, ALICE_KNOWS_BOB,
                        ALICE_LIKES_CAROL, CAROL_MARRIED_TO_DAVE ) ) );
    }

    @Test
    public void canConstructPathWithLoop()
    {
        // Given C->D->D
        Path path = PATH_WITH_LOOP;

        // Then
        assertThat( path.length(), equalTo( 2 ) );
        assertThat( path.startNode(), equalTo( CAROL ) );
        assertThat( path.endNode(), equalTo( DAVE ) );
        assertThat( nodes( path ), equalTo( nodes( CAROL, DAVE, DAVE ) ) );
        assertThat( path.lastRelationship(), equalTo( DAVE_WORKS_FOR_DAVE ) );
        assertThat( relationships( path ), equalTo(
                relationships( CAROL_MARRIED_TO_DAVE, DAVE_WORKS_FOR_DAVE ) ) );
    }

}