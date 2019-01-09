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
package org.neo4j.kernel.impl.newapi;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.RelationshipScanCursor;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class RelationshipScanCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> RELATIONSHIP_IDS;
    private static long none, loop, one, c, d;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        Relationship deleted;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node a = graphDb.createNode(), b = graphDb.createNode(), c = graphDb.createNode(),
                    d = graphDb.createNode(), e = graphDb.createNode(), f = graphDb.createNode();

            a.createRelationshipTo( b, withName( "CIRCLE" ) );
            b.createRelationshipTo( c, withName( "CIRCLE" ) );
            one = c.createRelationshipTo( d, withName( "CIRCLE" ) ).getId();
            d.createRelationshipTo( e, withName( "CIRCLE" ) );
            e.createRelationshipTo( f, withName( "CIRCLE" ) );
            f.createRelationshipTo( a, withName( "CIRCLE" ) );

            a.createRelationshipTo( b, withName( "TRIANGLE" ) );
            a.createRelationshipTo( c, withName( "TRIANGLE" ) );
            b.createRelationshipTo( c, withName( "TRIANGLE" ) );
            none = (deleted = c.createRelationshipTo( b, withName( "TRIANGLE" ) )).getId();
            RelationshipScanCursorTestBase.c = c.getId();
            RelationshipScanCursorTestBase.d = d.getId();

            d.createRelationshipTo( e, withName( "TRIANGLE" ) );
            e.createRelationshipTo( f, withName( "TRIANGLE" ) );
            f.createRelationshipTo( d, withName( "TRIANGLE" ) );

            loop = a.createRelationshipTo( a, withName( "LOOP" ) ).getId();

            tx.success();
        }

        RELATIONSHIP_IDS = new ArrayList<>();
        try ( Transaction tx = graphDb.beginTx() )
        {
            deleted.delete();
            for ( Relationship relationship : graphDb.getAllRelationships() )
            {
                RELATIONSHIP_IDS.add( relationship.getId() );
            }
            tx.success();
        }
    }

    @Test
    void shouldScanRelationships()
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            read.allRelationshipsScan( relationships );
            while ( relationships.next() )
            {
                ids.add( relationships.relationshipReference() );
            }
        }

        assertEquals( RELATIONSHIP_IDS, ids );
    }

    @Test
    void shouldAccessRelationshipByReference()
    {
        // given
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            for ( long id : RELATIONSHIP_IDS )
            {
                // when
                read.singleRelationship( id, relationships );

                // then
                assertTrue( relationships.next(), "should access defined relationship" );
                assertEquals( id, relationships.relationshipReference(), "should access the correct relationship" );
                assertFalse( relationships.next(), "should only access a single relationship" );
            }
        }
    }

    @Test
    void shouldNotAccessDeletedRelationship()
    {
        // given
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            read.singleRelationship( none, relationships );

            // then
            assertFalse( relationships.next(), "should not access deleted relationship" );
        }
    }

    // This is functionality which is only required for the hacky db.schema not to leak real data
    @Test
    void shouldNotAccessNegativeReferences()
    {
        // given
        try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
        {
            // when
            read.singleRelationship( -2L, relationship );

            // then
            assertFalse( relationship.next(), "should not access negative reference relationship" );
        }
    }

    @Test
    void shouldAccessRelationshipLabels()
    {
        // given
        Map<Integer,Integer> counts = new HashMap<>();

        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            read.allRelationshipsScan( relationships );
            while ( relationships.next() )
            {
                counts.compute( relationships.type(), ( k, v ) -> v == null ? 1 : v + 1 );
            }
        }

        // then
        assertEquals( 3, counts.size() );
        int[] values = new int[3];
        int i = 0;
        for ( int value : counts.values() )
        {
            values[i++] = value;
        }
        Arrays.sort( values );
        assertArrayEquals( new int[]{1, 6, 6}, values );
    }

    @Test
    void shouldAccessNodes()
    {
        // given
        try ( RelationshipScanCursor relationships = cursors.allocateRelationshipScanCursor() )
        {
            // when
            read.singleRelationship( one, relationships );

            // then
            assertTrue( relationships.next() );
            assertEquals( c, relationships.sourceNodeReference() );
            assertEquals( d, relationships.targetNodeReference() );
            assertFalse( relationships.next() );

            // when
            read.singleRelationship( loop, relationships );

            // then
            assertTrue( relationships.next() );
            assertEquals( relationships.sourceNodeReference(), relationships.targetNodeReference() );
            assertFalse( relationships.next() );
        }
    }
}
