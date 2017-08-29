/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.store.prototype.neole;

import org.junit.ClassRule;
import org.junit.Test;

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

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class RelationshipScanCursorTest
{
    private static List<Long> RELATIONSHIP_IDS;
    private static long none, loop, one, c, d;
    @ClassRule
    public static final GraphSetup graph = new GraphSetup()
    {
        @Override
        protected void create( GraphDatabaseService graphDb )
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
                RelationshipScanCursorTest.c = c.getId();
                RelationshipScanCursorTest.d = d.getId();

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
                long time = System.nanoTime();
                for ( Relationship relationship : graphDb.getAllRelationships() )
                {
                    RELATIONSHIP_IDS.add( relationship.getId() );
                }
                time = System.nanoTime() - time;
                System.out.printf( "neo4j scan time: %.3fms%n", time / 1_000_000.0 );

                tx.success();
            }
        }

        @Override
        protected void cleanup()
        {
            RELATIONSHIP_IDS = null;
        }
    }.withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldScanRelationships() throws Exception
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( RelationshipScanCursor relationships = graph.allocateRelationshipScanCursor() )
        {
            // when
            long time = System.nanoTime();
            graph.allRelationshipsScan( relationships );
            while ( relationships.next() )
            {
                ids.add( relationships.relationshipReference() );
            }
            time = System.nanoTime() - time;
            System.out.printf( "cursor scan time: %.3fms%n", time / 1_000_000.0 );
        }

        assertEquals( RELATIONSHIP_IDS, ids );
    }

    @Test
    public void shouldAccessRelationshipByReference() throws Exception
    {
        // given
        try ( RelationshipScanCursor relationships = graph.allocateRelationshipScanCursor() )
        {
            for ( long id : RELATIONSHIP_IDS )
            {
                // when
                graph.singleRelationship( id, relationships );

                // then
                assertTrue( "should access defined relationship", relationships.next() );
                assertEquals( "should access the correct relationship", id, relationships.relationshipReference() );
                assertFalse( "should only access a single relationship", relationships.next() );
            }
        }
    }

    @Test
    public void shouldNotAccessDeletedRelationship() throws Exception
    {
        // given
        try ( RelationshipScanCursor relationships = graph.allocateRelationshipScanCursor() )
        {
            // when
            graph.singleRelationship( none, relationships );

            // then
            assertFalse( "should not access deleted relationship", relationships.next() );
        }
    }

    @Test
    public void shouldAccessRelationshipLabels() throws Exception
    {
        // given
        Map<Integer,Integer> counts = new HashMap<>();

        try ( RelationshipScanCursor relationships = graph.allocateRelationshipScanCursor() )
        {
            // when
            graph.allRelationshipsScan( relationships );
            while ( relationships.next() )
            {
                counts.compute( relationships.label(), ( k, v ) -> v == null ? 1 : v + 1 );
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
    public void shouldAccessNodes() throws Exception
    {
        assumeThat( "x86_64", equalTo( System.getProperty( "os.arch" ) ) );
        // given
        try ( RelationshipScanCursor relationships = graph.allocateRelationshipScanCursor() )
        {
            // when
            graph.singleRelationship( one, relationships );

            // then
            assertTrue( relationships.next() );
            assertEquals( c, relationships.sourceNodeReference() );
            assertEquals( d, relationships.targetNodeReference() );
            assertFalse( relationships.next() );

            // when
            graph.singleRelationship( loop, relationships );

            // then
            assertTrue( relationships.next() );
            assertEquals( relationships.sourceNodeReference(), relationships.targetNodeReference() );
            assertFalse( relationships.next() );
        }
    }
}
