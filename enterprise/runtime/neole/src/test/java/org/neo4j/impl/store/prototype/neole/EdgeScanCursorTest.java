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
package org.neo4j.impl.store.prototype.neole;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.junit.ClassRule;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;
import org.neo4j.impl.kernel.api.EdgeScanCursor;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class EdgeScanCursorTest
{
    private static List<Long> EDGE_IDS;
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
                EdgeScanCursorTest.c = c.getId();
                EdgeScanCursorTest.d = d.getId();

                d.createRelationshipTo( e, withName( "TRIANGLE" ) );
                e.createRelationshipTo( f, withName( "TRIANGLE" ) );
                f.createRelationshipTo( d, withName( "TRIANGLE" ) );

                loop = a.createRelationshipTo( a, withName( "LOOP" ) ).getId();

                tx.success();
            }

            EDGE_IDS = new ArrayList<>();
            try ( Transaction tx = graphDb.beginTx() )
            {
                deleted.delete();
                long time = System.nanoTime();
                for ( Relationship relationship : graphDb.getAllRelationships() )
                {
                    EDGE_IDS.add( relationship.getId() );
                }
                time = System.nanoTime() - time;
                System.out.printf( "neo4j scan time: %.3fms%n", time / 1_000_000.0 );

                tx.success();
            }
        }

        @Override
        protected void cleanup()
        {
            EDGE_IDS = null;
        }
    }.withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldScanEdges() throws Exception
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( EdgeScanCursor edges = graph.allocateEdgeScanCursor() )
        {
            // when
            long time = System.nanoTime();
            graph.allEdgesScan( edges );
            while ( edges.next() )
            {
                ids.add( edges.edgeReference() );
            }
            time = System.nanoTime() - time;
            System.out.printf( "cursor scan time: %.3fms%n", time / 1_000_000.0 );
        }

        assertEquals( EDGE_IDS, ids );
    }

    @Test
    public void shouldAccessEdgeByReference() throws Exception
    {
        // given
        try ( EdgeScanCursor edges = graph.allocateEdgeScanCursor() )
        {
            for ( long id : EDGE_IDS )
            {
                // when
                graph.singleEdge( id, edges );

                // then
                assertTrue( "should access defined edge", edges.next() );
                assertEquals( "should access the correct edge", id, edges.edgeReference() );
                assertFalse( "should only access a single edge", edges.next() );
            }
        }
    }

    @Test
    public void shouldNotAccessDeletedEdge() throws Exception
    {
        // given
        try ( EdgeScanCursor edges = graph.allocateEdgeScanCursor() )
        {
            // when
            graph.singleEdge( none, edges );

            // then
            assertFalse( "should not access deleted edge", edges.next() );
        }
    }

    @Test
    public void shouldAccessEdgeLabels() throws Exception
    {
        // given
        Map<Integer,Integer> counts = new HashMap<>();

        try ( EdgeScanCursor edges = graph.allocateEdgeScanCursor() )
        {
            // when
            graph.allEdgesScan( edges );
            while ( edges.next() )
            {
                counts.compute( edges.label(), ( k, v ) -> v == null ? 1 : v + 1 );
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
        assertArrayEquals( new int[] {1, 6, 6}, values );
    }

    @Test
    public void shouldAccessNodes() throws Exception
    {
        // given
        try ( EdgeScanCursor edges = graph.allocateEdgeScanCursor() )
        {
            // when
            graph.singleEdge( one, edges );

            // then
            assertTrue( edges.next() );
            assertEquals( c, edges.sourceNodeReference() );
            assertEquals( d, edges.targetNodeReference() );
            assertFalse( edges.next() );

            // when
            graph.singleEdge( loop, edges );

            // then
            assertTrue( edges.next() );
            assertEquals( edges.sourceNodeReference(), edges.targetNodeReference() );
            assertFalse( edges.next() );
        }
    }
}
