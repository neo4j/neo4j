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
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.LabelSet;
import org.neo4j.internal.kernel.api.NodeCursor;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assume.assumeThat;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class NodeCursorTest
{
    private static List<Long> NODE_IDS;
    private static long foo, bar, baz, barbaz, bare, gone;
    @ClassRule
    public static final GraphSetup graph = new GraphSetup()
    {
        @Override
        protected void create( GraphDatabaseService graphDb )
        {
            Node deleted;
            try ( Transaction tx = graphDb.beginTx() )
            {
                foo = graphDb.createNode( label( "Foo" ) ).getId();
                bar = graphDb.createNode( label( "Bar" ) ).getId();
                baz = graphDb.createNode( label( "Baz" ) ).getId();
                barbaz = graphDb.createNode( label( "Bar" ), label( "Baz" ) ).getId();
                gone = (deleted = graphDb.createNode()).getId();
                bare = graphDb.createNode().getId();

                tx.success();
            }

            try ( Transaction tx = graphDb.beginTx() )
            {
                deleted.delete();

                tx.success();
            }

            try ( Transaction tx = graphDb.beginTx() )
            {
                NODE_IDS = new ArrayList<>();
                long time = System.nanoTime();
                for ( Node node : graphDb.getAllNodes() )
                {
                    NODE_IDS.add( node.getId() );
                }
                time = System.nanoTime() - time;
                System.out.printf( "neo4j scan time: %.3fms%n", time / 1_000_000.0 );
                tx.success();
            }
        }

        @Override
        protected void cleanup()
        {
            NODE_IDS = null;
        }
    }
            .withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldScanNodes() throws Exception
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = graph.allocateNodeCursor() )
        {
            // when
            long time = System.nanoTime();
            graph.allNodesScan( nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
            time = System.nanoTime() - time;
            System.out.printf( "cursor scan time: %.3fms%n", time / 1_000_000.0 );
        }

        // then
        assertEquals( NODE_IDS, ids );
    }

    @Test
    public void shouldAccessNodesByReference() throws Exception
    {
        // given
        try ( NodeCursor nodes = graph.allocateNodeCursor() )
        {
            for ( long id : NODE_IDS )
            {
                // when
                graph.singleNode( id, nodes );

                // then
                assertTrue( "should access defined node", nodes.next() );
                assertEquals( "should access the correct node", id, nodes.nodeReference() );
                assertFalse( "should only access a single node", nodes.next() );
            }
        }
    }

    @Test
    public void shouldNotFindDeletedNode() throws Exception
    {
        // given
        try ( NodeCursor nodes = graph.allocateNodeCursor() )
        {
            // when
            graph.singleNode( gone, nodes );

            // then
            assertFalse( "should not access deleted node", nodes.next() );
        }
    }

    @Test
    public void shouldReadLabels() throws Exception
    {
        assumeThat( "x86_64", equalTo( System.getProperty( "os.arch" ) ) );

        // given
        try ( NodeCursor nodes = graph.allocateNodeCursor() )
        {
            LabelSet labels;

            // when
            graph.singleNode( foo, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int _foo = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            // when
            graph.singleNode( bar, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int _bar = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            // when
            graph.singleNode( baz, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int _baz = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            assertNotEquals( "distinct labels", _foo, _bar );
            assertNotEquals( "distinct labels", _foo, _baz );
            assertNotEquals( "distinct labels", _bar, _baz );

            // when
            graph.singleNode( barbaz, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 2, labels.numberOfLabels() );
            if ( labels.label( 0 ) == _bar )
            {
                assertEquals( _baz, labels.label( 1 ) );
            }
            else
            {
                assertEquals( _baz, labels.label( 0 ) );
                assertEquals( _bar, labels.label( 1 ) );
            }
            assertFalse( "should only access a single node", nodes.next() );

            // when
            graph.singleNode( bare, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 0, labels.numberOfLabels() );
            assertFalse( "should only access a single node", nodes.next() );
        }
    }
}
