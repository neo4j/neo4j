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
package org.neo4j.kernel.impl.newapi;


import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.TokenSet;
import org.neo4j.internal.kernel.api.NodeCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Label.label;
import static org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer.NULL;

public abstract class NodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> NODE_IDS;
    private static long foo, bar, baz, barbaz, bare, gone;

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        Node deleted;
        try ( Transaction tx = graphDb.beginTx() )
        {
            foo = tx.createNode( label( "Foo" ) ).getId();
            bar = tx.createNode( label( "Bar" ) ).getId();
            baz = tx.createNode( label( "Baz" ) ).getId();
            barbaz = tx.createNode( label( "Bar" ), label( "Baz" ) ).getId();
            gone = (deleted = tx.createNode()).getId();
            bare = tx.createNode().getId();

            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            tx.getNodeById( deleted.getId() ).delete();

            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            NODE_IDS = new ArrayList<>();
            for ( Node node : tx.getAllNodes() )
            {
                NODE_IDS.add( node.getId() );
            }
            tx.commit();
        }
    }

    @Test
    void shouldScanNodes()
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.allNodesScan( nodes );
            while ( nodes.next() )
            {
                ids.add( nodes.nodeReference() );
            }
        }

        // then
        assertEquals( NODE_IDS, ids );
    }

    @Test
    void shouldAccessNodesByReference()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            for ( long id : NODE_IDS )
            {
                // when
                read.singleNode( id, nodes );

                // then
                assertTrue( nodes.next(), "should access defined node" );
                assertEquals( id, nodes.nodeReference(), "should access the correct node" );
                assertFalse( nodes.next(), "should only access a single node" );
            }
        }
    }

    // This is functionality which is only required for the hacky db.schema not to leak real data
    @Test
    void shouldNotAccessNegativeReferences()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( -2L, node );

            // then
            assertFalse( node.next(), "should not access negative reference node" );
        }
    }

    @Test
    void shouldNotFindDeletedNode()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            // when
            read.singleNode( gone, nodes );

            // then
            assertFalse( nodes.next(), "should not access deleted node" );
        }
    }

    @Test
    void shouldReadLabels()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor( NULL ) )
        {
            TokenSet labels;

            // when
            read.singleNode( foo, nodes );

            // then
            assertTrue( nodes.next(), "should access defined node" );
            labels = nodes.labels();
            assertEquals( 1, labels.numberOfTokens(), "number of labels" );
            int fooLabel = labels.token( 0 );
            assertTrue( nodes.hasLabel( fooLabel ) );
            assertFalse( nodes.next(), "should only access a single node" );

            // when
            read.singleNode( bar, nodes );

            // then
            assertTrue( nodes.next(), "should access defined node" );
            labels = nodes.labels();
            assertEquals( 1, labels.numberOfTokens(), "number of labels" );
            int barLabel = labels.token( 0 );
            assertFalse( nodes.hasLabel( fooLabel ) );
            assertTrue( nodes.hasLabel( barLabel ) );
            assertFalse( nodes.next(), "should only access a single node" );

            // when
            read.singleNode( baz, nodes );

            // then
            assertTrue( nodes.next(), "should access defined node" );
            labels = nodes.labels();
            assertEquals( 1, labels.numberOfTokens(), "number of labels" );
            int bazLabel = labels.token( 0 );
            assertFalse( nodes.hasLabel( fooLabel ) );
            assertFalse( nodes.hasLabel( barLabel ) );
            assertTrue( nodes.hasLabel( bazLabel ) );
            assertFalse( nodes.next(), "should only access a single node" );

            assertNotEquals( fooLabel, barLabel, "distinct labels" );
            assertNotEquals( fooLabel, bazLabel, "distinct labels" );
            assertNotEquals( barLabel, bazLabel, "distinct labels" );

            // when
            read.singleNode( barbaz, nodes );

            // then
            assertTrue( nodes.next(), "should access defined node" );
            labels = nodes.labels();
            assertEquals( 2, labels.numberOfTokens(), "number of labels" );
            if ( labels.token( 0 ) == barLabel )
            {
                assertEquals( bazLabel, labels.token( 1 ) );
            }
            else
            {
                assertEquals( bazLabel, labels.token( 0 ) );
                assertEquals( barLabel, labels.token( 1 ) );
            }
            assertFalse( nodes.hasLabel( fooLabel ) );
            assertTrue( nodes.hasLabel( barLabel ) );
            assertTrue( nodes.hasLabel( bazLabel ) );

            assertFalse( nodes.next(), "should only access a single node" );

            // when
            read.singleNode( bare, nodes );

            // then
            assertTrue( nodes.next(), "should access defined node" );
            labels = nodes.labels();
            assertEquals( 0, labels.numberOfTokens(), "number of labels" );
            assertFalse( nodes.hasLabel( fooLabel ) );
            assertFalse( nodes.hasLabel( barLabel ) );
            assertFalse( nodes.hasLabel( bazLabel ) );
            assertFalse( nodes.next(), "should only access a single node" );
        }
    }
}
