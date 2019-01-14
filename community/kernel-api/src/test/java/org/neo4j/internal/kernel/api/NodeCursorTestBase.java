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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Label.label;

public abstract class NodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> NODE_IDS;
    private static long foo, bar, baz, barbaz, bare, gone;

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
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
            for ( Node node : graphDb.getAllNodes() )
            {
                NODE_IDS.add( node.getId() );
            }
            tx.success();
        }
    }

    @Test
    public void shouldScanNodes()
    {
        // given
        List<Long> ids = new ArrayList<>();
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
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
    public void shouldAccessNodesByReference()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            for ( long id : NODE_IDS )
            {
                // when
                read.singleNode( id, nodes  );

                // then
                assertTrue( "should access defined node", nodes.next() );
                assertEquals( "should access the correct node", id, nodes.nodeReference() );
                assertFalse( "should only access a single node", nodes.next() );
            }
        }
    }

    // This is functionality which is only required for the hacky db.schema not to leak real data
    @Test
    public void shouldNotAccessNegativeReferences()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor() )
        {
            // when
            read.singleNode( -2L, node  );

            // then
            assertFalse( "should not access negative reference node", node.next() );
        }
    }

    @Test
    public void shouldNotFindDeletedNode()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            // when
            read.singleNode( gone, nodes );

            // then
            assertFalse( "should not access deleted node", nodes.next() );
        }
    }

    @Test
    public void shouldReadLabels()
    {
        // given
        try ( NodeCursor nodes = cursors.allocateNodeCursor() )
        {
            LabelSet labels;

            // when
            read.singleNode( foo, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int fooLabel = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            // when
            read.singleNode( bar, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int barLabel = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            // when
            read.singleNode( baz, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 1, labels.numberOfLabels() );
            int bazLabel = labels.label( 0 );
            assertFalse( "should only access a single node", nodes.next() );

            assertNotEquals( "distinct labels", fooLabel, barLabel );
            assertNotEquals( "distinct labels", fooLabel, bazLabel );
            assertNotEquals( "distinct labels", barLabel, bazLabel );

            // when
            read.singleNode( barbaz, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 2, labels.numberOfLabels() );
            if ( labels.label( 0 ) == barLabel )
            {
                assertEquals( bazLabel, labels.label( 1 ) );
            }
            else
            {
                assertEquals( bazLabel, labels.label( 0 ) );
                assertEquals( barLabel, labels.label( 1 ) );
            }
            assertFalse( "should only access a single node", nodes.next() );

            // when
            read.singleNode( bare, nodes );

            // then
            assertTrue( "should access defined node", nodes.next() );
            labels = nodes.labels();
            assertEquals( "number of labels", 0, labels.numberOfLabels() );
            assertFalse( "should only access a single node", nodes.next() );
        }
    }
}
