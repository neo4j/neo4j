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
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class LargeNodeCursorTestBase<G extends KernelAPIReadTestSupport> extends KernelAPIReadTestBase<G>
{
    private static List<Long> NODE_IDS = new ArrayList<>();
    private static int N_NODES = 10000;

    private static Random random = new Random( 2 );

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        List<Node> deleted = new ArrayList<>();
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_NODES; i++ )
            {
                Node node = graphDb.createNode();
                if ( random.nextBoolean() )
                {
                    NODE_IDS.add( node.getId() );
                }
                else
                {
                    deleted.add( node );
                }
            }
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( Node node : deleted )
            {
                node.delete();
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
                read.singleNode( id, nodes );

                // then
                assertTrue( "should access defined node", nodes.next() );
                assertEquals( "should access the correct node", id, nodes.nodeReference() );
                assertFalse( "should only access a single node", nodes.next() );
            }
        }
    }
}
