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
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public abstract class RandomRelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static final int N_TRAVERSALS = 10_000;
    private static int N_NODES = 100;
    private static int N_RELATIONSHIPS = 1000;
    private static long seed = (new Random()).nextInt();
    private static Random random = new Random( seed );
    private static List<Long> nodeIds = new ArrayList<>();

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_NODES; i++ )
            {
                nodeIds.add( graphDb.createNode( Label.label( "LABEL" + i ) ).getId() );
            }
            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_RELATIONSHIPS; i++ )
            {
                Long source = nodeIds.get( random.nextInt( N_NODES ) );
                Long target = nodeIds.get( random.nextInt( N_NODES ) );
                graphDb.getNodeById( source ).createRelationshipTo( graphDb.getNodeById( target ),
                        RelationshipType.withName( "REL" + (i % 10) ) );
            }
            tx.success();
        }
    }

    @Test
    public void shouldManageRandomTraversals()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            for ( int i = 0; i < N_TRAVERSALS; i++ )
            {
                // when
                long nodeId = nodeIds.get( random.nextInt( N_NODES ) );
                read.singleNode( nodeId, node );
                assertTrue( "access root node", node.next() );
                node.relationships( group );
                assertFalse( "single root", node.next() );

                // then
                while ( group.next() )
                {
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "incoming origin", nodeId, relationship.originNodeReference() );
                        relationship.neighbour( node );
                    }
                    group.outgoing( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "outgoing origin", nodeId, relationship.originNodeReference() );
                        relationship.neighbour( node );
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "loop origin", nodeId, relationship.originNodeReference() );
                        relationship.neighbour( node );
                    }
                }
            }
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( "Failed with random seed " + seed, t );
        }
    }
}
