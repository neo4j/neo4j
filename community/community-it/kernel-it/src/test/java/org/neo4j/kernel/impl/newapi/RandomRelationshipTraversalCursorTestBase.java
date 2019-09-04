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
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

public abstract class RandomRelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static final int N_TRAVERSALS = 10_000;
    private static final int N_NODES = 100;
    private static final int N_RELATIONSHIPS = 1000;
    private static final long SEED = (new Random()).nextInt();
    private static final Random RANDOM = new Random( SEED );
    private static final List<Long> NODE_IDS = new ArrayList<>();

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_NODES; i++ )
            {
                NODE_IDS.add( tx.createNode( Label.label( "LABEL" + i ) ).getId() );
            }
            tx.commit();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            for ( int i = 0; i < N_RELATIONSHIPS; i++ )
            {
                Long source = NODE_IDS.get( RANDOM.nextInt( N_NODES ) );
                Long target = NODE_IDS.get( RANDOM.nextInt( N_NODES ) );
                tx.getNodeById( source ).createRelationshipTo( tx.getNodeById( target ),
                        RelationshipType.withName( "REL" + (i % 10) ) );
            }
            tx.commit();
        }
    }

    @Test
    void shouldManageRandomTraversals()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            for ( int i = 0; i < N_TRAVERSALS; i++ )
            {
                // when
                long nodeId = NODE_IDS.get( RANDOM.nextInt( N_NODES ) );
                read.singleNode( nodeId, node );
                assertTrue( node.next(), "access root node" );
                node.relationships( group );
                assertFalse( node.next(), "single root" );

                // then
                while ( group.next() )
                {
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( nodeId, relationship.originNodeReference(), "incoming origin" );
                        relationship.neighbour( node );
                    }
                    group.outgoing( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( nodeId, relationship.originNodeReference(), "outgoing origin");
                        relationship.neighbour( node );
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( nodeId, relationship.originNodeReference(), "loop origin" );
                        relationship.neighbour( node );
                    }
                }
            }
        }
        catch ( Throwable t )
        {
            throw new RuntimeException( "Failed with random seed " + SEED, t );
        }
    }
}
