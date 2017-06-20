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

import org.junit.ClassRule;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Label;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.impl.kernel.api.EdgeGroupCursor;
import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.NodeCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class RandomEdgeTraversalCursorTest
{
    private static final int N_TRAVERSALS = 10_000;
    private static int N_NODES = 100;
    private static int N_EDGES = 1000;
    private static Random random = new Random( 666 );
    private static List<Long> nodeIds = new ArrayList<>();

    @ClassRule
    public static final GraphSetup graph = new GraphSetup()
    {
        @Override
        protected void create( GraphDatabaseService graphDb )
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
                for ( int i = 0; i < N_EDGES; i++ )
                {
                    Long source = nodeIds.get( random.nextInt( N_NODES ) );
                    Long target = nodeIds.get( random.nextInt( N_NODES ) );
                    graphDb.getNodeById( source ).createRelationshipTo( graphDb.getNodeById( target ),
                            RelationshipType.withName( "REL" + ( i % 10 ) ) );
                }
                tx.success();
            }
        }
    }.withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldManageRandomTraversals() throws Exception
    {
        // given
        try ( NodeCursor node = graph.allocateNodeCursor();
              EdgeGroupCursor group = graph.allocateEdgeGroupCursor();
              EdgeTraversalCursor edge = graph.allocateEdgeTraversalCursor() )
        {
            for ( int i = 0; i < N_TRAVERSALS; i++ )
            {
                // when
                long nodeId = nodeIds.get( random.nextInt( N_NODES ) );
                graph.singleNode( nodeId, node );
                assertTrue( "access root node", node.next() );
                node.edges( group );
                assertFalse( "single root", node.next() );

                // then
                while ( group.next() )
                {
                    group.incoming( edge );
                    while ( edge.next() )
                    {
                        assertEquals( "incoming origin", nodeId, edge.originNodeReference() );
                        edge.neighbour( node );
                    }
                    group.outgoing( edge );
                    while ( edge.next() )
                    {
                        assertEquals( "outgoing origin", nodeId, edge.originNodeReference() );
                        edge.neighbour( node );
                    }
                    group.loops( edge );
                    while ( edge.next() )
                    {
                        assertEquals( "loop origin", nodeId, edge.originNodeReference() );
                        edge.neighbour( node );
                    }
                }
            }
        }
    }
}
