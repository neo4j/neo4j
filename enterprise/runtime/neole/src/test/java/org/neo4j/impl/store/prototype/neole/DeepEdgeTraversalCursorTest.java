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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.impl.kernel.api.EdgeGroupCursor;
import org.neo4j.impl.kernel.api.EdgeTraversalCursor;
import org.neo4j.impl.kernel.api.NodeCursor;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.graphdb.factory.GraphDatabaseSettings.dense_node_threshold;

public class DeepEdgeTraversalCursorTest
{
    private static long three_root;
    private static int expected_total, expected_unique;
    @ClassRule
    public static final GraphSetup graph = new GraphSetup()
    {
        RelationshipType PARENT = withName( "PARENT" );

        @Override
        protected void create( GraphDatabaseService graphDb )
        {
            try ( Transaction tx = graphDb.beginTx() )
            {
                Node root = graphDb.createNode();
                three_root = root.getId();

                Node[] leafs = new Node[32];
                for ( int i = 0; i < leafs.length; i++ )
                {
                    leafs[i] = graphDb.createNode();
                }
                int offset = 0, duplicate = 12;

                Node interdup = graphDb.createNode();
                interdup.createRelationshipTo( root, PARENT );
                offset = relate( duplicate, leafs, offset, interdup );
                for ( int i = 0; i < 5; i++ )
                {
                    Node inter = graphDb.createNode();
                    inter.createRelationshipTo( root, PARENT );
                    offset = relate( 3 + i, leafs, offset, inter );
                }
                interdup.createRelationshipTo( root, PARENT );
                for ( int i = 0; i < 4; i++ )
                {
                    Node inter = graphDb.createNode();
                    inter.createRelationshipTo( root, PARENT );
                    offset = relate( 2 + i, leafs, offset, inter );
                }

                Node inter = graphDb.createNode();
                inter.createRelationshipTo( root, PARENT );
                offset = relate( 1, leafs, offset, inter );

                expected_total = offset + duplicate;
                expected_unique = leafs.length;

                tx.success();
            }
        }

        int relate( int count, Node[] selection, int offset, Node parent )
        {
            for ( int i = 0; i < count; i++ )
            {
                selection[offset++ % selection.length].createRelationshipTo( parent, PARENT );
            }
            return offset;
        }
    }.withConfig( dense_node_threshold, "1" );

    @Test
    public void shouldTraverseTreeOfDepthThree() throws Exception
    {
        try ( NodeCursor node = graph.allocateNodeCursor();
              EdgeGroupCursor group = graph.allocateEdgeGroupCursor();
              EdgeTraversalCursor edge1 = graph.allocateEdgeTraversalCursor();
              EdgeTraversalCursor edge2 = graph.allocateEdgeTraversalCursor();
              PrimitiveLongSet leafs = Primitive.longSet() )
        {
            long total = 0;

            // when
            graph.singleNode( three_root, node );
            assertTrue( "access root node", node.next() );
            node.edges( group );
            assertFalse( "single root", node.next() );

            assertTrue( "access group of root", group.next() );
            group.incoming( edge1 );
            assertFalse( "single group of root", group.next() );

            while ( edge1.next() )
            {
                edge1.neighbour( node );

                assertTrue( "child level 1", node.next() );
                node.edges( group );
                assertFalse( "single node", node.next() );

                assertTrue( "group of level 1 child", group.next() );
                group.incoming( edge2 );
                assertFalse( "single group of level 1 child", group.next() );

                while ( edge2.next() )
                {
                    leafs.add( edge2.neighbourNodeReference() );
                    total++;
                }
            }

            // then
            assertEquals( "total number of leaf nodes", expected_total, total );
            assertEquals( "number of distinct leaf nodes", expected_unique, leafs.size() );
        }
    }
}
