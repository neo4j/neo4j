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

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class DeepRelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long three_root;
    private static int expected_total, expected_unique;

    private RelationshipType PARENT = withName( "PARENT" );

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
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

    private int relate( int count, Node[] selection, int offset, Node parent )
    {
        for ( int i = 0; i < count; i++ )
        {
            selection[offset++ % selection.length].createRelationshipTo( parent, PARENT );
        }
        return offset;
    }

    @Test
    public void shouldTraverseTreeOfDepthThree()
    {
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship1 = cursors.allocateRelationshipTraversalCursor();
              RelationshipTraversalCursor relationship2 = cursors.allocateRelationshipTraversalCursor();
              PrimitiveLongSet leafs = Primitive.longSet() )
        {
            long total = 0;

            // when
            read.singleNode( three_root, node );
            assertTrue( "access root node", node.next() );
            node.relationships( group );
            assertFalse( "single root", node.next() );

            assertTrue( "access group of root", group.next() );
            group.incoming( relationship1 );
            assertFalse( "single group of root", group.next() );

            while ( relationship1.next() )
            {
                relationship1.neighbour( node );

                assertTrue( "child level 1", node.next() );
                node.relationships( group );
                assertFalse( "single node", node.next() );

                assertTrue( "group of level 1 child", group.next() );
                group.incoming( relationship2 );
                assertFalse( "single group of level 1 child", group.next() );

                while ( relationship2.next() )
                {
                    leafs.add( relationship2.neighbourNodeReference() );
                    total++;
                }
            }

            // then
            assertEquals( "total number of leaf nodes", expected_total, total );
            assertEquals( "number of distinct leaf nodes", expected_unique, leafs.size() );
        }
    }
}
