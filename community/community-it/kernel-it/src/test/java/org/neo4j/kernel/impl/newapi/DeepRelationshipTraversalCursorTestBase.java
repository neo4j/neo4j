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

import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.RelationshipType;
import org.neo4j.graphdb.Transaction;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class DeepRelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long three_root;
    private static int expected_total, expected_unique;

    private RelationshipType PARENT = withName( "PARENT" );

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
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

            tx.commit();
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
    void shouldTraverseTreeOfDepthThree()
    {
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship1 = cursors.allocateRelationshipTraversalCursor();
              RelationshipTraversalCursor relationship2 = cursors.allocateRelationshipTraversalCursor() )
        {
            MutableLongSet leafs = new LongHashSet();
            long total = 0;

            // when
            read.singleNode( three_root, node );
            assertTrue( node.next(), "access root node" );
            node.relationships( group );
            assertFalse( node.next(), "single root" );

            assertTrue( group.next(), "access group of root" );
            group.incoming( relationship1 );
            assertFalse( group.next(), "single group of root" );

            while ( relationship1.next() )
            {
                relationship1.neighbour( node );

                assertTrue( node.next(), "child level 1" );
                node.relationships( group );
                assertFalse( node.next(), "single node" );

                assertTrue( group.next(), "group of level 1 child" );
                group.incoming( relationship2 );
                assertFalse( group.next(), "single group of level 1 child" );

                while ( relationship2.next() )
                {
                    leafs.add( relationship2.neighbourNodeReference() );
                    total++;
                }
            }

            // then
            assertEquals( expected_total, total, "total number of leaf nodes" );
            assertEquals( expected_unique, leafs.size(), "number of distinct leaf nodes" );
        }
    }
}
