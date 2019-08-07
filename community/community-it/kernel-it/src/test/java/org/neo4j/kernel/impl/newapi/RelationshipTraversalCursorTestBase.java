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

import java.util.Map;

import org.neo4j.exceptions.KernelException;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.NodeCursor;
import org.neo4j.internal.kernel.api.RelationshipGroupCursor;
import org.neo4j.internal.kernel.api.RelationshipTraversalCursor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.assertCount;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.assertCounts;
import static org.neo4j.kernel.impl.newapi.RelationshipTestSupport.count;

public abstract class RelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long bare, start, end;
    private static RelationshipTestSupport.StartNode sparse, dense;

    private boolean supportsDirectTraversal()
    {
        return true;
    }

    private boolean supportsSparseNodes()
    {
        return true;
    }

    private static void bareStartAndEnd( GraphDatabaseService graphDb )
    {
        try ( org.neo4j.graphdb.Transaction tx = graphDb.beginTx() )
        {
            bare = graphDb.createNode().getId();

            Node x = graphDb.createNode(), y = graphDb.createNode();
            start = x.getId();
            end = y.getId();
            x.createRelationshipTo( y, withName( "GEN" ) );

            tx.commit();
        }
    }

    @Override
    public void createTestGraph( GraphDatabaseService graphDb )
    {
        RelationshipTestSupport.someGraph( graphDb );
        bareStartAndEnd( graphDb );

        sparse = RelationshipTestSupport.sparse( graphDb );
        dense = RelationshipTestSupport.dense( graphDb );
    }

    @Test
    void shouldNotAccessGroupsOfBareNode()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor() )
        {
            // when
            read.singleNode( bare, node );
            assertTrue( node.next(), "access node" );
            node.relationships( group );

            // then
            assertFalse( group.next(), "access group" );
        }
    }

    @Test
    void shouldTraverseRelationshipsOfGivenType()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            int empty = 0;
            // when
            read.allNodesScan( node );
            while ( node.next() )
            {
                node.relationships( group );
                boolean none = true;
                while ( group.next() )
                {
                    none = false;
                    Sizes degree = new Sizes();
                    group.outgoing( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( group.type(), relationship.type(), "node #" + node.nodeReference() +
                                                                         " relationship should have same label as group"
                        );
                        degree.outgoing++;
                    }
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( group.type(), relationship.type(), "node #" + node.nodeReference() +
                                                                         "relationship should have same label as group"
                        );
                        degree.incoming++;
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( group.type(), relationship.type(), "node #" + node.nodeReference() +
                                                                         "relationship should have same label as group"
                        );
                        degree.loop++;
                    }

                    // then
                    assertNotEquals( 0, degree.incoming + degree.outgoing + degree.loop, "all" );
                    assertEquals(
                            group.outgoingCount(), degree.outgoing, "node #" + node.nodeReference() + " outgoing"
                    );
                    assertEquals(
                            group.incomingCount(), degree.incoming, "node #" + node.nodeReference() + " incoming"
                    );
                    assertEquals( group.loopCount(), degree.loop, "node #" + node.nodeReference() + " loop" );
                    assertEquals( group.totalCount(), degree.incoming + degree.outgoing + degree.loop,
                            "node #" + node.nodeReference() + " all = incoming + outgoing - loop"
                    );
                }
                if ( none )
                {
                    empty++;
                }
            }

            // then
            assertEquals( 1, empty, "number of empty nodes" );
        }
    }

    @Test
    void shouldFollowSpecificRelationship()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when - traversing from start to end
            read.singleNode( start, node );
            assertTrue( node.next(), "access start node" );
            node.relationships( group );
            assertTrue( group.next(), "access relationship group" );
            group.outgoing( relationship );
            assertTrue( relationship.next(), "access outgoing relationships" );

            // then
            assertEquals( start, relationship.sourceNodeReference(), "source node" );
            assertEquals( end, relationship.targetNodeReference(), "target node" );

            assertEquals( start, relationship.originNodeReference(), "node of origin" );
            assertEquals( end, relationship.neighbourNodeReference(), "neighbouring node" );

            assertEquals( group.type(), relationship.type(), "relationship should have same label as group"
            );

            assertFalse( relationship.next(), "only a single relationship" );

            group.incoming( relationship );
            assertFalse( relationship.next(), "no incoming relationships" );
            group.loops( relationship );
            assertFalse( relationship.next(), "no loop relationships" );

            assertFalse( group.next(), "only a single group" );

            // when - traversing from end to start
            read.singleNode( end, node );
            assertTrue( node.next(), "access start node" );
            node.relationships( group );
            assertTrue( group.next(), "access relationship group" );
            group.incoming( relationship );
            assertTrue( relationship.next(), "access incoming relationships" );

            // then
            assertEquals( start, relationship.sourceNodeReference(), "source node" );
            assertEquals( end, relationship.targetNodeReference(), "target node" );

            assertEquals( end, relationship.originNodeReference(), "node of origin" );
            assertEquals( start, relationship.neighbourNodeReference(), "neighbouring node" );

            assertEquals( group.type(), relationship.type(), "relationship should have same label as group"
            );

            assertFalse( relationship.next(), "only a single relationship" );

            group.outgoing( relationship );
            assertFalse( relationship.next(), "no outgoing relationships" );
            group.loops( relationship );
            assertFalse( relationship.next(), "no loop relationships" );

            assertFalse( group.next(), "only a single group" );
        }
    }

    @Test
    void shouldHaveBeenAbleToCreateDenseAndSparseNodes()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor() )
        {
            read.singleNode( dense.id, node );
            assertTrue( node.next(), "access dense node" );
            assertTrue( node.isDense(), "dense node" );

            read.singleNode( sparse.id, node );
            assertTrue( node.next(), "access sparse node" );
            assertFalse( node.isDense() && supportsSparseNodes(), "sparse node" );
        }
    }

    @Test
    void shouldTraverseSparseNodeViaGroups() throws Exception
    {
        traverseViaGroups( sparse, false );
    }

    @Test
    void shouldTraverseDenseNodeViaGroups() throws Exception
    {
        traverseViaGroups( dense, false );
    }

    @Test
    void shouldTraverseSparseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( sparse, true );
    }

    @Test
    void shouldTraverseDenseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( dense, true );
    }

    @Test
    void shouldTraverseSparseNodeWithoutGroups() throws Exception
    {
        assumeTrue( supportsSparseNodes() && supportsDirectTraversal() );
        traverseWithoutGroups( sparse, false );
    }

    @Test
    void shouldTraverseDenseNodeWithoutGroups() throws Exception
    {
        assumeTrue( supportsDirectTraversal() );
        traverseWithoutGroups( dense, false );
    }

    @Test
    void shouldTraverseSparseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        assumeTrue( supportsSparseNodes() );
        traverseWithoutGroups( sparse, true );
    }

    @Test
    void shouldTraverseDenseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        assumeTrue( supportsDirectTraversal() );
        traverseWithoutGroups( dense, true );
    }

    private void traverseViaGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws KernelException
    {
        // given
        Map<String,Integer> expectedCounts = start.expectedCounts();

        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when
            read.singleNode( start.id, node );
            assertTrue( node.next(), "access node" );
            if ( detached )
            {
                read.relationshipGroups( start.id, node.relationshipGroupReference(), group );
            }
            else
            {
                node.relationships( group );
            }

            while ( group.next() )
            {
                // outgoing
                if ( detached )
                {
                    read.relationships( start.id, group.outgoingReference(), relationship );
                }
                else
                {
                    group.outgoing( relationship );
                }
                // then
                assertCount( tx, relationship, expectedCounts, group.type(), OUTGOING );

                // incoming
                if ( detached )
                {
                    read.relationships( start.id, group.incomingReference(), relationship );
                }
                else
                {
                    group.incoming( relationship );
                }
                // then
                assertCount( tx, relationship, expectedCounts, group.type(), INCOMING );

                // loops
                if ( detached )
                {
                    read.relationships( start.id, group.loopsReference(), relationship );
                }
                else
                {
                    group.loops( relationship );
                }
                // then
                assertCount( tx, relationship, expectedCounts, group.type(), BOTH );
            }
        }
    }

    private void traverseWithoutGroups( RelationshipTestSupport.StartNode start, boolean detached )
            throws KernelException
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when
            read.singleNode( start.id, node );
            assertTrue( node.next(), "access node" );

            if ( detached )
            {
                read.relationships( start.id, node.allRelationshipsReference(), relationship );
            }
            else
            {
                node.allRelationships( relationship );
            }

            Map<String,Integer> counts = count( tx, relationship );

            // then
            assertCounts( start.expectedCounts(), counts );
        }
    }

    private static class Sizes
    {
        int incoming, outgoing, loop;
    }
}
