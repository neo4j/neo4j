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

import org.junit.Assume;
import org.junit.Test;

import java.util.Map;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.graphdb.RelationshipType.withName;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCount;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCounts;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.count;

public abstract class RelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long bare, start, end;
    private static RelationshipTestSupport.StartNode sparse, dense;

    protected boolean supportsDirectTraversal()
    {
        return true;
    }

    protected boolean supportsSparseNodes()
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

            tx.success();
        }
    }

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        RelationshipTestSupport.someGraph( graphDb );
        bareStartAndEnd( graphDb );

        sparse = RelationshipTestSupport.sparse( graphDb );
        dense = RelationshipTestSupport.dense( graphDb );
    }

    @Test
    public void shouldNotAccessGroupsOfBareNode()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor() )
        {
            // when
            read.singleNode( bare, node );
            assertTrue( "access node", node.next() );
            node.relationships( group );

            // then
            assertFalse( "access group", group.next() );
        }
    }

    @Test
    public void shouldTraverseRelationshipsOfGivenType()
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
                        assertEquals( "node #" + node.nodeReference() +
                                        " relationship should have same label as group", group.type(),
                                relationship.type() );
                        degree.outgoing++;
                    }
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "node #" + node.nodeReference() +
                                        "relationship should have same label as group", group.type(),
                                relationship.type() );
                        degree.incoming++;
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "node #" + node.nodeReference() +
                                        "relationship should have same label as group", group.type(),
                                relationship.type() );
                        degree.loop++;
                    }

                    // then
                    assertNotEquals( "all", 0, degree.incoming + degree.outgoing + degree.loop );
                    assertEquals(
                            "node #" + node.nodeReference() + " outgoing",
                            group.outgoingCount(),
                            degree.outgoing );
                    assertEquals(
                            "node #" + node.nodeReference() + " incoming",
                            group.incomingCount(),
                            degree.incoming );
                    assertEquals( "node #" + node.nodeReference() + " loop", group.loopCount(), degree.loop );
                    assertEquals( "node #" + node.nodeReference() + " all = incoming + outgoing - loop",
                            group.totalCount(), degree.incoming + degree.outgoing + degree.loop );
                }
                if ( none )
                {
                    empty++;
                }
            }

            // then
            assertEquals( "number of empty nodes", 1, empty );
        }
    }

    @Test
    public void shouldFollowSpecificRelationship()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when - traversing from start to end
            read.singleNode( start, node );
            assertTrue( "access start node", node.next() );
            node.relationships( group );
            assertTrue( "access relationship group", group.next() );
            group.outgoing( relationship );
            assertTrue( "access outgoing relationships", relationship.next() );

            // then
            assertEquals( "source node", start, relationship.sourceNodeReference() );
            assertEquals( "target node", end, relationship.targetNodeReference() );

            assertEquals( "node of origin", start, relationship.originNodeReference() );
            assertEquals( "neighbouring node", end, relationship.neighbourNodeReference() );

            assertEquals( "relationship should have same label as group", group.type(),
                    relationship.type() );

            assertFalse( "only a single relationship", relationship.next() );

            group.incoming( relationship );
            assertFalse( "no incoming relationships", relationship.next() );
            group.loops( relationship );
            assertFalse( "no loop relationships", relationship.next() );

            assertFalse( "only a single group", group.next() );

            // when - traversing from end to start
            read.singleNode( end, node );
            assertTrue( "access start node", node.next() );
            node.relationships( group );
            assertTrue( "access relationship group", group.next() );
            group.incoming( relationship );
            assertTrue( "access incoming relationships", relationship.next() );

            // then
            assertEquals( "source node", start, relationship.sourceNodeReference() );
            assertEquals( "target node", end, relationship.targetNodeReference() );

            assertEquals( "node of origin", end, relationship.originNodeReference() );
            assertEquals( "neighbouring node", start, relationship.neighbourNodeReference() );

            assertEquals( "relationship should have same label as group", group.type(),
                    relationship.type() );

            assertFalse( "only a single relationship", relationship.next() );

            group.outgoing( relationship );
            assertFalse( "no outgoing relationships", relationship.next() );
            group.loops( relationship );
            assertFalse( "no loop relationships", relationship.next() );

            assertFalse( "only a single group", group.next() );
        }
    }

    @Test
    public void shouldHaveBeenAbleToCreateDenseAndSparseNodes()
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor() )
        {
            read.singleNode( dense.id, node );
            assertTrue( "access dense node", node.next() );
            assertTrue( "dense node", node.isDense() );

            read.singleNode( sparse.id, node );
            assertTrue( "access sparse node", node.next() );
            assertFalse( "sparse node", node.isDense() && supportsSparseNodes() );
        }
    }

    @Test
    public void shouldTraverseSparseNodeViaGroups() throws Exception
    {
        traverseViaGroups( sparse, false );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroups() throws Exception
    {
        traverseViaGroups( dense, false );
    }

    @Test
    public void shouldTraverseSparseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( sparse, true );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( dense, true );
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroups() throws Exception
    {
        Assume.assumeTrue( supportsSparseNodes() && supportsDirectTraversal() );
        traverseWithoutGroups( sparse, false );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroups() throws Exception
    {
        Assume.assumeTrue( supportsDirectTraversal() );
        traverseWithoutGroups( dense, false );
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        Assume.assumeTrue( supportsSparseNodes() );
        traverseWithoutGroups( sparse, true );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        Assume.assumeTrue( supportsDirectTraversal() );
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
            assertTrue( "access node", node.next() );
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
            assertTrue( "access node", node.next() );

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
