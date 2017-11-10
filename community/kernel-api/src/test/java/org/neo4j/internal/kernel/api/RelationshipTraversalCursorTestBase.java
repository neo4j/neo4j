/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

import org.junit.Assume;
import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static java.util.Arrays.sort;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class RelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long bare, start, end, sparse, dense;

    protected boolean supportsDirectTraversal()
    {
        return true;
    }

    protected boolean supportsSparseNodes()
    {
        return true;
    }

    @Override
    void createTestGraph( GraphDatabaseService graphDb )
    {
        Relationship dead;
        try ( Transaction tx = graphDb.beginTx() )
        {
            Node a = graphDb.createNode(),
                    b = graphDb.createNode(),
                    c = graphDb.createNode(),
                    d = graphDb.createNode();

            a.createRelationshipTo( a, withName( "ALPHA" ) );
            a.createRelationshipTo( b, withName( "BETA" ) );
            a.createRelationshipTo( c, withName( "GAMMA" ) );
            a.createRelationshipTo( d, withName( "DELTA" ) );

            bare = graphDb.createNode().getId();

            Node x = graphDb.createNode(), y = graphDb.createNode();
            start = x.getId();
            end = y.getId();
            x.createRelationshipTo( y, withName( "GEN" ) );

            graphDb.createNode().createRelationshipTo( a, withName( "BETA" ) );
            a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );
            dead = a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );
            a.createRelationshipTo( graphDb.createNode(), withName( "BETA" ) );

            Node clump = graphDb.createNode();
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( clump, withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            clump.createRelationshipTo( graphDb.createNode(), withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );
            graphDb.createNode().createRelationshipTo( clump, withName( "REL" ) );

            tx.success();
        }

        try ( Transaction tx = graphDb.beginTx() )
        {
            Node node = dead.getEndNode();
            dead.delete();
            node.delete();

            tx.success();
        }

        Node sparseNode = node(
                graphDb,
                outgoing( "FOO" ),
                outgoing( "BAR" ),
                outgoing( "BAR" ),
                incoming( "FOO" ),
                outgoing( "FOO" ),
                incoming( "BAZ" ),
                incoming( "BAR" ),
                outgoing( "BAZ" ) );
        Node denseNode;
        // dense node
        try ( Transaction tx = graphDb.beginTx() )
        {
            denseNode = graphDb.createNode();
            for ( Relationship rel : sparseNode.getRelationships() )
            {
                if ( sparseNode.equals( rel.getStartNode() ) )
                {
                    denseNode.createRelationshipTo( graphDb.createNode(), rel.getType() );
                }
                else
                {
                    graphDb.createNode().createRelationshipTo( denseNode, rel.getType() );
                }
            }
            for ( int i = 0; i < 200; i++ )
            {
                denseNode.createRelationshipTo( graphDb.createNode(), withName( "BULK" ) );
            }

            tx.success();
        }
        sparse = sparseNode.getId();
        dense = denseNode.getId();
    }

    @SafeVarargs
    private static Node node( GraphDatabaseService db, Consumer<Node>... changes )
    {
        Node node;
        try ( Transaction tx = db.beginTx() )
        {
            node = db.createNode();
            tx.success();
        }
        for ( int i = changes.length; i-- > 0; )
        {
            changes[i].accept( node );
        }
        return node;
    }

    private static Consumer<Node> outgoing( String type )
    {
        return node ->
        {
            GraphDatabaseService db = node.getGraphDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                node.createRelationshipTo( db.createNode(), withName( type ) );
                tx.success();
            }
        };
    }

    private static Consumer<Node> incoming( String type )
    {
        return node ->
        {
            GraphDatabaseService db = node.getGraphDatabase();
            try ( Transaction tx = db.beginTx() )
            {
                db.createNode().createRelationshipTo( node, withName( type ) );
                tx.success();
            }
        };
    }

    @Test
    public void shouldNotAccessGroupsOfBareNode() throws Exception
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
    public void shouldTraverseRelationshipsOfGivenType() throws Exception
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
                                        " relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
                        degree.outgoing++;
                    }
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "node #" + node.nodeReference() +
                                        "relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
                        degree.incoming++;
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "node #" + node.nodeReference() +
                                        "relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
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
                            group.totalCount(), degree.incoming + degree.outgoing - degree.loop );
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
    public void shouldFollowSpecificRelationship() throws Exception
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

            assertEquals( "relationship should have same label as group", group.relationshipLabel(),
                    relationship.label() );

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

            assertEquals( "relationship should have same label as group", group.relationshipLabel(),
                    relationship.label() );

            assertFalse( "only a single relationship", relationship.next() );

            group.outgoing( relationship );
            assertFalse( "no outgoing relationships", relationship.next() );
            group.loops( relationship );
            assertFalse( "no loop relationships", relationship.next() );

            assertFalse( "only a single group", group.next() );
        }
    }

    @Test
    public void shouldHaveBeenAbleToCreateDenseAndSparseNodes() throws Exception
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor() )
        {
            read.singleNode( dense, node );
            assertTrue( "access dense node", node.next() );
            assertTrue( "dense node", node.isDense() );

            read.singleNode( sparse, node );
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

    private void traverseViaGroups( long start, boolean detached )
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipGroupCursor group = cursors.allocateRelationshipGroupCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when
            read.singleNode( start, node );
            assertTrue( "access node", node.next() );
            if ( detached )
            {
                read.relationshipGroups( start, node.relationshipGroupReference(), group );
            }
            else
            {
                node.relationships( group );
            }
            Map<Integer,Integer> counts = new HashMap<>();
            while ( group.next() )
            {
                // outgoing
                if ( detached )
                {
                    read.relationships( start, group.outgoingReference(), relationship );
                }
                else
                {
                    group.outgoing( relationship );
                }
                count( relationship, counts, true );

                // incoming
                if ( detached )
                {
                    read.relationships( start, group.incomingReference(), relationship );
                }
                else
                {
                    group.incoming( relationship );
                }
                count( relationship, counts, true );

                // loops
                if ( detached )
                {
                    read.relationships( start, group.loopsReference(), relationship );
                }
                else
                {
                    group.loops( relationship );
                }
                count( relationship, counts, true );
            }

            // then
            assertCounts( counts );
        }
    }

    private void traverseWithoutGroups( long start, boolean detached )
    {
        // given
        try ( NodeCursor node = cursors.allocateNodeCursor();
              RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
        {
            // when
            read.singleNode( start, node );
            assertTrue( "access node", node.next() );
            if ( detached )
            {
                read.relationships( start, node.allRelationshipsReference(), relationship );
            }
            else
            {
                node.allRelationships( relationship );
            }
            Map<Integer,Integer> counts = new HashMap<>();
            count( relationship, counts, false );

            // then
            assertCounts( counts );
        }
    }

    private void count( RelationshipTraversalCursor relationship, Map<Integer,Integer> counts, boolean expectSameType )
    {
        Integer type = null;
        while ( relationship.next() )
        {
            if ( expectSameType )
            {
                if ( type != null )
                {
                    assertEquals( "same type", type.intValue(), relationship.label() );
                }
                else
                {
                    type = relationship.label();
                }
            }
            counts.compute( relationship.label(), ( key, value ) -> value == null ? 1 : value + 1 );
        }
    }

    private void assertCounts( Map<Integer,Integer> counts )
    {
        Integer[] values = counts.values().toArray( new Integer[0] );
        assertTrue( values.length >= 3 );
        sort( values );
        assertEquals( 2, values[0].intValue() );
        assertEquals( 3, values[1].intValue() );
        assertEquals( 3, values[2].intValue() );
    }

    private class Sizes
    {
        int incoming, outgoing, loop;
    }
}
