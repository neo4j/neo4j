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
package org.neo4j.internal.kernel.api;

import org.junit.Test;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Node;
import org.neo4j.graphdb.Relationship;
import org.neo4j.graphdb.Transaction;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.graphdb.RelationshipType.withName;

public abstract class RelationshipTraversalCursorTestBase<G extends KernelAPIReadTestSupport>
        extends KernelAPIReadTestBase<G>
{
    private static long bare, start, end;

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
                        assertEquals( "relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
                        degree.outgoing++;
                    }
                    group.incoming( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
                        degree.incoming++;
                    }
                    group.loops( relationship );
                    while ( relationship.next() )
                    {
                        assertEquals( "relationship should have same label as group", group.relationshipLabel(),
                                relationship.label() );
                        degree.loop++;
                    }

                    // then
                    assertNotEquals( "all", 0, degree.incoming + degree.outgoing + degree.loop );
                    assertEquals( "outgoing", group.outgoingCount(), degree.outgoing );
                    assertEquals( "incoming", group.incomingCount(), degree.incoming );
                    assertEquals( "loop", group.loopCount(), degree.loop );
                    assertEquals( "all = incoming + outgoing - loop",
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

    private class Sizes
    {
        int incoming, outgoing, loop;
    }
}
