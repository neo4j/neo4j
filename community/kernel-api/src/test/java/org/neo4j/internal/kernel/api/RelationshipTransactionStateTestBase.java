/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.neo4j.internal.kernel.api.exceptions.KernelException;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.graphdb.Direction.BOTH;
import static org.neo4j.graphdb.Direction.INCOMING;
import static org.neo4j.graphdb.Direction.OUTGOING;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCount;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCounts;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.computeKey;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.count;

@SuppressWarnings( "Duplicates" )
public abstract class RelationshipTransactionStateTestBase<G extends KernelAPIWriteTestSupport> extends KernelAPIWriteTestBase<G>
{
    @Test
    public void shouldSeeSingleRelationshipInTransaction() throws Exception
    {
        int label;
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup extra relationship to challenge the implementation
            long decoyNode = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            tx.dataWrite().relationshipCreate( n2, label, decoyNode );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().singleRelationship( r, relationship );
                assertTrue( relationship.next(), "should find relationship" );

                assertEquals( label, relationship.label() );
                assertEquals( n1, relationship.sourceNodeReference() );
                assertEquals( n2, relationship.targetNodeReference() );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( relationship.next(), "should only find one relationship" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotSeeSingleRelationshipWhichWasDeletedInTransaction() throws Exception
    {
        int label;
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );

            long decoyNode = tx.dataWrite().nodeCreate();
            tx.dataWrite().relationshipCreate( n2, label, decoyNode ); // to have >1 relationship in the db

            r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().relationshipDelete( r ), "should delete relationship" );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().singleRelationship( r, relationship );
                assertFalse( relationship.next(), "should not find relationship" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldScanRelationshipInTransaction() throws Exception
    {
        final int nRelationshipsInStore = 10;

        int type;
        long n1, n2;

        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            // setup some in store relationships
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            relateNTimes( nRelationshipsInStore, type, n1, n2, tx );
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            long r = tx.dataWrite().relationshipCreate( n1, type, n2 );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().allRelationshipsScan( relationship );
                assertCountRelationships( relationship, nRelationshipsInStore + 1, n1, type, n2 );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotScanRelationshipWhichWasDeletedInTransaction() throws Exception
    {
        final int nRelationshipsInStore = 5 + 1 + 5;

        int type;
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            type = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );

            relateNTimes( 5, type, n1, n2, tx );
            r = tx.dataWrite().relationshipCreate( n1, type, n2 );
            relateNTimes( 5, type, n1, n2, tx );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            assertTrue( tx.dataWrite().relationshipDelete( r ), "should delete relationship" );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().allRelationshipsScan( relationship );
                assertCountRelationships( relationship, nRelationshipsInStore - 1, n1, type, n2 );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeRelationshipInTransaction() throws Exception
    {
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( node.next(), "should access node" );

                node.allRelationships( relationship );
                assertTrue( relationship.next(), "should find relationship" );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( relationship.next(), "should only find one relationship" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldNotSeeRelationshipDeletedInTransaction() throws Exception
    {
        long n1, n2, r;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();

            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            r = tx.dataWrite().relationshipCreate( n1, label, n2 );

            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            tx.dataWrite().relationshipDelete( r );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( node.next(), "should access node" );

                node.allRelationships( relationship );
                assertFalse( relationship.next(), "should not find relationship" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldSeeRelationshipInTransactionBeforeCursorInitialization() throws Exception
    {
        long n1, n2;
        try ( Transaction tx = session.beginTransaction() )
        {
            n1 = tx.dataWrite().nodeCreate();
            n2 = tx.dataWrite().nodeCreate();
            tx.success();
        }

        try ( Transaction tx = session.beginTransaction() )
        {
            int label = tx.tokenWrite().relationshipTypeGetOrCreateForName( "R" );
            long r = tx.dataWrite().relationshipCreate( n1, label, n2 );
            try ( NodeCursor node = cursors.allocateNodeCursor();
                    RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                tx.dataRead().singleNode( n1, node );
                assertTrue( node.next(), "should access node" );

                node.allRelationships( relationship );
                assertTrue( relationship.next(), "should find relationship" );
                assertEquals( r, relationship.relationshipReference() );

                tx.dataWrite().relationshipCreate( n1, label, n2 ); // should not be seen
                assertFalse( relationship.next(), "should not find relationship added after cursor init" );
            }
            tx.success();
        }
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroups() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.sparse( graphDb ), false );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroups() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.dense( graphDb ), false );
    }

    @Test
    public void shouldTraverseSparseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.sparse( graphDb ), true );
    }

    @Test
    public void shouldTraverseDenseNodeWithoutGroupsWithDetachedReferences() throws Exception
    {
        traverseWithoutGroups( RelationshipTestSupport.dense( graphDb ), true );
    }

    @Test
    public void shouldTraverseSparseNodeViaGroups() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.sparse( graphDb ), false );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroups() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), false );
    }

    @Test
    public void shouldTraverseSparseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.sparse( graphDb ), true );
    }

    @Test
    public void shouldTraverseDenseNodeViaGroupsWithDetachedReferences() throws Exception
    {
        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), true );
    }

    private void traverseWithoutGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Map<String,Integer> expectedCounts = modifyStartNodeRelationships( start, tx );

            // given
            try ( NodeCursor node = cursors.allocateNodeCursor();
                  RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                // when
                tx.dataRead().singleNode( start.id, node );

                assertTrue( node.next(), "access node" );
                if ( detached )
                {
                    tx.dataRead().relationships( start.id, node.allRelationshipsReference(), relationship );
                }
                else
                {
                    node.allRelationships( relationship );
                }

                Map<String,Integer> counts = count( session, relationship );

                // then
                assertCounts( expectedCounts, counts );
            }

            tx.failure();
        }
    }

    private void traverseViaGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Read read = tx.dataRead();
            Map<String,Integer> expectedCounts = modifyStartNodeRelationships( start, tx );

            // given
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
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), OUTGOING );

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
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), INCOMING );

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
                    assertCount( session, relationship, expectedCounts, group.relationshipLabel(), BOTH );
                }
            }
        }
    }

    private Map<String,Integer> modifyStartNodeRelationships( RelationshipTestSupport.StartNode start, Transaction tx )
            throws KernelException
    {
        Map<String, Integer> expectedCounts = new HashMap<>();
        for ( Map.Entry<String,List<RelationshipTestSupport.StartRelationship>> kv : start.relationships.entrySet() )
        {
            List<RelationshipTestSupport.StartRelationship> rs = kv.getValue();
            RelationshipTestSupport.StartRelationship head = rs.get( 0 );
            int type = session.token().relationshipType( head.type.name() );
            switch ( head.direction )
            {
            case INCOMING:
                tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), type, start.id );
                tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), type, start.id );
                break;
            case OUTGOING:
                tx.dataWrite().relationshipCreate( start.id, type, tx.dataWrite().nodeCreate() );
                tx.dataWrite().relationshipCreate( start.id, type, tx.dataWrite().nodeCreate() );
                break;
            case BOTH:
                tx.dataWrite().relationshipCreate( start.id, type, start.id );
                tx.dataWrite().relationshipCreate( start.id, type, start.id );
                break;
            default:
                throw new IllegalStateException( "Oh ye be cursed, foul checkstyle!" );
            }
            tx.dataWrite().relationshipDelete( head.id );
            expectedCounts.put( kv.getKey(), rs.size() + 1 );
        }

        String newTypeName = "NEW";
        int newType = session.token().relationshipTypeGetOrCreateForName( newTypeName );
        tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), newType, start.id );
        tx.dataWrite().relationshipCreate( start.id, newType, tx.dataWrite().nodeCreate() );
        tx.dataWrite().relationshipCreate( start.id, newType, start.id );

        expectedCounts.put( computeKey( newTypeName, OUTGOING ), 1 );
        expectedCounts.put( computeKey( newTypeName, INCOMING ), 1 );
        expectedCounts.put( computeKey( newTypeName, BOTH ), 1 );

        return expectedCounts;
    }

    private void relateNTimes( int nRelationshipsInStore, int type, long n1, long n2, Transaction tx )
            throws KernelException
    {
        for ( int i = 0; i < nRelationshipsInStore; i++ )
        {
            tx.dataWrite().relationshipCreate( n1, type, n2 );
        }
    }

    private void assertCountRelationships(
            RelationshipScanCursor relationship, int expectedCount, long sourceNode, int type, long targetNode )
    {
        int count = 0;
        while ( relationship.next() )
        {
            assertEquals( sourceNode, relationship.sourceNodeReference() );
            assertEquals( type, relationship.label() );
            assertEquals( targetNode, relationship.targetNodeReference() );
            count++;
        }
        assertEquals( expectedCount, count );
    }
}
