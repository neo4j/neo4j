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

import org.junit.Test;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.neo4j.internal.kernel.api.RelationshipTestSupport.assertCounts;
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
                assertTrue( "should find relationship", relationship.next() );

                assertEquals( label, relationship.label() );
                assertEquals( n1, relationship.sourceNodeReference() );
                assertEquals( n2, relationship.targetNodeReference() );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( "should only find one relationship", relationship.next() );
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
            assertTrue( "should delete relationship", tx.dataWrite().relationshipDelete( r ) );
            try ( RelationshipScanCursor relationship = cursors.allocateRelationshipScanCursor() )
            {
                tx.dataRead().singleRelationship( r, relationship );
                assertFalse( "should not find relationship", relationship.next() );
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
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertTrue( "should find relationship", relationship.next() );
                assertEquals( r, relationship.relationshipReference() );

                assertFalse( "should only find one relationship", relationship.next() );
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
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertFalse( "should not find relationship", relationship.next() );
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
                assertTrue( "should access node", node.next() );

                node.allRelationships( relationship );
                assertTrue( "should find relationship", relationship.next() );
                assertEquals( r, relationship.relationshipReference() );

                tx.dataWrite().relationshipCreate( n1, label, n2 ); // should not be seen
                assertFalse( "should not find relationship added after cursor init", relationship.next() );
            }
            tx.success();
        }
    }

//    @Test
//    public void shouldTraverseSparseNodeViaGroups() throws Exception
//    {
//        traverseViaGroups( RelationshipTestSupport.sparse( graphDb ), false );
//    }
//
//    @Test
//    public void shouldTraverseDenseNodeViaGroups() throws Exception
//    {
//        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), false );
//    }
//
//    @Test
//    public void shouldTraverseSparseNodeViaGroupsWithDetachedReferences() throws Exception
//    {
//        traverseViaGroups( RelationshipTestSupport.sparse( graphDb ), true );
//    }
//
//    @Test
//    public void shouldTraverseDenseNodeViaGroupsWithDetachedReferences() throws Exception
//    {
//        traverseViaGroups( RelationshipTestSupport.dense( graphDb ), true );
//    }

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

    private void traverseWithoutGroups( RelationshipTestSupport.StartNode start, boolean detached ) throws Exception
    {
        try ( Transaction tx = session.beginTransaction() )
        {
            Map<String, Integer> expectedCounts = new HashMap<>();
            for ( Map.Entry<String,List<RelationshipTestSupport.R>> kv : start.relationships.entrySet() )
            {
                List<RelationshipTestSupport.R> r = kv.getValue();
                RelationshipTestSupport.R head = r.get( 0 );
                int label = session.token().relationshipType( head.type.name() );
                switch ( head.direction )
                {
                case INCOMING:
                    tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), label, start.id );
                    tx.dataWrite().relationshipCreate( tx.dataWrite().nodeCreate(), label, start.id );
                    break;
                case OUTGOING:
                    tx.dataWrite().relationshipCreate( start.id, label, tx.dataWrite().nodeCreate() );
                    tx.dataWrite().relationshipCreate( start.id, label, tx.dataWrite().nodeCreate() );
                    break;
                case BOTH:
                    tx.dataWrite().relationshipCreate( start.id, label, start.id );
                    tx.dataWrite().relationshipCreate( start.id, label, start.id );
                    break;
                    default:
                        throw new IllegalStateException( "Oh ye be cursed, foul checkstyle!" );
                }
                tx.dataWrite().relationshipDelete( head.id );
                expectedCounts.put( kv.getKey(), r.size() + 1 );
            }

            // given
            try ( NodeCursor node = cursors.allocateNodeCursor();
                    RelationshipTraversalCursor relationship = cursors.allocateRelationshipTraversalCursor() )
            {
                // when
                tx.dataRead().singleNode( start.id, node );

                assertTrue( "access node", node.next() );
                if ( detached )
                {
                    tx.dataRead().relationships( start.id, node.allRelationshipsReference(), relationship );
                }
                else
                {
                    node.allRelationships( relationship );
                }

                Map<String,Integer> counts = new HashMap<>();
                count( session, relationship, counts, false );

                // then
                assertCounts( expectedCounts, counts );
            }

            tx.failure();
        }
    }
}
