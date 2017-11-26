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
package org.neo4j.kernel.impl.api;

import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.internal.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.locking.LockTracer;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.EntityType;
import org.neo4j.storageengine.api.NodeItem;
import org.neo4j.storageengine.api.RelationshipItem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.helpers.collection.Iterators.set;

public class TwoPhaseNodeForRelationshipLockingTest
{
    private final EntityReadOperations ops = mock( EntityReadOperations.class );
    private final KernelStatement state = mock( KernelStatement.class );
    private final Locks.Client locks = mock( Locks.Client.class );
    private final long nodeId = 42L;

    {
        when( state.locks() ).thenReturn( new SimpleStatementLocks( locks ) );
        when( state.lockTracer() ).thenReturn( LockTracer.NONE );
    }

    @Test
    public void shouldLockNodesInOrderAndConsumeTheRelationships() throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( ops, collector );

        returnRelationships(
                ops, state, nodeId, false,
                new RelationshipData( 21L, nodeId, 43L ),
                new RelationshipData( 22L, 40L, nodeId ),
                new RelationshipData( 23L, nodeId, 41L ),
                new RelationshipData( 2L, nodeId, 3L ),
                new RelationshipData( 3L, 49L, nodeId ),
                new RelationshipData( 50L, nodeId, 41L ) );

        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, state );

        // then
        inOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 3L, 40L, 41L, nodeId, 43L, 49L );
        assertEquals( set( 21L, 22L, 23L, 2L, 3L, 50L ), collector.set );
    }

    @Test
    public void shouldLockNodesInOrderAndConsumeTheRelationshipsAndRetryIfTheNewRelationshipsAreCreated()
            throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( ops, collector );

        RelationshipData relationship1 = new RelationshipData( 21L, nodeId, 43L );
        RelationshipData relationship2 = new RelationshipData( 22L, 40L, nodeId );
        RelationshipData relationship3 = new RelationshipData( 23L, nodeId, 41L );
        returnRelationships( ops, state, nodeId, true, relationship1, relationship2, relationship3 );

        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, state );

        // then
        inOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 40L, 41L, nodeId );

        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, 40L );
        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, 41L );
        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, nodeId );

        inOrder.verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, 40L, 41L, nodeId, 43L );
        assertEquals( set( 21L, 22L, 23L ), collector.set );
    }

    @Test
    public void lockNodeWithoutRelationships() throws Exception
    {
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( ops, collector );
        returnRelationships( ops, state, nodeId, false );

        locking.lockAllNodesAndConsumeRelationships( nodeId, state );

        verify( locks ).acquireExclusive( LockTracer.NONE, ResourceTypes.NODE, nodeId );
        verifyNoMoreInteractions( locks );
    }

    public static class RelationshipData
    {
        public final long relId;
        public final long startNodeId;
        public final long endNodeId;

        RelationshipData( long relId, long startNodeId, long endNodeId )
        {
            this.relId = relId;
            this.startNodeId = startNodeId;
            this.endNodeId = endNodeId;
        }

        RelationshipItem asRelationshipItem()
        {
            RelationshipItem rel = mock( RelationshipItem.class );
            when( rel.id() ).thenReturn( relId );
            when( rel.startNode() ).thenReturn( startNodeId );
            when( rel.endNode() ).thenReturn( endNodeId );
            return rel;
        }
    }

    static void returnRelationships( EntityReadOperations ops, KernelStatement state, long nodeId,
            final boolean skipFirst, final RelationshipData... relIds ) throws EntityNotFoundException
    {
        NodeItem nodeItem = mock( NodeItem.class );
        when( ops.nodeGetRelationships( state, nodeItem, Direction.BOTH ) )
                .thenAnswer( new Answer<Cursor<RelationshipItem>>()
                {
                    private boolean first = skipFirst;

                    @Override
                    public Cursor<RelationshipItem> answer( InvocationOnMock invocation ) throws Throwable
                    {
                        try
                        {
                            return new Cursor<RelationshipItem>()
                            {
                                private int i = first ? 1 : 0;
                                private RelationshipData relationshipData;

                                @Override
                                public boolean next()
                                {
                                    boolean next = i < relIds.length;
                                    relationshipData = next ? relIds[i++] : null;
                                    return next;
                                }

                                @Override
                                public RelationshipItem get()
                                {
                                    if ( relationshipData == null )
                                    {
                                        throw new NoSuchElementException();
                                    }

                                    return relationshipData.asRelationshipItem();
                                }

                                @Override
                                public void close()
                                {
                                }
                            };
                        }
                        finally
                        {
                            first = false;
                        }
                    }
                } );

        when( ops.nodeCursorById( state, nodeId ) ).thenAnswer( invocationOnMock ->
        {
            Cursor<NodeItem> cursor = new Cursor<NodeItem>()
            {
                private int i;

                @Override
                public boolean next()
                {
                    return i++ == 0;
                }

                @Override
                public NodeItem get()
                {
                    if ( i != 1 )
                    {
                        throw new NoSuchElementException();
                    }
                    return nodeItem;
                }

                @Override
                public void close()
                {

                }
            };
            if ( !cursor.next() )
            {
                throw new EntityNotFoundException( EntityType.NODE, nodeId );
            }
            return cursor;
        } );
    }

    private static class Collector implements ThrowingConsumer<Long,KernelException>
    {
        public final Set<Long> set = new HashSet<>();

        @Override
        public void accept( Long input ) throws KernelException
        {
            assertNotNull( input );
            set.add( input );
        }
    }
}
