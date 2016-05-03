/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.apache.commons.lang3.NotImplementedException;
import org.junit.Test;
import org.mockito.InOrder;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import java.util.HashSet;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.cursor.Cursor;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.kernel.api.exceptions.EntityNotFoundException;
import org.neo4j.kernel.api.exceptions.KernelException;
import org.neo4j.kernel.impl.api.operations.EntityReadOperations;
import org.neo4j.kernel.impl.api.store.RelationshipIterator;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.ResourceTypes;
import org.neo4j.storageengine.api.Direction;
import org.neo4j.storageengine.api.NodeItem;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.helpers.collection.Iterators.set;

public class TwoPhaseNodeForRelationshipLockingTest
{
    private final EntityReadOperations ops = mock( EntityReadOperations.class );
    private final KernelStatement state = mock( KernelStatement.class );
    private final Locks.Client locks = mock( Locks.Client.class );
    private final long nodeId = 42L;

    {
        when( state.locks() ).thenReturn( locks );
    }

    @Test
    public void shouldLockNodesInOrderAndConsumeTheRelationships() throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( ops, collector );

        returnRelationships( nodeId, false, 21L, 22L, 23L );
        returnNodesForRelationship( 21L, nodeId, 43L );
        returnNodesForRelationship( 22L, 40L, nodeId );
        returnNodesForRelationship( 23L, nodeId, 41L );

        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, state );

        // then
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 40L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 41L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, nodeId );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 43L );
        assertEquals( set( 21L, 22L, 23L ), collector.set );
    }

    @Test
    public void shouldLockNodesInOrderAndConsumeTheRelationshipsAndRetryIfTheNewRelationshipsAreCreated() throws Throwable
    {
        // given
        Collector collector = new Collector();
        TwoPhaseNodeForRelationshipLocking locking = new TwoPhaseNodeForRelationshipLocking( ops, collector );

        returnRelationships( nodeId, true, 21L, 22L, 23L );
        returnNodesForRelationship( 21L, nodeId, 43L );
        returnNodesForRelationship( 22L, 40L, nodeId );
        returnNodesForRelationship( 23L, nodeId, 41L );

        InOrder inOrder = inOrder( locks );

        // when
        locking.lockAllNodesAndConsumeRelationships( nodeId, state );

        // then
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 40L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 41L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, nodeId );

        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, 40L );
        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, 41L );
        inOrder.verify( locks ).releaseExclusive( ResourceTypes.NODE, nodeId );

        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 40L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 41L );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, nodeId );
        inOrder.verify( locks ).acquireExclusive( ResourceTypes.NODE, 43L );
        assertEquals( set( 21L, 22L, 23L ), collector.set );
    }

    private void returnNodesForRelationship( final long relId, final long startNodeId, final long endNodeId )
            throws Exception
    {
        doAnswer( new Answer<Void>()
        {
            @Override
            public Void answer( InvocationOnMock invocation ) throws Throwable
            {
                @SuppressWarnings( "unchecked" ) RelationshipVisitor<RuntimeException> visitor =
                        (RelationshipVisitor<RuntimeException>) invocation.getArguments()[2];
                visitor.visit( relId, 6, startNodeId, endNodeId );
                return null;
            }
        } ).when( ops ).relationshipVisit( eq( state ), eq( relId ), any( RelationshipVisitor.class ) );
    }

    private void returnRelationships( long nodeId, final boolean skipFirst, final long... relIds )
            throws EntityNotFoundException
    {
        //noinspection unchecked
        Cursor<NodeItem> cursor = mock( Cursor.class );
        when( ops.nodeCursorById( state, nodeId ) ).thenReturn( cursor );
        NodeItem nodeItem = mock( NodeItem.class );
        when( cursor.get() ).thenReturn( nodeItem );
        when( nodeItem.getRelationships( Direction.BOTH ) ).thenAnswer( new Answer<RelationshipIterator>()
        {
            private boolean first = skipFirst;

            @Override
            public RelationshipIterator answer( InvocationOnMock invocation ) throws Throwable
            {
                try
                {
                    return new RelationshipIterator()
                    {
                        private int i = first ? 1 : 0;

                        @Override
                        public <EXCEPTION extends Exception> boolean relationshipVisit( long relationshipId,
                                RelationshipVisitor<EXCEPTION> visitor )
                        {
                            throw new NotImplementedException( "don't call this!" );
                        }

                        @Override
                        public boolean hasNext()
                        {
                            return i < relIds.length;
                        }

                        @Override
                        public long next()
                        {
                            if ( !hasNext() )
                            {
                                throw new NoSuchElementException();
                            }
                            return relIds[i++];
                        }
                    };
                }
                finally
                {
                    first = false;
                }
            }
        });
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
