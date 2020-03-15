/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import org.mockito.InOrder;

import java.util.HashSet;
import java.util.Set;
import java.util.function.LongPredicate;

import org.neo4j.internal.kernel.api.helpers.StubCursorFactory;
import org.neo4j.internal.kernel.api.helpers.StubNodeCursor;
import org.neo4j.internal.kernel.api.helpers.StubRead;
import org.neo4j.internal.kernel.api.helpers.StubRelationshipCursor;
import org.neo4j.internal.kernel.api.helpers.TestRelationshipChain;
import org.neo4j.kernel.impl.api.KernelTransactionImplementation;
import org.neo4j.kernel.impl.locking.Locks;
import org.neo4j.kernel.impl.locking.SimpleStatementLocks;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.mockito.Mockito.when;
import static org.neo4j.internal.helpers.collection.Iterators.set;
import static org.neo4j.lock.LockTracer.NONE;
import static org.neo4j.lock.ResourceTypes.NODE;
import static org.neo4j.lock.ResourceTypes.RELATIONSHIP;

class DetachingRelationshipDeleterTest
{
    private static final long nodeId = 42L;
    private static final int TYPE = 77;
    private final KernelTransactionImplementation ktx = mock( KernelTransactionImplementation.class );
    private final Locks.Client locks = mock( Locks.Client.class );

    @Test
    void shouldLockNodesInOrderAndConsumeTheRelationships()
    {
        // given
        Collector collector = new Collector();
        DetachingRelationshipDeleter locking = new DetachingRelationshipDeleter( collector );

        returnRelationships( ktx, new TestRelationshipChain( nodeId ).outgoing( 21L, 43L, 0 )
                        .incoming( 22L, 40L, TYPE )
                        .outgoing( 23L, 41L, TYPE )
                        .outgoing( 2L, 3L, TYPE )
                        .incoming( 3L, 49L, TYPE )
                        .outgoing( 50L, 41L, TYPE ) );
        when( ktx.statementLocks() ).thenReturn( new SimpleStatementLocks( locks ) );
        InOrder inOrder = inOrder( locks );

        // when
        locking.lockNodesAndDeleteRelationships( nodeId, ktx );

        // then
        inOrder.verify( locks ).acquireExclusive( NONE, NODE, 3L, 40L, 41L, nodeId, 43L, 49L );
        inOrder.verify( locks ).acquireExclusive( NONE, RELATIONSHIP, 2L );
        inOrder.verify( locks ).acquireExclusive( NONE, RELATIONSHIP, 3L );
        inOrder.verify( locks ).acquireExclusive( NONE, RELATIONSHIP, 22L );
        inOrder.verify( locks ).acquireExclusive( NONE, RELATIONSHIP, 23L );
        inOrder.verify( locks ).acquireExclusive( NONE, RELATIONSHIP, 50L );
        assertEquals( set( 21L, 22L, 23L, 2L, 3L, 50L ), collector.set );
    }

    @Test
    void lockNodeWithoutRelationships()
    {
        Collector collector = new Collector();
        DetachingRelationshipDeleter locking = new DetachingRelationshipDeleter( collector );
        returnRelationships( ktx, new TestRelationshipChain( 42 ) );
        when( ktx.statementLocks() ).thenReturn( new SimpleStatementLocks( locks ) );

        locking.lockNodesAndDeleteRelationships( nodeId, ktx );

        verify( locks ).acquireExclusive( NONE, NODE, nodeId );
        verifyNoMoreInteractions( locks );
    }

    public static void returnRelationships( KernelTransactionImplementation ktx, final TestRelationshipChain relIds )
    {
        StubRead read = new StubRead();
        when( ktx.dataRead() ).thenReturn( read );
        StubCursorFactory cursorFactory = new StubCursorFactory( true );
        cursorFactory.withRelationshipTraversalCursors( new StubRelationshipCursor( relIds ) );

        when( ktx.lockTracer() ).thenReturn( NONE );
        when( ktx.cursors() ).thenReturn( cursorFactory );
        when( ktx.ambientNodeCursor() ).thenAnswer( args -> new StubNodeCursor( false ).withNode( nodeId ) );
    }

    private static class Collector implements LongPredicate
    {
        public final Set<Long> set = new HashSet<>();

        @Override
        public boolean test( long input )
        {
            set.add( input );
            return true;
        }
    }
}
