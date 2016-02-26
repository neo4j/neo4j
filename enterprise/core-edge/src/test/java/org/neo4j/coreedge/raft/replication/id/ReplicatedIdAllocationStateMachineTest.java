/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.raft.replication.id;

import java.io.IOException;

import org.junit.Test;

import org.neo4j.coreedge.raft.state.InMemoryStateStorage;
import org.neo4j.coreedge.raft.state.id_allocation.IdAllocationState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;

import static junit.framework.TestCase.assertEquals;

public class ReplicatedIdAllocationStateMachineTest
{
    CoreMember me =
            new CoreMember( new AdvertisedSocketAddress( "a:1" ), new AdvertisedSocketAddress( "a:2" ) );

    IdType someType = IdType.NODE;
    IdType someOtherType = IdType.RELATIONSHIP;

    @Test
    public void shouldNotHaveAnyIdsInitially() throws IOException
    {
        // given
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );

        // then
        assertEquals( 0, pendingRequests.firstUnallocated( someType ) );
    }

    @Test
    public void shouldUpdateStateOnlyForTypeRequested() throws Exception
    {
        // given
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );
        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024 );

        // when
        stateMachine.applyCommand( idAllocationRequest, 0 );

        // then
        assertEquals( 1024, pendingRequests.firstUnallocated( someType ) );
        assertEquals( 0, pendingRequests.firstUnallocated( someOtherType ) );
    }

    @Test
    public void severalDistinctRequestsShouldIncrementallyUpdate() throws IOException
    {
        // given
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );
        long index = 0;

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), index++ );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), index++ );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), index );

        // then
        assertEquals( 3072, pendingRequests.firstUnallocated( someType ) );
    }

    @Test
    public void severalEqualRequestsShouldOnlyUpdateOnce() throws IOException
    {
        // given
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );

        // then
        assertEquals( 1024, pendingRequests.firstUnallocated( someType ) );
    }


    @Test
    public void outOfOrderRequestShouldBeIgnored() throws IOException
    {
        // given
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), 0 );

        // then
        assertEquals( 1024, pendingRequests.firstUnallocated( someType ) );
    }

    @Test
    public void shouldIgnoreNotContiguousRequestAndAlreadySeenIndex() throws Exception
    {
        PendingIdAllocationRequests pendingRequests = new PendingIdAllocationRequests();
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ), pendingRequests, NullLogProvider.getInstance() );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0L, 10 ), 0L );
        assertEquals( 10L, pendingRequests.firstUnallocated( someType ) );

        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10 ), 1L );
        assertEquals( 10L, pendingRequests.firstUnallocated( someType ) );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 10L, 10 ), 2L );
        assertEquals( 20L, pendingRequests.firstUnallocated( someType ) );

        // try applying the same command again. The requested range is now contiguous, but the log index
        // has already been exceeded
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10 ), 1L );
        assertEquals( 20L, pendingRequests.firstUnallocated( someType ) );
    }
}
