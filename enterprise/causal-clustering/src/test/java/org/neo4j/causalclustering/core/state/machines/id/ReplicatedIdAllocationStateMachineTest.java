/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Test;

import java.io.IOException;
import java.util.UUID;

import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.impl.store.id.IdType;

import static org.junit.Assert.assertEquals;

public class ReplicatedIdAllocationStateMachineTest
{
    private MemberId me = new MemberId( UUID.randomUUID() );

    private IdType someType = IdType.NODE;
    private IdType someOtherType = IdType.RELATIONSHIP;

    @Test
    public void shouldNotHaveAnyIdsInitially()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );

        // then
        assertEquals( 0, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void shouldUpdateStateOnlyForTypeRequested()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );
        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024 );

        // when
        stateMachine.applyCommand( idAllocationRequest, 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
        assertEquals( 0, stateMachine.firstUnallocated( someOtherType ) );
    }

    @Test
    public void severalDistinctRequestsShouldIncrementallyUpdate()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );
        long index = 0;

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), index++, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), index++, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), index, r -> {} );

        // then
        assertEquals( 3072, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void severalEqualRequestsShouldOnlyUpdateOnce()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0, r -> {} );
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void outOfOrderRequestShouldBeIgnored()
    {
        // given
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );

        // when
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0, r -> {} );
        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), 0, r -> {} );

        // then
        assertEquals( 1024, stateMachine.firstUnallocated( someType ) );
    }

    @Test
    public void shouldIgnoreNotContiguousRequestAndAlreadySeenIndex()
    {
        ReplicatedIdAllocationStateMachine stateMachine = new ReplicatedIdAllocationStateMachine(
                new InMemoryStateStorage<>( new IdAllocationState() ) );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0L, 10 ), 0L, r -> {} );
        assertEquals( 10L, stateMachine.firstUnallocated( someType ) );

        // apply command that doesn't consume ids because the requested range is non-contiguous
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10 ), 1L, r -> {} );
        assertEquals( 10L, stateMachine.firstUnallocated( someType ) );

        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 10L, 10 ), 2L, r -> {} );
        assertEquals( 20L, stateMachine.firstUnallocated( someType ) );

        // try applying the same command again. The requested range is now contiguous, but the log index
        // has already been exceeded
        stateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 20L, 10 ), 1L, r -> {} );
        assertEquals( 20L, stateMachine.firstUnallocated( someType ) );
    }
}
