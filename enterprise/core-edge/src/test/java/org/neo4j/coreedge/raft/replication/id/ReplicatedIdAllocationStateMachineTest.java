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

import org.junit.Test;

import org.neo4j.coreedge.raft.state.id_allocation.InMemoryIdAllocationState;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.logging.NullLogProvider;

import static junit.framework.TestCase.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyZeroInteractions;
import static org.mockito.Mockito.when;

public class ReplicatedIdAllocationStateMachineTest
{
    CoreMember me = new CoreMember( new AdvertisedSocketAddress( "a:1" ), new AdvertisedSocketAddress( "a:2" ) );
    CoreMember someoneElse = new CoreMember( new AdvertisedSocketAddress( "b:1" ),
            new AdvertisedSocketAddress( "b:2" ) );

    IdType someType = IdType.NODE;
    IdType someOtherType = IdType.RELATIONSHIP;

    @Test
    public void shouldNotHaveAnyIdsInitially()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );

        // when
        IdRange myHighestIdRange = idAllocationStateMachine.getHighestIdRange( me, someType );
        long firstNotAllocated = idAllocationStateMachine.getFirstNotAllocated( someType );

        // then
        assertEquals( null, myHighestIdRange );
        assertEquals( 0, firstNotAllocated );
    }

    @Test
    public void shouldUpdateStateOnlyForTypeRequested()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );
        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024 );

        // when
        idAllocationStateMachine.onReplicated( idAllocationRequest, 0 );

        // then
        assertEquals( 1024, idAllocationStateMachine.getFirstNotAllocated( someType ) );
        assertEquals( 0, idAllocationStateMachine.getFirstNotAllocated( someOtherType ) );
    }

    @Test
    public void shouldUpdateHighestIdRangeForSelf()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );
        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024 );

        // when
        idAllocationStateMachine.onReplicated( idAllocationRequest, 0 );
        IdRange highestIdRange = idAllocationStateMachine.getHighestIdRange( me, someType );

        // then
        assertEquals( 0, highestIdRange.getRangeStart() );
        assertEquals( 1024, highestIdRange.getRangeLength() );
    }

    @Test
    public void severalDistinctRequestsShouldIncrementallyUpdate()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );
        long index = 0;

        // when
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), index++ );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), index++ );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), index );

        // then
        assertEquals( 3072, idAllocationStateMachine.getFirstNotAllocated( someType ) );
    }

    @Test
    public void severalEqualRequestsShouldOnlyUpdateOnce()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );

        // when
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );

        // then
        assertEquals( 1024, idAllocationStateMachine.getFirstNotAllocated( someType ) );
    }

    @Test
    public void outOfOrderRequestShouldBeIgnored()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );

        // when
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), 0 ); //
        // should be ignored - not adjacent to previous

        // then
        assertEquals( 1024, idAllocationStateMachine.getFirstNotAllocated( someType ) );
    }

    @Test
    public void requestLosingRaceShouldBeIgnored()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationState(), NullLogProvider.getInstance() );

        // when
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( someoneElse, someType, 0, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 ); //
        // should be ignored - someone else took it first

        IdRange highestIdRange = idAllocationStateMachine.getHighestIdRange( me, someType );

        // then
        assertEquals( null, highestIdRange );
    }

    @Test
    public void shouldCorrectlyRestartWithPreviousState() throws Exception
    {
        // given
        IdAllocationState idAllocationState = new InMemoryIdAllocationState();

        ReplicatedIdAllocationStateMachine firstIdAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState, NullLogProvider.getInstance() );

        firstIdAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        firstIdAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), 1 );

        // when
        ReplicatedIdAllocationStateMachine secondIdAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState, NullLogProvider.getInstance() );

        // then
        assertEquals( firstIdAllocationStateMachine.getHighestIdRange( me, someType ),
                secondIdAllocationStateMachine.getHighestIdRange( me, someType ) );

        assertEquals( firstIdAllocationStateMachine.getFirstNotAllocated( someType ),
                secondIdAllocationStateMachine.getFirstNotAllocated( someType ) );

        assertEquals( firstIdAllocationStateMachine.getFirstNotAllocated( someType ),
                secondIdAllocationStateMachine.getFirstNotAllocated( someType ) );
    }

    @Test
    public void shouldIgnoreAlreadySeenIndex() throws Exception
    {
        /*
         * This test essentially verifies that the ReplicatedIdAllocationStateMachine is idempotent. It checks that
         * if an onReplicated() request comes in with an index already seen by the state machine, then it will be
         * promptly ignored. We check that by ensuring that no interactions happen with the mock request, which means
         * that the state it carried was not read and therefor the state machine state was not updated.
         * The state store is chosen to not be a mock because the last seen index is expected to be stored there. This
         * does not imply the index storage details leak from the API, since a mock that only stores the index could
         * be used instead - this is merely a convenience.
         */
        // given
        // a state machine
        final long AN_INDEX = 24;
        IdAllocationState idAllocationState = new InMemoryIdAllocationState();
        ReplicatedIdAllocationStateMachine stateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState, NullLogProvider.getInstance() );

        // which has seen a replicated content at a specific index
        ReplicatedIdAllocationRequest mockRequest = mock( ReplicatedIdAllocationRequest.class );
        when( mockRequest.owner() ).thenReturn( someoneElse );
        when( mockRequest.idType() ).thenReturn( someType );
        stateMachine.onReplicated( mockRequest, AN_INDEX );

        // when
        // we see content at an index before and content at the above index
        ReplicatedIdAllocationRequest replicatedMockRequest = mock( ReplicatedIdAllocationRequest.class );
        when( replicatedMockRequest.owner() ).thenReturn( someoneElse );
        stateMachine.onReplicated( replicatedMockRequest, AN_INDEX - 3 ); // random already seen index

        ReplicatedIdAllocationRequest anotherReplicatedMockRequest = mock( ReplicatedIdAllocationRequest.class );
        when( anotherReplicatedMockRequest.owner() ).thenReturn( someoneElse );
        stateMachine.onReplicated( anotherReplicatedMockRequest, AN_INDEX );

        // then
        // there should be only one interaction with the mock, the one for the first time onReplicated() was called
        verifyZeroInteractions( replicatedMockRequest );
        verifyZeroInteractions( anotherReplicatedMockRequest );
    }

    @Test
    public void shouldContinueAcceptingRequestsAfterIgnoreAlreadySeenIndex() throws Exception
    {
        /*
         * This test completes the previous one (shouldIgnoreAlreadySeenIndex), ensuring that while past indexes are
         * ignored, future indexes will continue to be applied against the state machine.
         */
        // given
        // a state machine
        final long AN_INDEX = 24;
        IdAllocationState idAllocationState = new InMemoryIdAllocationState();
        ReplicatedIdAllocationStateMachine stateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState, NullLogProvider.getInstance() );

        // which has seen a replicated content at a specific index
        ReplicatedIdAllocationRequest mockRequest = mock( ReplicatedIdAllocationRequest.class );
        when( mockRequest.owner() ).thenReturn( someoneElse );
        when( mockRequest.idType() ).thenReturn( someType );
        stateMachine.onReplicated( mockRequest, AN_INDEX );

        // we see content at an index before the last seen one
        ReplicatedIdAllocationRequest replicatedMockRequest = mock( ReplicatedIdAllocationRequest.class );
        when( replicatedMockRequest.owner() ).thenReturn( someoneElse );
        when( replicatedMockRequest.idType() ).thenReturn( someType );
        stateMachine.onReplicated( replicatedMockRequest, AN_INDEX - 3 ); // random already seen index

        // when
        // we receive value for an index larger than the one seen
        ReplicatedIdAllocationRequest newReplicatedRequest = mock( ReplicatedIdAllocationRequest.class );
        when( newReplicatedRequest.owner() ).thenReturn( someoneElse );
        when( newReplicatedRequest.idType() ).thenReturn( someType );
        stateMachine.onReplicated( newReplicatedRequest, AN_INDEX + 12 ); // random future index

        // then
        // there should be two interactions with the mock, the one for the first time onReplicated() was called and
        // another for the index seen after the duplication
        verifyZeroInteractions( replicatedMockRequest );
    }
}
