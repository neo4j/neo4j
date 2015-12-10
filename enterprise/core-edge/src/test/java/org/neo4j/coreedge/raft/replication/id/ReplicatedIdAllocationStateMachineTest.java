/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.IdType;
import org.neo4j.kernel.impl.store.id.IdRange;

import static junit.framework.TestCase.assertEquals;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class ReplicatedIdAllocationStateMachineTest
{
    CoreMember me = new CoreMember( address( "a:1" ), address( "a:2" ) );
    CoreMember someoneElse = new CoreMember( address( "b:1" ), address( "b:2" ) );

    IdType someType = IdType.NODE;
    IdType someOtherType = IdType.RELATIONSHIP;

    @Test
    public void shouldNotHaveAnyIdsInitially()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationStateStore() );

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
                new InMemoryIdAllocationStateStore() );
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
                new InMemoryIdAllocationStateStore() );
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
                new InMemoryIdAllocationStateStore() );

        // when
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), 0 );
        idAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 2048, 1024 ), 0 );

        // then
        assertEquals( 3072, idAllocationStateMachine.getFirstNotAllocated( someType ) );
    }

    @Test
    public void severalEqualRequestsShouldOnlyUpdateOnce()
    {
        // given
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( me,
                new InMemoryIdAllocationStateStore() );

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
                new InMemoryIdAllocationStateStore() );

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
                new InMemoryIdAllocationStateStore() );

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
        IdAllocationState idAllocationState = new InMemoryIdAllocationStateStore();

        ReplicatedIdAllocationStateMachine firstIdAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState );

        firstIdAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 );
        firstIdAllocationStateMachine.onReplicated( new ReplicatedIdAllocationRequest( me, someType, 1024, 1024 ), 1 );

        // when
        ReplicatedIdAllocationStateMachine secondIdAllocationStateMachine =
                new ReplicatedIdAllocationStateMachine( me, idAllocationState );

        // then
        assertEquals( firstIdAllocationStateMachine.getHighestIdRange( me, someType ),
                secondIdAllocationStateMachine.getHighestIdRange( me, someType ) );

        assertEquals( firstIdAllocationStateMachine.getFirstNotAllocated( someType ),
                secondIdAllocationStateMachine.getFirstNotAllocated( someType ) );
    }
}
