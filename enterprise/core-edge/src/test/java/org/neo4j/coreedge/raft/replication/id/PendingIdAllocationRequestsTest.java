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

import static org.junit.Assert.*;

/*
 * Copyright (C) 2012 Neo Technology
 * All rights reserved
 */
public class PendingIdAllocationRequestsTest
{

    //    @Test
//    public void shouldUpdateHighestIdRangeForSelf()
//    {
//        // given
//        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine(
//                new InMemoryStateStorage<>( new IdAllocationState() ),
//                new PendingIdAllocationRequests(), NullLogProvider.getInstance() );
//        ReplicatedIdAllocationRequest idAllocationRequest = new ReplicatedIdAllocationRequest( me, someType, 0, 1024 );
//
//        // when
//        idAllocationStateMachine.applyCommand( idAllocationRequest, 0 );
//        IdRange highestIdRange = idAllocationStateMachine.getHighestIdRange( me, someType );
//
//        // then
//        assertEquals( 0, highestIdRange.getRangeStart() );
//        assertEquals( 1024, highestIdRange.getRangeLength() );
//    }

//    @Test
//    public void requestLosingRaceShouldBeIgnored()
//    {
//        // given
//        InMemoryStateStorage<IdAllocationState> storage = new InMemoryStateStorage<>( new IdAllocationState() );
//        ReplicatedIdAllocationStateMachine idAllocationStateMachine =
//                new ReplicatedIdAllocationStateMachine( storage,
//                new PendingIdAllocationRequests(), NullLogProvider.getInstance() );
//
//        // when
//        idAllocationStateMachine.applyCommand( new ReplicatedIdAllocationRequest( someoneElse, someType, 0, 1024 ), 0 );
//        idAllocationStateMachine.applyCommand( new ReplicatedIdAllocationRequest( me, someType, 0, 1024 ), 0 ); //
//        // should be ignored - someone else took it first
//
//        IdRange highestIdRange = idAllocationStateMachine.getHighestIdRange( me, someType );
//
//        // then
//        assertEquals( null, highestIdRange );
//    }

}