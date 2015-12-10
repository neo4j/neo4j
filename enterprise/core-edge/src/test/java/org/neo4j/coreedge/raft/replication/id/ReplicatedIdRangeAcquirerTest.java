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

import java.util.HashSet;
import java.util.Set;

import org.junit.Test;

import org.neo4j.coreedge.raft.replication.StubReplicator;
import org.neo4j.coreedge.server.CoreMember;
import org.neo4j.kernel.IdType;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertTrue;

import static org.neo4j.coreedge.server.AdvertisedSocketAddress.address;

public class ReplicatedIdRangeAcquirerTest
{
    private final CoreMember one = new CoreMember( address( "a:1" ), address( "a:2" ) );
    private final CoreMember two = new CoreMember( address( "b:1" ), address( "b:2" ) );
    private final StubReplicator replicator = new StubReplicator();

    @Test
    public void consecutiveAllocationsFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateWhenInitialIdIsZero()
            throws Exception
    {
        consecutiveAllocationFromSeparateIdGeneratosForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 0 );
    }

    @Test
    public void consecutiveAllocationsFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateWhenInitialIdIsNotZero()
            throws Exception
    {
        consecutiveAllocationFromSeparateIdGeneratosForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 1 );

    }

    private void consecutiveAllocationFromSeparateIdGeneratosForSameIdTypeShouldNotDuplicateForGivenInitialHighId(
            long initialHighId ) throws Exception
    {
        Set<Long> idAllocations = new HashSet<>();
        int idRangeLength = 8;

        ReplicatedIdGenerator generatorOne = createForMemberWithInitialIdAndRangeLength( one, initialHighId,
                idRangeLength );
        ReplicatedIdGenerator generatorTwo = createForMemberWithInitialIdAndRangeLength( two, initialHighId,
                idRangeLength );

        // First iteration is bootstrapping the set, so we do it outside the loop to avoid an if check in there
        long newId = generatorOne.nextId();
        idAllocations.add( newId );

        for ( int i = 1; i < idRangeLength - initialHighId; i++ )
        {
            newId = generatorOne.nextId();
            boolean wasNew = idAllocations.add( newId );
            assertTrue( "Id " + newId + " has already been returned", wasNew );
            assertTrue( "Detected gap in id generation, missing " + (newId - 1), idAllocations.contains( newId - 1 ) );
        }

        for ( int i = 0; i < idRangeLength; i++ )
        {
            newId = generatorTwo.nextId();
            boolean wasNew = idAllocations.add( newId );
            assertTrue( "Id " + newId + " has already been returned", wasNew );
            assertTrue( "Detected gap in id generation, missing " + (newId - 1), idAllocations.contains( newId - 1 ) );
        }
    }

    private ReplicatedIdGenerator createForMemberWithInitialIdAndRangeLength( CoreMember member, long initialHighId,
                                                                              int idRangeLength )
    {
        ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine( member,
                new InMemoryIdAllocationStateStore() );

        replicator.subscribe( idAllocationStateMachine );

        ReplicatedIdRangeAcquirer acquirer = new ReplicatedIdRangeAcquirer( replicator,
                idAllocationStateMachine, idRangeLength, 1, member, NullLogProvider.getInstance() );

        ReplicatedIdGenerator generator = new ReplicatedIdGenerator( IdType.ARRAY_BLOCK, initialHighId, acquirer,
                NullLogProvider.getInstance() );

        return generator;
    }
}
