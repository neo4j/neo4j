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
package org.neo4j.coreedge.core.state.machines.id;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.neo4j.coreedge.core.replication.DirectReplicator;
import org.neo4j.coreedge.core.state.storage.InMemoryStateStorage;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertTrue;

public class ReplicatedIdRangeAcquirerTest
{
    private final MemberId memberA =
            new MemberId( UUID.randomUUID() );
    private final MemberId memberB =
            new MemberId( UUID.randomUUID() );

    private final ReplicatedIdAllocationStateMachine idAllocationStateMachine = new ReplicatedIdAllocationStateMachine(
            new InMemoryStateStorage<>( new IdAllocationState() ) );

    private final DirectReplicator<ReplicatedIdAllocationRequest> replicator =
            new DirectReplicator<>( idAllocationStateMachine );

    @Test
    public void consecutiveAllocationsFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateWhenInitialIdIsZero()
            throws Exception
    {
        consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 0 );
    }

    @Test
    public void consecutiveAllocationsFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateWhenInitialIdIsNotZero()
            throws Exception
    {
        consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 1 );
    }

    private void consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId(
            long initialHighId ) throws Exception
    {
        Set<Long> idAllocations = new HashSet<>();
        int idRangeLength = 8;

        ReplicatedIdGenerator generatorOne = createForMemberWithInitialIdAndRangeLength(
                memberA, initialHighId, idRangeLength );
        ReplicatedIdGenerator generatorTwo = createForMemberWithInitialIdAndRangeLength(
                memberB, initialHighId, idRangeLength );

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

    private ReplicatedIdGenerator createForMemberWithInitialIdAndRangeLength(
            MemberId member, long initialHighId, int idRangeLength )
    {
        Map<IdType,Integer> allocationSizes =
                Arrays.stream( IdType.values() ).collect( Collectors.toMap( idType -> idType, idType -> idRangeLength ) );
        ReplicatedIdRangeAcquirer acquirer = new ReplicatedIdRangeAcquirer( replicator, idAllocationStateMachine,
                allocationSizes, member, NullLogProvider.getInstance() );

        return new ReplicatedIdGenerator( IdType.ARRAY_BLOCK, initialHighId, acquirer,
                NullLogProvider.getInstance() );
    }
}
