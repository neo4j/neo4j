/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.causalclustering.core.replication.DirectReplicator;
import org.neo4j.causalclustering.core.state.storage.InMemoryStateStorage;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static org.junit.Assert.assertTrue;

public class ReplicatedIdRangeAcquirerTest
{
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    @Rule
    public FileSystemRule defaultFileSystemRule = new DefaultFileSystemRule();

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

        FileSystemAbstraction fs = defaultFileSystemRule.get();
        ReplicatedIdGenerator generatorOne = createForMemberWithInitialIdAndRangeLength(
                memberA, initialHighId, idRangeLength, fs, testDirectory.file( "gen1" ) );
        ReplicatedIdGenerator generatorTwo = createForMemberWithInitialIdAndRangeLength(
                memberB, initialHighId, idRangeLength, fs, testDirectory.file( "gen2" ) );

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

    private ReplicatedIdGenerator createForMemberWithInitialIdAndRangeLength( MemberId member, long initialHighId,
            int idRangeLength, FileSystemAbstraction fs, File file )
    {
        Map<IdType,Integer> allocationSizes =
                Arrays.stream( IdType.values() ).collect( Collectors.toMap( idType -> idType, idType -> idRangeLength ) );
        ReplicatedIdRangeAcquirer acquirer = new ReplicatedIdRangeAcquirer( replicator, idAllocationStateMachine,
                allocationSizes, member, NullLogProvider.getInstance() );

        return new ReplicatedIdGenerator( fs, file, IdType.ARRAY_BLOCK, () -> initialHighId, acquirer,
                NullLogProvider.getInstance(), 10, true );
    }
}
