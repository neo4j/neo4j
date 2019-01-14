/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.state.machines.id;

import org.junit.Rule;
import org.junit.Test;

import java.io.File;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

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
    {
        consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 0 );
    }

    @Test
    public void consecutiveAllocationsFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateWhenInitialIdIsNotZero()
    {
        consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId( 1 );
    }

    private void consecutiveAllocationFromSeparateIdGeneratorsForSameIdTypeShouldNotDuplicateForGivenInitialHighId(
            long initialHighId )
    {
        Set<Long> idAllocations = new HashSet<>();
        int idRangeLength = 8;

        FileSystemAbstraction fs = defaultFileSystemRule.get();
        File generatorFile1 = testDirectory.file( "gen1" );
        File generatorFile2 = testDirectory.file( "gen2" );
        try ( ReplicatedIdGenerator generatorOne = createForMemberWithInitialIdAndRangeLength( memberA, initialHighId, idRangeLength, fs, generatorFile1 );
              ReplicatedIdGenerator generatorTwo = createForMemberWithInitialIdAndRangeLength( memberB, initialHighId, idRangeLength, fs, generatorFile2 ); )
        {
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
