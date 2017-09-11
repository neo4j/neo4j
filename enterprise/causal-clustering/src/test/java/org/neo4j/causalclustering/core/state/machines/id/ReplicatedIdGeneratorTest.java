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
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;
import java.util.function.BooleanSupplier;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mockito;

import org.neo4j.causalclustering.core.consensus.RaftMachine;
import org.neo4j.causalclustering.core.consensus.state.ExposedRaftState;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.store.IdGeneratorContractTest;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.test.rule.TestDirectory;
import org.neo4j.test.rule.fs.DefaultFileSystemRule;
import org.neo4j.test.rule.fs.FileSystemRule;

import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedIdGeneratorTest extends IdGeneratorContractTest
{
    private NullLogProvider logProvider = NullLogProvider.getInstance();

    @Rule
    public FileSystemRule fileSystemRule = new DefaultFileSystemRule();
    @Rule
    public TestDirectory testDirectory = TestDirectory.testDirectory();
    private File file;
    private FileSystemAbstraction fs;
    private MemberId myself = new MemberId( UUID.randomUUID() );
    private RaftMachine raftMachine = Mockito.mock( RaftMachine.class );
    private ExposedRaftState state = mock( ExposedRaftState.class );
    private final CommandIndexTracker commandIndexTracker = mock( CommandIndexTracker.class );
    private IdReusabilityCondition idReusabilityCondition;

    @Before
    public void setUp() throws Exception
    {
        file = testDirectory.file( "idgen" );
        fs = fileSystemRule.get();
        when( raftMachine.state() ).thenReturn( state );
        idReusabilityCondition = getIdReusabilityCondition();
    }

    @Override
    protected IdGenerator createIdGenerator( int grabSize )
    {
        return openIdGenerator( grabSize );
    }

    @Override
    protected IdGenerator openIdGenerator( int grabSize )
    {
        ReplicatedIdGenerator replicatedIdGenerator =
            new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> 0L, stubAcquirer(), logProvider, grabSize, true );
        return new FreeIdFilteredIdGenerator( replicatedIdGenerator, idReusabilityCondition );
    }

    @Test
    public void shouldCreateIdFileForPersistence() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> 0L, rangeAcquirer, logProvider,
                10, true );

        assertTrue( fs.fileExists( file ) );
    }

    @Test
    public void shouldNotStepBeyondAllocationBoundaryWithoutBurnedId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> 0L, rangeAcquirer, logProvider,
                10, true );

        Set<Long> idsGenerated = collectGeneratedIds( idGenerator, 1024 );

        long minId = min( idsGenerated );
        long maxId = max( idsGenerated );

        assertEquals( 0L, minId );
        assertEquals( 1023L, maxId );
    }

    @Test
    public void shouldNotStepBeyondAllocationBoundaryWithBurnedId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> burnedIds, rangeAcquirer, logProvider,
                10, true );

        Set<Long> idsGenerated = collectGeneratedIds( idGenerator, 1024 - burnedIds );

        long minId = min( idsGenerated );
        long maxId = max( idsGenerated );

        assertEquals( burnedIds, minId );
        assertEquals( 1023, maxId );
    }

    @Test( expected = IllegalStateException.class )
    public void shouldThrowIfAdjustmentFailsDueToInconsistentValues() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) ).thenReturn( allocation( 3, 21, 21 ) );
        ReplicatedIdGenerator idGenerator =
                new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> 42L, rangeAcquirer, logProvider, 10,
                        true );

        idGenerator.nextId();
    }

    @Test
    public void shouldReuseIdOnlyWhenLeader() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        IdGenerator idGenerator = new FreeIdFilteredIdGenerator(
                new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> burnedIds, rangeAcquirer, logProvider, 10, true ),
                idReusabilityCondition );

        idGenerator.freeId( 10 );
        assertEquals( 0, idGenerator.getDefragCount() );
        assertEquals( 23, idGenerator.nextId() );

        when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 6L ); // gap-free
        when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );
        idReusabilityCondition.receive( myself );

        idGenerator.freeId( 10 );
        assertEquals( 1, idGenerator.getDefragCount() );
        assertEquals( 10, idGenerator.nextId() );
        assertEquals( 0, idGenerator.getDefragCount() );
    }

    @Test
    public void shouldReuseIdBeforeHighId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        long burnedIds = 23L;
        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> burnedIds, rangeAcquirer, logProvider,
                10, true );

        assertEquals( 23, idGenerator.nextId() );

        idGenerator.freeId( 10 );
        idGenerator.freeId( 5 );

        assertEquals( 10, idGenerator.nextId() );
        assertEquals( 5, idGenerator.nextId() );
        assertEquals( 24, idGenerator.nextId() );
    }

    @Test
    public void freeIdOnlyWhenReusabilityConditionAllows() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        IdReusabilityCondition idReusabilityCondition = getIdReusabilityCondition();

        long burnedIds = 23L;
        FreeIdFilteredIdGenerator idGenerator = new FreeIdFilteredIdGenerator(
                new ReplicatedIdGenerator( fs, file, IdType.NODE, () -> burnedIds, rangeAcquirer, logProvider, 10, true ),
                idReusabilityCondition );

        idGenerator.freeId( 10 );
        assertEquals( 0, idGenerator.getDefragCount() );
        assertEquals( 23, idGenerator.nextId() );

        when( commandIndexTracker.getAppliedCommandIndex() ).thenReturn( 4L, 6L ); // gap-free
        when( state.lastLogIndexBeforeWeBecameLeader() ).thenReturn( 5L );
        idReusabilityCondition.receive( myself );

        assertEquals( 24, idGenerator.nextId() );
        idGenerator.freeId( 11 );
        assertEquals( 25, idGenerator.nextId() );
        idGenerator.freeId( 6 );
        assertEquals( 6, idGenerator.nextId() );
    }

    private IdReusabilityCondition getIdReusabilityCondition()
    {
        return new IdReusabilityCondition( commandIndexTracker, raftMachine, myself );
    }

    private Set<Long> collectGeneratedIds( ReplicatedIdGenerator idGenerator, long expectedIds )
    {
        Set<Long> idsGenerated = new HashSet<>();

        long nextId;
        for ( int i = 0; i < expectedIds; i++ )
        {
            nextId = idGenerator.nextId();
            assertThat( nextId, greaterThanOrEqualTo( 0L ) );
            idsGenerated.add( nextId );
        }

        try
        {
            idGenerator.nextId();
            fail( "Too many ids produced, expected " + expectedIds );
        }
        catch ( NoMoreIds e )
        {
            // rock and roll!
        }

        return idsGenerated;
    }

    private ReplicatedIdRangeAcquirer simpleRangeAcquirer( IdType idType, long start, int length )
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        //noinspection unchecked
        when( rangeAcquirer.acquireIds( idType ) )
                .thenReturn( allocation( start, length, -1 ) ).thenThrow( NoMoreIds.class );
        return rangeAcquirer;
    }

    private static class NoMoreIds extends RuntimeException
    {
    }

    private IdAllocation allocation( long start, int length, int highestIdInUse )
    {
        return new IdAllocation( new IdRange( new long[0], start, length ), highestIdInUse, 0 );
    }

    private ReplicatedIdRangeAcquirer stubAcquirer()
    {
        final ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) )
                .thenReturn( allocation( 0, 1024, -1 ) )
                .thenReturn( allocation( 1024, 1024, 1023 ) )
                .thenReturn( allocation( 2048, 1024, 2047 ) )
                .thenReturn( allocation( 3072, 1024, 3071 ) )
                .thenReturn( allocation( 4096, 1024, 4095 ) )
                .thenReturn( allocation( 5120, 1024, 5119 ) )
                .thenReturn( allocation( 6144, 1024, 6143 ) )
                .thenReturn( allocation( 7168, 1024, 7167 ) )
                .thenReturn( allocation( 8192, 1024, 8191 ) )
                .thenReturn( allocation( 9216, 1024, 9215 ) )
                .thenReturn( allocation( -1, 0, 9216 + 1024 ) );
        return rangeAcquirer;
    }
}
