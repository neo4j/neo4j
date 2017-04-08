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

import org.junit.Test;

import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.impl.store.IdGeneratorContractTest;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.kernel.impl.store.id.IdType;
import org.neo4j.logging.NullLogProvider;

import static java.util.Collections.max;
import static java.util.Collections.min;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedIdGeneratorTest extends IdGeneratorContractTest
{
    private NullLogProvider logProvider = NullLogProvider.getInstance();

    @Override
    protected IdGenerator createIdGenerator( int grabSize )
    {
        return openIdGenerator( grabSize );
    }

    @Override
    protected IdGenerator openIdGenerator( int grabSize )
    {
        return new ReplicatedIdGenerator( IdType.NODE, 0, stubAcquirer(), logProvider );
    }

    @Test
    public void shouldNotStepBeyondAllocationBoundaryWithoutBurnedId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = simpleRangeAcquirer( IdType.NODE, 0, 1024 );

        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( IdType.NODE, 0, rangeAcquirer, logProvider );

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

        int burnedIds = 23;
        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( IdType.NODE, burnedIds, rangeAcquirer, logProvider );

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
                new ReplicatedIdGenerator( IdType.NODE, 42, rangeAcquirer, logProvider );

        idGenerator.nextId();
    }

    private Set<Long> collectGeneratedIds( ReplicatedIdGenerator idGenerator, int expectedIds )
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
