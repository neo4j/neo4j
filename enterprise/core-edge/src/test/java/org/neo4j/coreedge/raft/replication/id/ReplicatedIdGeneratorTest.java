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

import org.junit.Before;
import org.junit.Test;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import org.neo4j.kernel.IdType;
import org.neo4j.kernel.ha.id.IdAllocation;
import org.neo4j.kernel.impl.store.IdGeneratorContractTest;
import org.neo4j.kernel.impl.store.id.IdGenerator;
import org.neo4j.kernel.impl.store.id.IdRange;
import org.neo4j.logging.NullLogProvider;

import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class ReplicatedIdGeneratorTest extends IdGeneratorContractTest
{
    private final ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );

    @Before
    public void stubAcquirer()
    {
        when( rangeAcquirer.acquireIds( IdType.NODE ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 0, 1024 ), -1, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 1024, 1024 ), 1023, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 2048, 1024 ), 2047, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 3072, 1024 ), 3071, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 4096, 1024 ), 4095, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 5120, 1024 ), 5119, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 6144, 1024 ), 6143, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 7168, 1024 ), 7167, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 8192, 1024 ), 8191, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 9216, 1024 ), 9215, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], -1, 0 ), 9216 + 1024, 0 ) );
    }

    @Override
    protected IdGenerator createIdGenerator( int grabSize )
    {
        return openIdGenerator( grabSize );
    }

    @Override
    protected IdGenerator openIdGenerator( int grabSize )
    {
        return openIdGenerator( 0, grabSize );
    }

    protected IdGenerator openIdGenerator( long highId, int grabSize )
    {
        return new ReplicatedIdGenerator( IdType.NODE, highId, rangeAcquirer, NullLogProvider.getInstance() );
    }

    @Test
    public void shouldNotStepBeyondAllocationBoundaryWithBurnedId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 0, 1024 ), -1, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], -1, 0 ), 1024, 0 ) );

        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( IdType.NODE, 1, rangeAcquirer, NullLogProvider
                .getInstance() );

        Set<Long> idsGenerated = new HashSet<>();

        long nextId;
        while( ( nextId = idGenerator.nextId() ) != -1)
        {
            idsGenerated.add( nextId );
        }

        long minId = Collections.min( idsGenerated );
        long maxId = Collections.max( idsGenerated );

        assertEquals( 1, minId );
        assertEquals( 1023, maxId );
    }

    @Test
    public void shouldNotStepBeyondAllocationBoundaryWithoutBurnedId() throws Exception
    {
        ReplicatedIdRangeAcquirer rangeAcquirer = mock( ReplicatedIdRangeAcquirer.class );
        when( rangeAcquirer.acquireIds( IdType.NODE ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], 0, 1024 ), -1, 0 ) )
                .thenReturn( new IdAllocation( new IdRange( new long[0], -1, 0 ), 1024, 0 ) );

        ReplicatedIdGenerator idGenerator = new ReplicatedIdGenerator( IdType.NODE, 0, rangeAcquirer, NullLogProvider
                .getInstance() );

        Set<Long> idsGenerated = new HashSet<>();

        long nextId;
        while( ( nextId = idGenerator.nextId() ) != -1)
        {
            idsGenerated.add( nextId );
        }

        long minId = Collections.min( idsGenerated );
        long maxId = Collections.max( idsGenerated );

        assertEquals( 0, minId );
        assertEquals( 1023, maxId );
    }
}
