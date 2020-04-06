/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.memory;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class LocalMemoryTrackerWithPoolTest
{
    private static final long LOCAL_LIMIT = 10;
    private static final long GRAB_SIZE = 2;

    private MemoryPool memoryPool;
    private LocalMemoryTracker memoryTracker;

    @BeforeEach
    void setUp()
    {
        memoryPool = MemoryPools.fromLimit( 0 );
        memoryTracker = new LocalMemoryTracker( memoryPool, LOCAL_LIMIT, GRAB_SIZE );
    }

    @AfterEach
    void tearDown()
    {
        memoryTracker.reset();
        assertReserved( 0 );
    }

    @Test
    void grabSize()
    {
        memoryTracker.allocateHeap( 1 );
        assertReserved( GRAB_SIZE );
    }

    @Test
    void respectsLocalLimit()
    {
        assertThrows( HeapMemoryLimitExceeded.class, () -> memoryTracker.allocateHeap( LOCAL_LIMIT + 1 ) );
    }

    @Test
    void reserveFromParentWhenLocalPoolIsEmpty()
    {
        memoryTracker.allocateHeap( GRAB_SIZE + 2 );
        assertThat( memoryPool.used() ).isGreaterThan( GRAB_SIZE );
    }

    @Test
    void negativeAdjustments()
    {
        memoryTracker.allocateHeap( 1 );
        memoryTracker.releaseHeap( 1 );
        memoryTracker.allocateHeap( 1 );
        memoryTracker.releaseHeap( 1 );
        memoryTracker.allocateHeap( 1 );
        memoryTracker.releaseHeap( 1 );
        assertReserved( GRAB_SIZE );
    }

    @Test
    void largeAdjustments()
    {
        memoryTracker.allocateHeap( LOCAL_LIMIT );
        assertThat( memoryPool.used() ).isGreaterThanOrEqualTo( LOCAL_LIMIT );
    }

    @Test
    void zeroAdjustmentsAllowed()
    {
        memoryTracker.allocateHeap( 0 );
    }

    private void assertReserved( long i )
    {
        assertEquals( i, memoryPool.used() );
    }
}
