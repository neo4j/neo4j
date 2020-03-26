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

import org.junit.jupiter.api.Test;

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class MemoryPoolImplTest
{
    @Test
    void unboundedShouldBeUnbounded()
    {
        MemoryPool memoryPool = new MemoryPoolImpl.UnboundedMemoryPool();
        memoryPool.reserve( Long.MAX_VALUE - 1 );
        assertEquals( 1, memoryPool.free() );
        memoryPool.release( Long.MAX_VALUE - 2 );
        assertEquals( 1, memoryPool.used() );
    }

    @Test
    void imposeLimit()
    {
        final long limit = 10;
        final long halfLimit = limit / 2;
        MemoryPool memoryPool = new MemoryPoolImpl.BoundedMemoryPool( limit );
        assertState( limit, limit, 0, memoryPool );

        memoryPool.reserve( halfLimit );
        assertState( limit, halfLimit, halfLimit, memoryPool );

        memoryPool.reserve( halfLimit );
        assertState( limit, 0, limit, memoryPool );

        HeapMemoryLimitExceeded heapMemoryLimitExceeded = assertThrows( HeapMemoryLimitExceeded.class, () -> memoryPool.reserve( 1 ) );
        assertThat( heapMemoryLimitExceeded.getMessage() ).contains( "The allocation of 1 would use more than the limit " + limit );

        memoryPool.release( halfLimit );
        assertState( limit, halfLimit, halfLimit, memoryPool );

        memoryPool.reserve( 1 );
        assertState( limit, halfLimit - 1, halfLimit + 1, memoryPool );
    }

    @SuppressWarnings( "SameParameterValue" )
    private static void assertState( long available, long free, long used, MemoryPool memoryPool )
    {
        assertEquals( available, memoryPool.totalSize() );
        assertEquals( free, memoryPool.free() );
        assertEquals( used, memoryPool.used() );
    }
}
