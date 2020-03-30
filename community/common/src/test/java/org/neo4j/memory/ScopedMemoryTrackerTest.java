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

import static org.junit.jupiter.api.Assertions.assertEquals;

class ScopedMemoryTrackerTest
{
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final ScopedMemoryTracker scopedMemoryTracker = new ScopedMemoryTracker( memoryTracker );

    @Test
    void delegatesToParent()
    {
        scopedMemoryTracker.allocateNative( 10 );
        scopedMemoryTracker.releaseNative( 2 );
        scopedMemoryTracker.allocateHeap( 12 );
        scopedMemoryTracker.releaseHeap( 1 );

        assertEquals( 8, memoryTracker.usedNativeMemory() );
        assertEquals( 11, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void dontReleaseParentsResources()
    {
        memoryTracker.allocateNative( 1 );
        memoryTracker.allocateHeap( 3 );

        scopedMemoryTracker.allocateNative( 10 );
        scopedMemoryTracker.releaseNative( 2 );
        scopedMemoryTracker.allocateHeap( 12 );
        scopedMemoryTracker.releaseHeap( 1 );

        assertEquals( 9, memoryTracker.usedNativeMemory() );
        assertEquals( 8, scopedMemoryTracker.usedNativeMemory() );
        assertEquals( 14, memoryTracker.estimatedHeapMemory() );
        assertEquals( 11, scopedMemoryTracker.estimatedHeapMemory() );

        scopedMemoryTracker.close();

        assertEquals( 1, memoryTracker.usedNativeMemory() );
        assertEquals( 3, memoryTracker.estimatedHeapMemory() );
    }
}
