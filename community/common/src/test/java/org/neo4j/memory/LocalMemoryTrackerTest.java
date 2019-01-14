/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class LocalMemoryTrackerTest
{
    @Test
    public void trackMemoryAllocations()
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocated( 10 );
        memoryTracker.allocated( 20 );
        memoryTracker.allocated( 40 );
        assertEquals( 70, memoryTracker.usedDirectMemory());
    }

    @Test
    public void trackMemoryDeallocations()
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        memoryTracker.allocated( 100 );
        assertEquals( 100, memoryTracker.usedDirectMemory() );

        memoryTracker.deallocated( 20 );
        assertEquals( 80, memoryTracker.usedDirectMemory() );

        memoryTracker.deallocated( 40 );
        assertEquals( 40, memoryTracker.usedDirectMemory() );
    }

    @Test
    public void localMemoryTrackerPropagatesAllocationsToGlobalTracker()
    {
        GlobalMemoryTracker globalMemoryTracker = GlobalMemoryTracker.INSTANCE;
        long initialGlobalUsage = globalMemoryTracker.usedDirectMemory();
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();

        memoryTracker.allocated( 100 );
        assertEquals( 100, memoryTracker.usedDirectMemory() );
        assertEquals( 100, globalMemoryTracker.usedDirectMemory() - initialGlobalUsage );

        memoryTracker.deallocated( 50 );
        assertEquals( 50, memoryTracker.usedDirectMemory() );
        assertEquals( 50, globalMemoryTracker.usedDirectMemory() - initialGlobalUsage );
    }

}
