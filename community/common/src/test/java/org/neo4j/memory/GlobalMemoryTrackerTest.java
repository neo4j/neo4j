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

public class GlobalMemoryTrackerTest
{

    @Test
    public void trackMemoryAllocations()
    {
        long initialUsedMemory = GlobalMemoryTracker.INSTANCE.usedDirectMemory();
        GlobalMemoryTracker.INSTANCE.allocated( 10 );
        GlobalMemoryTracker.INSTANCE.allocated( 20 );
        GlobalMemoryTracker.INSTANCE.allocated( 40 );
        assertEquals( 70, GlobalMemoryTracker.INSTANCE.usedDirectMemory() - initialUsedMemory );
    }

    @Test
    public void trackMemoryDeallocations()
    {
        long initialUsedMemory = GlobalMemoryTracker.INSTANCE.usedDirectMemory();
        GlobalMemoryTracker.INSTANCE.allocated( 100 );
        assertEquals( 100, GlobalMemoryTracker.INSTANCE.usedDirectMemory() - initialUsedMemory );

        GlobalMemoryTracker.INSTANCE.deallocated( 20 );
        assertEquals( 80, GlobalMemoryTracker.INSTANCE.usedDirectMemory() - initialUsedMemory );

        GlobalMemoryTracker.INSTANCE.deallocated( 40 );
        assertEquals( 40, GlobalMemoryTracker.INSTANCE.usedDirectMemory() - initialUsedMemory );
    }
}
