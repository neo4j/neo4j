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
package org.neo4j.collection.primitive;

import org.junit.Test;

import org.neo4j.memory.LocalMemoryTracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class PrimitiveCollectionsAllocationsTest
{

    @Test
    public void trackPrimitiveMemoryAllocations()
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        PrimitiveIntSet offHeapIntSet = Primitive.offHeapIntSet( memoryTracker );
        assertTrue( memoryTracker.usedDirectMemory() > 0 );

        offHeapIntSet.close();
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }

    @Test
    public void trackPrimitiveMemoryOnResize()
    {
        LocalMemoryTracker memoryTracker = new LocalMemoryTracker();
        PrimitiveIntSet offHeapIntSet = Primitive.offHeapIntSet( memoryTracker );
        long originalSetMemory = memoryTracker.usedDirectMemory();

        for ( int i = 0; i < Primitive.DEFAULT_OFFHEAP_CAPACITY + 1; i++ )
        {
            offHeapIntSet.add( i );
        }

        assertTrue( memoryTracker.usedDirectMemory() > originalSetMemory );

        offHeapIntSet.close();
        assertEquals( 0, memoryTracker.usedDirectMemory() );
    }
}
