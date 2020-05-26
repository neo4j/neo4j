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
package org.neo4j.collection.trackable;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.Iterator;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

@ExtendWith( RandomExtension.class )
class HeapTrackingArrayListTrackingTest
{
    @Inject
    private RandomRule rnd;

    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();
    private final HeapTrackingArrayList<Long> list = HeapTrackingArrayList.newArrayList( 16, memoryTracker );

    @AfterEach
    void tearDown()
    {
        list.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void addAndIterateElements()
    {
        long iterations = rnd.nextLong( 10, 1000 );
        for ( int i = 0; i < iterations; i++ )
        {
            list.add( (long) i );
        }

        // Validate size
        long itemSize = meter.measure( 1L ) * iterations;
        long actualSize = meter.measureDeep( list ) - meter.measureDeep( memoryTracker ) - itemSize;
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() );

        // Validate items
        Iterator<Long> iterator = list.iterator();
        for ( int i = 0; i < iterations; i++ )
        {
            assertTrue( iterator.hasNext() );
            assertEquals( i, iterator.next() );
        }
        assertFalse( iterator.hasNext() );
    }

}
