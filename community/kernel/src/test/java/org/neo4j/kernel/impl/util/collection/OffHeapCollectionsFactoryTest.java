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
package org.neo4j.kernel.impl.util.collection;

import org.junit.Before;
import org.junit.Test;

import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.PrimitiveLongSet;
import org.neo4j.kernel.impl.util.diffsets.PrimitiveLongDiffSets;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class OffHeapCollectionsFactoryTest
{
    private MemoryAllocationTracker memoryTracker;
    private OffHeapCollectionsFactory factory;

    @Before
    public void setUp() throws Exception
    {
        memoryTracker = spy( new LocalMemoryTracker() );
        factory = new OffHeapCollectionsFactory( memoryTracker );
    }

    @Test
    public void longSetAllocationAndRelease()
    {
        final long mem0 = memoryTracker.usedDirectMemory();

        final PrimitiveLongSet set = factory.newLongSet();

        final long mem1 = memoryTracker.usedDirectMemory();

        assertNotEquals( mem0, mem1 );

        set.close();

        assertEquals( 0, memoryTracker.usedDirectMemory() );

        verify( memoryTracker ).allocated( anyLong() );
        verify( memoryTracker ).deallocated( anyLong() );
    }

    @Test
    public void longDiffSetsAllocationAndRelease()
    {
        final long mem0 = memoryTracker.usedDirectMemory();

        final PrimitiveLongDiffSets diffSets = factory.newLongDiffSets();
        diffSets.add( 1 );
        diffSets.remove( 2 );

        final long mem1 = memoryTracker.usedDirectMemory();

        assertNotEquals( mem0, mem1 );

        diffSets.close();

        assertEquals( 0, memoryTracker.usedDirectMemory() );

        verify( memoryTracker, times( 2 ) ).allocated( anyLong() );
        verify( memoryTracker, times( 2 ) ).deallocated( anyLong() );
    }

    @Test
    public void longObjectMapAllocationAndRelease()
    {
        final long mem0 = memoryTracker.usedDirectMemory();

        final PrimitiveLongObjectMap<Object> map = factory.newLongObjectMap();
        map.put( 1L, "foo" );

        final long mem1 = memoryTracker.usedDirectMemory();

        assertEquals( "update the test after switching to off-heap map", mem0, mem1 );
    }

    @Test
    public void intObjectMapAllocationAndRelease()
    {
        final long mem0 = memoryTracker.usedDirectMemory();

        final PrimitiveIntObjectMap<Object> map = factory.newIntObjectMap();
        map.put( 1, "foo" );

        final long mem1 = memoryTracker.usedDirectMemory();

        assertEquals( "update the test after switching to off-heap map", mem0, mem1 );
    }
}
