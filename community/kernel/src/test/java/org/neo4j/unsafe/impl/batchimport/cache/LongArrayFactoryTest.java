/**
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Test;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

public class LongArrayFactoryTest
{
    private static final int KILO = 1024;
    private static final long MEGA = KILO*KILO;
    private static final long GIGA = KILO*MEGA;

    @Test
    public void shouldAllocateOnHeapIfAvailable() throws Exception
    {
        // GIVEN
        AvailableMemoryCalculator memory = mock( AvailableMemoryCalculator.class );
        when( memory.availableHeapMemory() ).thenReturn( 10*MEGA );
        LongArrayFactory factory = new LongArrayFactory.AutoLongArrayFactory( memory, 10*KILO );

        // WHEN
        LongArray array = factory.newLongArray( 1*KILO );

        // THEN
        assertTrue( array instanceof HeapLongArray );
    }

    @Test
    public void shouldAllocateOffHeapIfNotEnoughHeapAvailable() throws Exception
    {
        // GIVEN
        AvailableMemoryCalculator memory = mock( AvailableMemoryCalculator.class );
        when( memory.availableHeapMemory() ).thenReturn( 1*MEGA );
        when( memory.availableOffHeapMemory() ).thenReturn( 10*MEGA );
        LongArrayFactory factory = new LongArrayFactory.AutoLongArrayFactory( memory, 10*KILO );

        // WHEN
        LongArray array = factory.newLongArray( 1*MEGA );

        // THEN
        assertTrue( array instanceof OffHeapLongArray );
    }

    @Test
    public void shouldAllocateDynamicGrowingInBothHeapAndOffHeapIfEnoughCollectiveMemoryAvailable() throws Exception
    {
        // GIVEN
        AvailableMemoryCalculator memory = mock( AvailableMemoryCalculator.class );
        when( memory.availableHeapMemory() ).thenReturn( 5*MEGA );
        when( memory.availableOffHeapMemory() ).thenReturn( 5*MEGA );
        LongArrayFactory factory = new LongArrayFactory.AutoLongArrayFactory( memory, 10*KILO );

        // WHEN
        LongArray array = factory.newLongArray( 1*MEGA ); // i.e. 8 Mb

        // THEN
        assertTrue( array instanceof DynamicLongArray );
    }
}
