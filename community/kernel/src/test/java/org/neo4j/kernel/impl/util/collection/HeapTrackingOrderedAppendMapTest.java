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
package org.neo4j.kernel.impl.util.collection;

import org.github.jamm.MemoryMeter;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Random;

import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryTracker;
import org.neo4j.values.storable.LongValue;
import org.neo4j.values.storable.Values;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class HeapTrackingOrderedAppendMapTest
{
    private final MemoryMeter meter = new MemoryMeter();
    private final MemoryTracker memoryTracker = new LocalMemoryTracker();

    private final HeapTrackingOrderedAppendMap<LongValue,LongValue> table = HeapTrackingOrderedAppendMap.createOrderedMap( memoryTracker );

    @AfterEach
    void tearDown()
    {
        table.close();
        assertEquals( 0, memoryTracker.estimatedHeapMemory(), "Leaking memory" );
    }

    @Test
    void emptySize()
    {
        long actual = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actual, memoryTracker.estimatedHeapMemory() );
    }

    @Test
    void countInternalStructure()
    {
        // Allocate outside of buffer
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        var key1 = Values.longValue( 1L );
        var key2 = Values.longValue( 2L );
        var key3 = Values.longValue( 3L );
        var value1 = Values.longValue( 11L );
        var value2 = Values.longValue( 12L );
        var value3 = Values.longValue( 13L );

        table.getIfAbsentPutWithMemoryTracker( key1, scopedMemoryTracker -> {
            scopedMemoryTracker.allocateHeap( key1.estimatedHeapUsage() + value1.estimatedHeapUsage() );
            return value1;
        } );
        table.getIfAbsentPutWithMemoryTracker( key2, scopedMemoryTracker -> {
            scopedMemoryTracker.allocateHeap( key2.estimatedHeapUsage() + value2.estimatedHeapUsage() );
            return value2;
        } );
        table.getIfAbsentPutWithMemoryTracker( key3, scopedMemoryTracker -> {
            scopedMemoryTracker.allocateHeap( key3.estimatedHeapUsage() + value3.estimatedHeapUsage() );
            return value3;
        } );
        assertEquals( value1, table.getIfAbsentPutWithMemoryTracker( key1, scopedMemoryTracker -> fail() ) );
        assertEquals( value2, table.getIfAbsentPutWithMemoryTracker( key2, scopedMemoryTracker -> fail() ) );
        assertEquals( value3, table.getIfAbsentPutWithMemoryTracker( key3, scopedMemoryTracker -> fail() ) );

        // Validate size
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize, memoryTracker.estimatedHeapMemory() - externalAllocation );

        // Validate content
        Iterator<? extends Map.Entry<LongValue,LongValue>> iterator = table.autoClosingEntryIterator();
        assertTrue( iterator.hasNext() );
        assertEquals( value1, iterator.next().getValue() );
        assertTrue( iterator.hasNext() );
        assertEquals( value2, iterator.next().getValue() );
        assertTrue( iterator.hasNext() );
        assertEquals( value3, iterator.next().getValue() );
        assertFalse( iterator.hasNext() );
        assertThrows( NoSuchElementException.class, iterator::next );

        // Exhausting the iterator should have closed the buffer automatically at this point
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );
        memoryTracker.releaseHeap( externalAllocation );
    }

    @Test
    void closeShouldReleaseEverything()
    {
        // Allocate outside of table
        long externalAllocation = 113L;
        memoryTracker.allocateHeap( externalAllocation );

        var key1 = Values.longValue( 1L );
        var value1 = Values.longValue( 11L );

        table.getIfAbsentPutWithMemoryTracker( key1, scopedMemoryTracker -> {
            scopedMemoryTracker.allocateHeap( key1.estimatedHeapUsage() + value1.estimatedHeapUsage() );
            return value1;
        } );

        // Validate size
        long actualSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        assertEquals( actualSize + externalAllocation, memoryTracker.estimatedHeapMemory() );

        // Close should release everything related to the table
        table.close();
        assertEquals( externalAllocation, memoryTracker.estimatedHeapMemory() );

        memoryTracker.releaseHeap( externalAllocation );
    }

    @Test
    void shouldNotUseMoreMemoryThanLinkedHashMap()
    {
        var random = new Random( 1337L );

        var lhm = new LinkedHashMap<>();
        var lhmMemoryTracker = new ScopedMemoryTracker( EmptyMemoryTracker.INSTANCE );
        // There a some slightly worse cases for estimation, e.g. 6000, 49000 or 98000 gives an underestimation of 3%,
        // then add a thousand and it changes to an overestimation to 6-8% after rehash.
        // This is because we only estimate the memory used by the chains for collisions, based on an assumption of the
        // average length of chains of 8. (See table below for more details)
        //     int nEntries = (int)(((double)(1 << 20)) * 0.75d);     // Just before rehash
        //     int nEntries = (int)(((double)(1 << 20)) * 0.75d) + 1; // Just after rehash

        int nEntries = 49000;
        for ( int i = 0; i < nEntries; i++ )
        {
            long keyLong = random.nextLong();
            var key = Values.longValue( keyLong );
            var value = Values.longValue( i );
            table.getIfAbsentPutWithMemoryTracker2( key, ( k, scopedMemoryTracker ) -> {
                scopedMemoryTracker.allocateHeap( k.estimatedHeapUsage() + value.estimatedHeapUsage() );
                return value;
            } );
            lhm.computeIfAbsent( key, k -> {
                lhmMemoryTracker.allocateHeap( key.estimatedHeapUsage() + value.estimatedHeapUsage() );
                return value;
            } );
        }

        // Validate size
        long actualTableSize = meter.measureDeep( table ) - meter.measureDeep( memoryTracker );
        long actualLhmSize = meter.measureDeep( lhm );

        //System.out.println( String.format( "HeapTrackingOrderedMap used %s bytes (estimation %s)", actualTableSize, memoryTracker.estimatedHeapMemory() ) );
        //System.out.println( String.format( "LinkedHashMap used %s bytes (estimation %s)", actualLhmSize, lhmMemoryTracker.estimatedHeapMemory() ) );

        assertTrue( actualTableSize <= actualLhmSize, "Used more memory than a LinkedHashMap." );
        assertTrue( Math.abs( actualTableSize - memoryTracker.estimatedHeapMemory() ) < actualTableSize * 0.1,
                String.format( "Mis-estimation of %s%% exceeds 10%%. Actual heap usage=%s. Estimated heap usage=%s.",
                        Math.round( (((double) Math.abs( actualTableSize - memoryTracker.estimatedHeapMemory() )) / ((double) actualTableSize)) * 100.0d ),
                        actualTableSize, memoryTracker.estimatedHeapMemory() ) );

        // Validate contents (assumes LinkedHashMap is correct)
        var tableIter = table.autoClosingEntryIterator();
        var lhmIter = lhm.entrySet().iterator();
        while ( lhmIter.hasNext() )
        {
            assertTrue( tableIter.hasNext() );
            assertEquals( lhmIter.next(), tableIter.next() );
        }
        assertFalse( tableIter.hasNext() );

        // Exhausting the iterator should close the map
    }

    //------------------------------------------------------------------------------------------
    // Assuming average length of chains of 4 key-value pairs:
    //    6000: HeapTrackingOrderedMap used  468696 bytes (estimation  440056 is 6% under)
    //    7000: HeapTrackingOrderedMap used  571528 bytes (estimation  576104 is 1% over)
    //   49000: HeapTrackingOrderedMap used 3672248 bytes (estimation 3442520 is 6% under)
    //   50000: HeapTrackingOrderedMap used 4126160 bytes (estimation 4219984 is 2% over)
    //   98000: HeapTrackingOrderedMap used 7350936 bytes (estimation 6884872 is 6% under)
    //   99000: HeapTrackingOrderedMap used 8163280 bytes (estimation 8357696 is 2% over)
    //
    // Assuming average length of chains of 8 key-value pairs:
    //    6000: HeapTrackingOrderedMap used  468696 bytes (estimation  454488 is 3% under) colliding buckets =  1405
    //    7000: HeapTrackingOrderedMap used  571528 bytes (estimation  605544 is 6% over)  colliding buckets =  1158
    //   49000: HeapTrackingOrderedMap used 3672248 bytes (estimation 3558040 is 3% under) colliding buckets = 11237
    //   50000: HeapTrackingOrderedMap used 4126160 bytes (estimation 4450416 is 8% over)  colliding buckets =  7416
    //   98000: HeapTrackingOrderedMap used 7350936 bytes (estimation 7115304 is 3% under) colliding buckets = 22698
    //   99000: HeapTrackingOrderedMap used 8163280 bytes (estimation 8817088 is 8% over)  colliding buckets = 14538
    //
    // (INITIAL_CHUNK_SIZE = 256, MAX_CHUNK_SIZE = 8192)

    //--------------------------------------------------------------------------------------------
    // Collisions for different types of value sequences:
    // This is the result of an interaction between the LongValue hash code and the UnifiedMap hash function (index())
    // LongValue hash code is computed by (value ^ (value >>> 32))
    // (i is the sequence from 0 until nEntries)
    // values            nEntries Collisions
    // random(seed=1337) 98000    22698
    // random(seed=42)   98000    22595
    // i                 98000        0
    // i*2               98000    32464
    // i*3               98000    19663
    // i*4               98000    32768
    // i*8               98000        0
    // i*16              98000        0
    // i*32              98000        0

    //--------------------------------------------------------------------------------------------
    // Chain lengths just before rehash:
    // random(seed=1337) 49000 entries gives 11237 colliding buckets (23%)
    //  chain length 4 count: 8608
    //  chain length 8 count: 2547
    //  chain length 12 count: 80
    //  chain length 16 count: 2
    //  chain length 20 count: 0
    //
    // random(seed=1337) 98000 entries gives 22698 colliding buckets (23%)
    //  chain length 4 count: 17502
    //  chain length 8 count: 5059
    //  chain length 12 count: 135
    //  chain length 16 count: 2
    //  chain length 20 count: 0
    //
    // random(seed=1337) 786432 entries gives 181837 colliding buckets (23%) (nEntries = (int)(((double)(1 << 20)) * 0.75d))
    //  chain length 4 count: 139601
    //  chain length 8 count: 41200
    //  chain length 12 count: 1020
    //  chain length 16 count: 16
    //  chain length 20 count: 0
    //
    // Chains lengths just after rehash:
    // random(seed=1337) 50000 entries gives 7416 colliding buckets (15%)
    //  chain length 4 count: 6520
    //  chain length 8 count: 886
    //  chain length 12 count: 10
    //  chain length 16 count: 0
    //
    // random(seed=1337) 786433 entries gives 115236 colliding buckets (15%) (nEntries = (int)(((double)(1 << 20)) * 0.75d) + 1)
    //  chain length 4 count: 101463
    //  chain length 8 count: 13671
    //  chain length 12 count: 101
    //  chain length 16 count: 1
    //  chain length 20 count: 0
    //
    // Using this method in HeapTrackingUnifiedMap:
    //    public int[] getCollidingBucketStatistics()
    //    {
    //        int[] histogram = new int[33];
    //        for (int i = 0; i < this.table.length; i += 2)
    //        {
    //            if (this.table[i] == CHAINED_KEY)
    //            {
    //                Object[] chain = (Object[]) this.table[i+1];
    //                int chainLength = chain.length;
    //                if (chainLength <= 32)
    //                {
    //                    histogram[chainLength]++;
    //                }
    //            }
    //        }
    //        return histogram;
    //    }
    //
    // And print with:
    // int collidingBuckets = table.getCollidingBuckets();
    // System.out.println( String.format( "// %s entries gives %s colliding buckets (%s%%)", nEntries, collidingBuckets,
    //     Math.round(100.0d * ((double)collidingBuckets) / ((double)nEntries)) ) );
    // int[] histogram = table.getCollidingBucketStatistics();
    // for ( int i = 4; i < histogram.length; i += 4 )
    // {
    //     System.out.println( String.format( "//  chain length %s count: %s", i, histogram[i] ) );
    // }

}
