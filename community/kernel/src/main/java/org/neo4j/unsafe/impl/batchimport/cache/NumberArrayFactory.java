/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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

import static java.lang.String.format;

import static org.neo4j.helpers.Format.bytes;
import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;

/**
 * Factory of {@link LongArray} and {@link IntArray} instances. Users can select in which type of memory
 * the arrays will be placed, either in {@link #HEAP} or {@link #OFF_HEAP}, or even {@link #AUTO} which
 * will have each instance placed where it fits best, favoring off-heap.
 */
public interface NumberArrayFactory
{
    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link IntArray}.
     */
    IntArray newIntArray( long length, int defaultValue );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link IntArray}.
     */
    IntArray newDynamicIntArray( long chunkSize, int defaultValue );

    /**
     * @param length size of the array.
     * @param defaultValue value which will represent unset values.
     * @return a fixed size {@link LongArray}.
     */
    LongArray newLongArray( long length, long defaultValue );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param defaultValue value which will represent unset values.
     * @return dynamically growing {@link LongArray}.
     */
    LongArray newDynamicLongArray( long chunkSize, long defaultValue );

    /**
     * Implements the dynamic array methods, because they are the same in most implementations.
     */
    public static abstract class Adapter implements NumberArrayFactory
    {
        @Override
        public IntArray newDynamicIntArray( long chunkSize, int defaultValue )
        {
            return new DynamicIntArray( this, chunkSize, defaultValue );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize, long defaultValue )
        {
            return new DynamicLongArray( this, chunkSize, defaultValue );
        }
    }

    /**
     * Puts arrays inside the heap.
     */
    public static final NumberArrayFactory HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue )
        {
            return new HeapIntArray( safeCastLongToInt( length ), defaultValue );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue )
        {
            return new HeapLongArray( safeCastLongToInt( length ), defaultValue );
        }
    };

    /**
     * Puts arrays off-heap, using unsafe calls.
     */
    public static final NumberArrayFactory OFF_HEAP = new Adapter()
    {
        @Override
        public IntArray newIntArray( long length, int defaultValue )
        {
            return new OffHeapIntArray( length, defaultValue );
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue )
        {
            return new OffHeapLongArray( length, defaultValue );
        }
    };

    /**
     * Looks at available memory and decides where the requested array fits best. Tries to allocate the whole
     * array off-heap, then inside heap. If all else fails a dynamic array is returned with a smaller chunk size
     * so that collectively the whole array will fit in memory available on the machine.
     */
    public static class Auto extends Adapter
    {
        private final AvailableMemoryCalculator calculator;
        private final long margin;

        public Auto( AvailableMemoryCalculator calculator, long margin )
        {
            this.calculator = calculator;
            this.margin = margin;
        }

        @Override
        public LongArray newLongArray( long length, long defaultValue )
        {
            return mostAppropriateFactory( length, 8 ).newLongArray( length, defaultValue );
        }

        @Override
        public IntArray newIntArray( long length, int defaultValue )
        {
            return mostAppropriateFactory( length, 4 ).newIntArray( length, defaultValue );
        }

        private NumberArrayFactory mostAppropriateFactory( long length, int i )
        {
            long bytesRequired = length * 8;

            // Try to fit it outside heap
            long freeOffHeap = calculator.availableOffHeapMemory() - margin;
            if ( bytesRequired < freeOffHeap )
            {
                return OFF_HEAP;
            }

            // Otherwise, try to fit it in heap
            long freeHeap = calculator.availableHeapMemory() - margin;
            if ( bytesRequired < Integer.MAX_VALUE && bytesRequired < freeHeap )
            {
                try
                {
                    return HEAP;
                }
                catch ( OutOfMemoryError e )
                {   // It seems there wasn't room after all...
                }
            }

            // If there's room for it, off-heap and on-heap collectively, then allocate a dynamic
            // array (which can have parts on- and parts off-heap).
            if ( bytesRequired < (freeHeap + freeOffHeap) )
            {
                // There is! (at the moment at least). OK, so for the sake of conformity return a factory
                // that creates dynamic arrays even when requesting static arrays. Just because we can.
                return CHUNKED_STATIC;
            }

            throw new IllegalArgumentException( format( "Neither enough free heap (%d), nor off-heap (%d) space " +
                    "for allocating %s", freeHeap, freeOffHeap, bytes( bytesRequired ) ) );
        }
    }

    /**
     * Used as part of the fallback strategy for {@link Auto}.
     */
    public static final NumberArrayFactory CHUNKED_STATIC = new Adapter()
    {
        @Override
        public LongArray newLongArray( long length, long defaultValue )
        {
            return newDynamicLongArray( fractionOf( length ), defaultValue );
        }

        @Override
        public IntArray newIntArray( long length, int defaultValue )
        {
            return newDynamicIntArray( fractionOf( length ), defaultValue );
        }

        private long fractionOf( long length )
        {
            return length / 10;
        }

        @Override
        public IntArray newDynamicIntArray( long chunkSize, int defaultValue )
        {
            return new DynamicIntArray( AUTO, chunkSize, defaultValue );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize, long defaultValue )
        {
            return new DynamicLongArray( AUTO, chunkSize, defaultValue );
        }
    };

    /**
     * {@link Auto} factory which uses JVM stats for gathering information about available memory.
     */
    public static final NumberArrayFactory AUTO = new Auto(
            AvailableMemoryCalculator.RUNTIME, 300*1024*1024 );
}
