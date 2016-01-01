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

import org.neo4j.helpers.Format;

import static java.lang.String.format;

/**
 * Factory of {@link LongArray} instances.
 */
public interface LongArrayFactory
{
    LongArray newLongArray( long length );

    /**
     * @param chunkSize the size of each array (number of items). Where new chunks are added when needed.
     * @param initialValue initial value to set all default values to, {@link LongArray#setAll(long)}.
     */
    LongArray newDynamicLongArray( long chunkSize );

    public static final LongArrayFactory HEAP = new LongArrayFactory()
    {
        @Override
        public LongArray newLongArray( long length )
        {
            return new HeapLongArray( length );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize )
        {
            return new DynamicLongArray( this, chunkSize );
        }
    };

    public static final LongArrayFactory OFF_HEAP = new LongArrayFactory()
    {
        @Override
        public LongArray newLongArray( long length )
        {
            return new OffHeapLongArray( length );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize )
        {
            return new DynamicLongArray( this, chunkSize );
        }
    };

    public static class AutoLongArrayFactory implements LongArrayFactory
    {
        private final AvailableMemoryCalculator calculator;
        private final long margin;

        public AutoLongArrayFactory( AvailableMemoryCalculator calculator, long margin )
        {
            this.calculator = calculator;
            this.margin = margin;
        }

        @Override
        public LongArray newLongArray( long length )
        {
            long bytesRequired = length * 8;

            // Try to fit it in heap
            long freeHeap = calculator.availableHeapMemory() - margin;
            if ( bytesRequired < Integer.MAX_VALUE && bytesRequired < freeHeap )
            {
                return HEAP.newLongArray( length );
            }

            // Otherwise if there's room outside heap
            long freeOffHeap = calculator.availableOffHeapMemory() - margin;
            if ( bytesRequired < freeOffHeap )
            {
                return OFF_HEAP.newLongArray( length );
            }

            // If there's room for it, off-heap and on-heap collectively, then allocate a dynamic
            // array (which can have parts on- and parts off-heap).
            if ( bytesRequired < (freeHeap + freeOffHeap) )
            {
                // TODO Ideally return a chunk size that is roughly divisible by both on-heap and off-heap
                // free memory size. But for now just take the requested length / 10
                return newDynamicLongArray( length / 10 );
            }

            throw new IllegalArgumentException( format( "Neither enough free heap (%d), nor off-heap (%d) space " +
                    "for allocating %s", freeHeap, freeOffHeap, Format.bytes( bytesRequired ) ) );
        }

        @Override
        public LongArray newDynamicLongArray( long chunkSize )
        {
            return new DynamicLongArray( this, chunkSize );
        }
    }

    public static final LongArrayFactory AUTO = new AutoLongArrayFactory(
            AvailableMemoryCalculator.RUNTIME, 300*1024*1024 );
}
