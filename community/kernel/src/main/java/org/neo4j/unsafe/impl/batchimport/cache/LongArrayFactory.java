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

import org.neo4j.helpers.Format;

import static java.lang.String.format;

/**
 * Factory of {@link LongArray} instances.
 */
public interface LongArrayFactory
{
    LongArray newLongArray( long length );

    /**
     * @param chunkSize the size of each array. Where new chunks are added when needed.
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

    public static final LongArrayFactory AUTO = new LongArrayFactory()
    {
        @Override
        public LongArray newLongArray( long length )
        {
            long bytesRequired = length * 8;

            System.gc();
            int margin = 500*1024*1024;
            long freeHeap = Runtime.getRuntime().freeMemory() - margin; /*leave 500M or so*/

            // Try to fit it in heap
            if ( bytesRequired < Integer.MAX_VALUE && bytesRequired < freeHeap )
            {
                return HEAP.newLongArray( length );
            }

            // Otherwise if there's room outside heap
            com.sun.management.OperatingSystemMXBean bean =
                    (com.sun.management.OperatingSystemMXBean)
                    java.lang.management.ManagementFactory.getOperatingSystemMXBean();
            long osMemory = bean.getTotalPhysicalMemorySize();
            long freeOffHeap = osMemory-Runtime.getRuntime().maxMemory() - margin;
            if ( length < freeOffHeap )
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
    };
}
