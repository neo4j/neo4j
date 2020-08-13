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

import org.eclipse.collections.impl.map.mutable.primitive.IntObjectHashMap;

import org.neo4j.memory.MemoryTracker;

import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

@SuppressWarnings( "ExternalizableWithoutPublicNoArgConstructor" )
class HeapTrackingIntObjectHashMap<V> extends IntObjectHashMap<V> implements AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingLongObjectHashMap.class );
    private static final int DEFAULT_INITIAL_CAPACITY = 16;

    private final MemoryTracker memoryTracker;
    private int trackedCapacity;

    static <V> HeapTrackingIntObjectHashMap<V> createIntObjectHashMap( MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + arraysHeapSize( DEFAULT_INITIAL_CAPACITY ) );
        return new HeapTrackingIntObjectHashMap<>( memoryTracker, DEFAULT_INITIAL_CAPACITY );
    }

    private HeapTrackingIntObjectHashMap( MemoryTracker memoryTracker, int trackedCapacity )
    {
        this.memoryTracker = requireNonNull( memoryTracker );
        this.trackedCapacity = trackedCapacity;
    }

    @Override
    protected void allocateTable( int sizeToAllocate )
    {
        if ( memoryTracker != null )
        {
            memoryTracker.allocateHeap( arraysHeapSize( sizeToAllocate ) );
            memoryTracker.releaseHeap( arraysHeapSize( trackedCapacity ) );
            trackedCapacity = sizeToAllocate;
        }
        super.allocateTable( sizeToAllocate );
    }

    @Override
    public void close()
    {
        memoryTracker.releaseHeap( arraysHeapSize( trackedCapacity ) + SHALLOW_SIZE );
    }

    static long arraysHeapSize( int arrayLength )
    {
        long keyArray = alignObjectSize( ARRAY_HEADER_BYTES + (long) arrayLength * Integer.BYTES );
        long valueArray = alignObjectSize( ARRAY_HEADER_BYTES + (long) arrayLength * OBJECT_REFERENCE_BYTES );
        return keyArray + valueArray;
    }
}
