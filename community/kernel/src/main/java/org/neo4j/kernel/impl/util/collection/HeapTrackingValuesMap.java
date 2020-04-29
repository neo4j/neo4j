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

import org.neo4j.collection.trackable.HeapTrackingLongObjectHashMap;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

@SuppressWarnings( "ExternalizableWithoutPublicNoArgConstructor" )
public class HeapTrackingValuesMap extends HeapTrackingLongObjectHashMap<Value>
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingValuesMap.class );
    private long valuesHeapSize;

    static HeapTrackingValuesMap createValuesMap( MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + arraysHeapSize( DEFAULT_INITIAL_CAPACITY ) );
        return new HeapTrackingValuesMap( memoryTracker, DEFAULT_INITIAL_CAPACITY );
    }

    private HeapTrackingValuesMap( MemoryTracker memoryTracker, int trackedCapacity )
    {
        super( memoryTracker, trackedCapacity );
    }

    @Override
    public Value put( long key, Value value )
    {
        allocate( value );
        Value old = super.put( key, value );
        if ( old != null )
        {
            release( old );
        }
        return old;
    }

    @Override
    public Value remove( long key )
    {
        Value remove = super.remove( key );
        if ( remove != null )
        {
            release( remove );
        }
        return remove;
    }

    @Override
    public void clear()
    {
        super.clear();
        memoryTracker.releaseHeap( valuesHeapSize );
        valuesHeapSize = 0;
    }

    private void allocate( Value value )
    {
        long valueHeapSize = value.estimatedHeapUsage();
        valuesHeapSize += valueHeapSize;
        memoryTracker.allocateHeap( valueHeapSize );
    }

    private void release( Value old )
    {
        long oldHeapSize = old.estimatedHeapUsage();
        valuesHeapSize -= oldHeapSize;
        memoryTracker.releaseHeap( oldHeapSize );
    }
}
