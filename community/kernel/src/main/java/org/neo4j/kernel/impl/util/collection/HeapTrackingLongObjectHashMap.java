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

import org.eclipse.collections.api.block.function.Function;
import org.eclipse.collections.api.block.function.Function0;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.impl.map.mutable.primitive.LongObjectHashMap;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.neo4j.memory.MemoryTracker;

import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.OBJECT_REFERENCE_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

@SuppressWarnings( "ExternalizableWithoutPublicNoArgConstructor" )
class HeapTrackingLongObjectHashMap<V> extends LongObjectHashMap<V> implements AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingLongObjectHashMap.class );
    private static final int DEFAULT_INITIAL_CAPACITY;
    private static final VarHandle OCCUPIED_WITH_DATA;
    private static final VarHandle OCCUPIED_WITH_SENTINELS;

    private int occupiedWithData()
    {
        return (int) OCCUPIED_WITH_DATA.get( this );
    }

    private int occupiedWithSentinels()
    {
        return (int) OCCUPIED_WITH_SENTINELS.get( this );
    }

    static
    {
        try
        {
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn( LongObjectHashMap.class, MethodHandles.lookup() );
            DEFAULT_INITIAL_CAPACITY = (int) lookup.findStaticGetter( LongObjectHashMap.class, "DEFAULT_INITIAL_CAPACITY", int.class ).invoke();
            OCCUPIED_WITH_DATA = lookup.findVarHandle( LongObjectHashMap.class, "occupiedWithData", int.class );
            OCCUPIED_WITH_SENTINELS = lookup.findVarHandle( LongObjectHashMap.class, "occupiedWithSentinels", int.class );
        }
        catch ( Throwable e )
        {
            throw new LinkageError( "Unable to get VarHandle to LongObjectHashMap internals", e );
        }
    }

    private final MemoryTracker memoryTracker;
    private int trackedCapacity;

    static <V> HeapTrackingLongObjectHashMap<V> createLongObjectHashMap( MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + arraysHeapSize( DEFAULT_INITIAL_CAPACITY << 1 ) );
        return new HeapTrackingLongObjectHashMap<>( memoryTracker, DEFAULT_INITIAL_CAPACITY << 1 );
    }

    private HeapTrackingLongObjectHashMap( MemoryTracker memoryTracker, int trackedCapacity )
    {
        this.memoryTracker = requireNonNull( memoryTracker );
        this.trackedCapacity = trackedCapacity;
    }

    @Override
    public void close()
    {
        memoryTracker.releaseHeap( arraysHeapSize( trackedCapacity ) + SHALLOW_SIZE );
    }

    @Override
    public V put( long key, V value )
    {
        maybeRehash();
        return super.put( key, value );
    }

    @Override
    public V getIfAbsentPut( long key, Function0<? extends V> function )
    {
        maybeRehash();
        return super.getIfAbsentPut( key, function );
    }

    @Override
    public <P> V getIfAbsentPutWith( long key, Function<? super P,? extends V> function, P parameter )
    {
        maybeRehash();
        return super.getIfAbsentPutWith( key, function, parameter );
    }

    @Override
    public V getIfAbsentPutWithKey( long key, LongToObjectFunction<? extends V> function )
    {
        maybeRehash();
        return super.getIfAbsentPutWithKey( key, function );
    }

    @Override
    public void compact()
    {
        rehash( smallestPowerOfTwoGreaterThan( size() ) );
        super.compact();
    }

    private void maybeRehash()
    {
        // The backing arrays only shrink when compact() is called, so we only track growth here
        if ( occupiedWithData() + occupiedWithSentinels() >= maxOccupiedWithData() )
        {
            rehashAndGrow();
        }
    }

    private void rehashAndGrow()
    {
        int max = maxOccupiedWithData();
        int newCapacity = Math.max( max, smallestPowerOfTwoGreaterThan( (occupiedWithData() + 1) << 1 ) );
        if ( occupiedWithSentinels() > 0 && (max >> 1) + (max >> 2) < occupiedWithData() )
        {
            newCapacity <<= 1;
        }
        rehash( newCapacity );
    }

    private void rehash( int newCapacity )
    {
        long newBytes = arraysHeapSize( newCapacity );
        long oldBytes = arraysHeapSize( trackedCapacity );
        memoryTracker.allocateHeap( newBytes ); // throws if quota reached
        memoryTracker.releaseHeap( oldBytes );
        trackedCapacity = newCapacity;
    }

    private int maxOccupiedWithData()
    {
        // need at least one free slot for open addressing
        return Math.min( trackedCapacity - 1, trackedCapacity >> 1 );
    }

    private static long arraysHeapSize( int arrayLength )
    {
        long keyArray = alignObjectSize( ARRAY_HEADER_BYTES + arrayLength * Long.BYTES );
        long valueArray = alignObjectSize( ARRAY_HEADER_BYTES + arrayLength * OBJECT_REFERENCE_BYTES );
        return keyArray + valueArray;
    }

    private static int smallestPowerOfTwoGreaterThan( int n )
    {
        return n > 1 ? Integer.highestOneBit( n - 1 ) << 1 : 1;
    }
}
