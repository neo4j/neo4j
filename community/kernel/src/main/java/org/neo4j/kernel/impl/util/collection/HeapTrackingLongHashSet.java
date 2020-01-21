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

import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;

import org.neo4j.memory.MemoryTracker;

import static java.util.Objects.requireNonNull;
import static org.neo4j.memory.HeapEstimator.ARRAY_HEADER_BYTES;
import static org.neo4j.memory.HeapEstimator.alignObjectSize;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

@SuppressWarnings( "ExternalizableWithoutPublicNoArgConstructor" )
class HeapTrackingLongHashSet extends LongHashSet implements AutoCloseable
{
    private static final long SHALLOW_SIZE = shallowSizeOfInstance( HeapTrackingLongHashSet.class );

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
            MethodHandles.Lookup lookup = MethodHandles.privateLookupIn( LongHashSet.class, MethodHandles.lookup() );
            DEFAULT_INITIAL_CAPACITY = (int) lookup.findStaticGetter( LongHashSet.class, "DEFAULT_INITIAL_CAPACITY", int.class ).invoke();
            OCCUPIED_WITH_DATA = lookup.findVarHandle( LongHashSet.class, "occupiedWithData", int.class );
            OCCUPIED_WITH_SENTINELS = lookup.findVarHandle( LongHashSet.class, "occupiedWithSentinels", int.class );
        }
        catch ( Throwable e )
        {
            throw new LinkageError( "Unable to get VarHandle to LongHashSet internals", e );
        }
    }

    private final MemoryTracker memoryTracker;
    private int trackedCapacity;
    private boolean copyOnWrite;

    static HeapTrackingLongHashSet createLongHashSet( MemoryTracker memoryTracker )
    {
        memoryTracker.allocateHeap( SHALLOW_SIZE + arrayHeapSize( DEFAULT_INITIAL_CAPACITY ) );
        return new HeapTrackingLongHashSet( memoryTracker, DEFAULT_INITIAL_CAPACITY );
    }

    private HeapTrackingLongHashSet( MemoryTracker memoryTracker, int trackedCapacity )
    {
        this.memoryTracker = requireNonNull( memoryTracker );
        this.trackedCapacity = trackedCapacity;
    }

    @Override
    public boolean add( long element )
    {
        maybeCopy();
        maybeRehash();
        return super.add( element );
    }

    @Override
    public boolean remove( long value )
    {
        maybeCopy();
        return super.remove( value );
    }

    @Override
    public void compact()
    {
        // Compact unconditionally rehashes
        rehash( smallestPowerOfTwoGreaterThan( size() ) );
        super.compact();
    }

    @Override
    public LongSet freeze()
    {
        if ( size() > 1 )
        {
            copyOnWrite = true;
        }
        return super.freeze();
    }

    @Override
    public void close()
    {
        memoryTracker.releaseHeap( arrayHeapSize( trackedCapacity ) + SHALLOW_SIZE );
    }

    private void maybeCopy()
    {
        if ( copyOnWrite )
        {
            memoryTracker.allocateHeap( arrayHeapSize( trackedCapacity ) );
            copyOnWrite = false;
        }
    }

    private void maybeRehash()
    {
        // The backing arrays only shrink when compact() is called so we only track growth here
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
        long newBytes = arrayHeapSize( newCapacity );
        long oldBytes = arrayHeapSize( trackedCapacity );
        memoryTracker.allocateHeap( newBytes ); // throws if quota reached
        memoryTracker.releaseHeap( oldBytes );
        trackedCapacity = newCapacity;
    }

    private int maxOccupiedWithData()
    {
        return trackedCapacity >> 1;
    }

    private static int smallestPowerOfTwoGreaterThan( int n )
    {
        return n > 1 ? Integer.highestOneBit( n - 1 ) << 1 : 1;
    }

    private static long arrayHeapSize( int arrayLength )
    {
        return alignObjectSize( ARRAY_HEADER_BYTES + arrayLength * Long.BYTES );
    }
}
