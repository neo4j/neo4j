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
package org.neo4j.unsafe.impl.batchimport.cache.idmapping.string;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Set;

import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

import static java.lang.Math.max;

import static org.neo4j.unsafe.impl.batchimport.Utils.unsignedCompare;

/**
 * Maps arbitrary values to long ids. The values can be {@link #put(Object, long) added} in any order,
 * but {@link #needsPreparation() needs} {@link #prepare() preparation} in order to {@link #get(Object) get}
 * ids back later.
 *
 * In the {@link #prepare() preparation phase} the added entries are sorted according to a number representation
 * of each input value and {@link #get(Object)} does simple binary search to find the correct one.
 *
 * The implementation is space-efficient, much more so than using, say, a {@link HashMap}.
 */
public class EncodingIdMapper implements IdMapper
{
    // Bit in encoded String --> long values that marks that the particular item has a collision,
    // i.e. that there's at least one other string that encodes into the same long value.
    // This bit is the least significant in the most significant byte of the encoded values,
    // where the 7 most significant bits in that byte denotes length of original string.
    // See StringEncoder.
    private static LongBitsManipulator COLLISION_BIT = new LongBitsManipulator( 56, 1 );
    public static int CACHE_CHUNK_SIZE = 1_000_000; // 8MB a piece
    private final IntArray trackerCache;
    private final LongArray dataCache;
    private final Encoder encoder;
    private final Radix radix;
    private final int processorsForSorting;
    private final LongArray collisionCache;
    private final IntArray collisionValuesIndex;
    private final List<Object> collisionValues = new ArrayList<>();
    private boolean readyForUse;
    private long[][] sortBuckets;
    private long size;

    public EncodingIdMapper( NumberArrayFactory cacheFactory, Encoder encoder, Radix radix )
    {
        this( cacheFactory, encoder, radix, CACHE_CHUNK_SIZE, Runtime.getRuntime().availableProcessors() - 1 );
    }

    public EncodingIdMapper( NumberArrayFactory cacheFactory, Encoder encoder, Radix radix,
            int chunkSize, int processorsForSorting )
    {
        this.processorsForSorting = max( processorsForSorting, 1 );
        this.dataCache = newLongArray( cacheFactory, chunkSize );
        this.trackerCache = newIntArray( cacheFactory, chunkSize );
        this.encoder = encoder;
        this.radix = radix;
        this.collisionCache = newLongArray( cacheFactory, chunkSize );
        this.collisionValuesIndex = newIntArray( cacheFactory, chunkSize );
    }

    private static IntArray newIntArray( NumberArrayFactory cacheFactory, int chunkSize )
    {
        return cacheFactory.newDynamicIntArray( chunkSize, -1 );
    }

    private static LongArray newLongArray( NumberArrayFactory cacheFactory, int chunkSize )
    {
        return cacheFactory.newDynamicLongArray( chunkSize, -1 );
    }

    @Override
    public long get( Object inputId )
    {
        assert readyForUse;
        return binarySearch( inputId );
    }

    @Override
    public void put( Object inputId, long id )
    {
        long code = encoder.encode( inputId );
        dataCache.set( id, code );
        radix.registerRadixOf( code );
        size++;
    }

    @Override
    public boolean needsPreparation()
    {
        return true;
    }

    @Override
    public void prepare( ResourceIterable<Object> ids )
    {
        synchronized ( this )
        {
            // Synchronized since there's this concern that a couple of other threads are changing trackerCache
            // and it's nice to go through a memory barrier afterwards to ensure this CPU see correct data.
            sortBuckets = new ParallelSort( radix, dataCache, trackerCache, processorsForSorting ).run();
        }
        if ( detectAndMarkCollisions() > 0 )
        {
            try ( ResourceIterator<Object> idIterator = ids.iterator() )
            {
                buildCollisionInfo( idIterator );
            }
        }
        readyForUse = true;
    }

    private int radixOf( long value )
    {
        return radix.calculator().radixOf( value );
    }

    private long binarySearch( Object inputId )
    {
        long low = 0;
        long highestSetTrackerIndex = trackerCache.highestSetIndex();
        long high = highestSetTrackerIndex;
        long x = encoder.encode( inputId );
        int rIndex = radixOf( x );
        for ( int k = 0; k < sortBuckets.length; k++ )
        {
            if ( rIndex <= sortBuckets[k][0] )//bucketRange[k] > rIndex )
            {
                low = sortBuckets[k][1];
                high = (k == sortBuckets.length - 1) ? trackerCache.size() - 1 : sortBuckets[k + 1][1];
                break;
            }
        }

        long returnVal = binarySearch( x, inputId, false, low, high );
        if ( returnVal == -1 )
        {
            low = 0;
            high = trackerCache.size() - 1;
            returnVal = binarySearch( x, inputId, false, low, high );
        }
        return returnVal;
    }

    private static long setCollision( long value )
    {
        return COLLISION_BIT.set( value, 1, 1 );
    }

    static long clearCollision( long value )
    {
        return COLLISION_BIT.clear( value, 1, false );
    }

    private static boolean isCollision( long value )
    {
        return COLLISION_BIT.get( value, 1 ) != 0;
    }

    private int detectAndMarkCollisions()
    {
        int numCollisions = 0;
        for ( int i = 0; i < trackerCache.size() - 1; i++ )
        {
            if ( compareDataCache( dataCache, trackerCache, i, i + 1, CompareType.GE ) )
            {
                if ( !compareDataCache( dataCache, trackerCache, i, i + 1, CompareType.EQ ) )
                {
                    throw new IllegalStateException( "Failure:[" + i + "] " +
                            Long.toHexString( dataCache.get( trackerCache.get( i ) ) ) + ":" +
                            Long.toHexString( dataCache.get( trackerCache.get( i + 1 ) ) ) + " | " +
                            radixOf( dataCache.get( trackerCache.get( i ) ) ) + ":" +
                            radixOf( dataCache.get( trackerCache.get( i + 1 ) ) ) );
                }

                if ( trackerCache.get( i ) > trackerCache.get( i + 1 ) )
                {   // swap
                    trackerCache.swap( i, i+1, 1 );
                }
                long value = dataCache.get( trackerCache.get( i ) );
                value = setCollision( value );
                dataCache.set( trackerCache.get( i ), value );
                value = dataCache.get( trackerCache.get( i + 1 ) );
                value = setCollision( value );
                dataCache.set( trackerCache.get( i + 1 ), value );
                numCollisions++;
            }
        }
        return numCollisions;
    }

    private void buildCollisionInfo( Iterator<Object> ids )
    {
        int collisionIndex = 0;
        Set<Object> collidedIds = new HashSet<>();
        for ( long i = 0; ids.hasNext(); i++ )
        {
            Object id = ids.next();
            long value = dataCache.get( i );
            if ( isCollision( value ) )
            {
                if ( !collidedIds.add( id ) )
                {
                    throw new IllegalStateException( "Duplicate input ids. '" +
                            id + "' existed in input more than once" );
                }

                long val = encoder.encode( id );
                assert val == clearCollision( value );
                int valueIndex = collisionValues.size();
                collisionValues.add( id );
                collisionCache.set( collisionIndex, i );
                collisionValuesIndex.set( collisionIndex, valueIndex );
                collisionIndex++;
            }
        }
    }

    private long binarySearch( long x, Object inputId, boolean trackerIndex, long low, long high )
    {
        while ( low <= high )
        {
            long mid = low + (high - low)/2;//(low + high) / 2;
            int index = trackerCache.get( mid );
            if ( index == -1 )
            {
                return -1;
            }
            long midValue = dataCache.get( index );
            if ( unsignedCompare( clearCollision( midValue ), x, CompareType.EQ ) )
            {
                if ( trackerIndex )
                {
                    //get the lowest tracking index
                    if ( isCollision( midValue ) )
                    {
                        while ( midValue == dataCache.get( trackerCache.get( mid - 1 ) ) )
                        {
                            mid -= 1;
                        }
                    }
                    return mid;
                }
                if ( isCollision( midValue ) )
                {
                    return findFromCollisions( mid, inputId );
                }
                return index;
            }
            else if ( unsignedCompare( clearCollision( midValue ), x, CompareType.LT ) )
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }
        return -1;
    }

    private long findIndex( LongArray array, long value )
    {
        // can't be done on unsorted data
        long low = 0 + 0;
        long high = size - 1;
        while ( low <= high )
        {
            long mid = (low + high) / 2;
            long midValue = array.get( mid );
            if ( unsignedCompare( midValue, value, CompareType.EQ ) )
            {
                return mid;
            }
            else if ( unsignedCompare( midValue, value, CompareType.LT ) )
            {
                low = mid + 1;
            }
            else
            {
                high = mid - 1;
            }
        }
        return -1;
    }

    private int findFromCollisions( long index, Object inputId )
    {
        if ( collisionValues.isEmpty() )
        {
            return -1;
        }

        long val = clearCollision( dataCache.get( trackerCache.get( index ) ) );
        assert val == encoder.encode( inputId );

        while ( index > 0 &&
                unsignedCompare( val, clearCollision( dataCache.get( trackerCache.get( index - 1 ) ) ), CompareType.EQ ) )
        {
            index--;
        }
        long fromIndex = index;
        while ( index < trackerCache.highestSetIndex() &&
                unsignedCompare( val, clearCollision( dataCache.get( trackerCache.get( index + 1 ) ) ), CompareType.EQ ) )
        {
            index++;
        }
        long toIndex = index;
        long[] collisionVals = new long[(int) (toIndex - fromIndex + 1)];
        for ( index = fromIndex; index <= toIndex; index++ )
        {
            collisionVals[(int) (index - fromIndex)] = findIndex( collisionCache, trackerCache.get( index ) );
        }

        for ( int i = 0; i < collisionVals.length; i++ )
        {
            int collisionIndex = collisionValuesIndex.get( collisionVals[i] );
            Object value = collisionValues.get( collisionIndex );
            if ( inputId.equals( value ) )
            {
                return trackerCache.get( fromIndex + i );
            }
        }
        return -1;
    }

    static boolean compareDataCache( LongArray dataCache, IntArray tracker, int a, int b, CompareType compareType )
    {
        int indexA = tracker.get( a );
        int indexB = tracker.get( b );
        if ( indexA == -1 || indexB == -1 )
        {
            return false;
        }

        return unsignedCompare(
                clearCollision( dataCache.get( indexA ) ),
                clearCollision( dataCache.get( indexB ) ),
                compareType );
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        dataCache.visitMemoryStats( visitor );
        trackerCache.visitMemoryStats( visitor );
        collisionCache.visitMemoryStats( visitor );
        collisionValuesIndex.visitMemoryStats( visitor );
    }
}
