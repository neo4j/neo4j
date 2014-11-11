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

import java.util.HashMap;
import java.util.Iterator;

import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

import static java.lang.Math.pow;

import static org.neo4j.unsafe.impl.batchimport.Utils.unsignedCompare;

/**
 * Maps arbitrary strings to ids. The strings can be {@link #put(Object, long) added} in any order,
 * but {@link #needsPreparation() needs} {@link #prepare() preparation} in order to {@link #get(Object) get}
 * ids back later.
 *
 * In the {@link #prepare() preparation phase} the added entries are sorted according to a number representation
 * of each string and {@link #get(Object)} does simple binary search to find the correct one.
 *
 * The implementation is space-efficient, much more so than using, say, a {@link HashMap}.
 */
public class StringIdMapper implements IdMapper
{
    private static LongBitsManipulator COLLISION_BIT = new LongBitsManipulator( 62, 1, 1 );
    private static int CACHE_CHUNK_SIZE = 1_000_000; // 8MB a piece
    private static final int RADIX_BITS = 24;
    private static final int LENGTH_MASK = (int) (0xFE000000_00000000L >>> (64 - RADIX_BITS));
    private static final int HASHCODE_MASK = (int) (0x00FFFF00_00000000L >>> (64 - RADIX_BITS));

    private final IntArray trackerCache;
    private final LongArray dataCache;
    private final StringEncoder strEncoder;
    private final int processorsForSorting;

    private final LongArray collisionCache;
    private final IntArray collisionStringIndex;
    private final StringBuilder collisionStrings;

    private final int[] radixIndexCount = new int[(int) pow( 2, RADIX_BITS - 1 )];
    private boolean readyForUse;
    private long[][] sortBuckets;
    private long size;

    public StringIdMapper( LongArrayFactory cacheFactory )
    {
        this( cacheFactory, Runtime.getRuntime().availableProcessors() - 1 );
    }

    public StringIdMapper( LongArrayFactory cacheFactory, int processorsForSorting )
    {
        this.processorsForSorting = processorsForSorting;
        this.dataCache = newLongArray( cacheFactory );
        this.trackerCache = newIntArray( cacheFactory );
        this.strEncoder = new StringEncoder( 2 );
        this.collisionCache = newLongArray( cacheFactory );
        this.collisionStringIndex = newIntArray( cacheFactory );
        this.collisionStrings = new StringBuilder();
    }

    private static IntArray newIntArray( LongArrayFactory cacheFactory )
    {
        return new IntArray( cacheFactory, CACHE_CHUNK_SIZE, -1 );
    }

    private static LongArray newLongArray( LongArrayFactory cacheFactory )
    {
        return cacheFactory.newDynamicLongArray( CACHE_CHUNK_SIZE, -1 );
    }

    @Override
    public long get( Object stringValue )
    {
        assert readyForUse;
        return binarySearch( (String) stringValue );
    }

    @Override
    public void put( Object stringValue, long id )
    {
        // synchronize if/when node encoder stage gets multi threaded
        long code = strEncoder.encode( (String) stringValue );
        dataCache.set( id, code );
        int radix = radixOf( code );
        radixIndexCount[radix]++;
        size++;
    }

    static int radixOf( long val )
    {
        int index = (int) (val >>> (64 - RADIX_BITS));
        index = (((index & LENGTH_MASK) >>> 1) | (index & HASHCODE_MASK));
        return index;
    }

    @Override
    public boolean needsPreparation()
    {
        return true;
    }

    @Override
    public void prepare( Iterable<Object> ids )
    {
        sortBuckets = new ParallelSort( radixIndexCount, dataCache, trackerCache, processorsForSorting ).run();

        if ( detectAndMarkCollisions() > 0 )
        {
            buildCollisionInfo( ids.iterator() );
        }
        readyForUse = true;
    }

    private long binarySearch( String strValue )
    {
        long low = 0;
        long high = trackerCache.highestSetIndex();
        long x = strEncoder.encode( strValue );
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

        long returnVal = binarySearch( x, strValue, false, low, high );
        if ( returnVal == -1 )
        {
            low = 0;
            high = trackerCache.size() - 1;
            returnVal = binarySearch( x, strValue, false, low, high );
        }
        return returnVal;
    }

    private static long setCollision( long value )
    {
        return COLLISION_BIT.set( value, 1, 1 );
    }

    private static long clearCollision( long value )
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
        for ( int i = 0; i < trackerCache.size(); i++ )
        {
            if ( i < trackerCache.size() - 1
                    && compareDataCache( dataCache, trackerCache, i, i + 1, CompareType.GE ) )
            {
                if ( !compareDataCache( dataCache, trackerCache, i, i + 1, CompareType.EQ ) )
                {
                    throw new IllegalStateException( "Failure:[" + i + "] " +
                            Long.toHexString( dataCache.get( trackerCache.get( i ) ) ) + ":" +
                            Long.toHexString( dataCache.get( trackerCache.get( i + 1 ) ) ) + " | " +
                            radixOf( dataCache.get( trackerCache.get( i ) ) ) +
                            ":" + radixOf( dataCache.get( trackerCache.get( i + 1 ) ) ) );
                }

                if ( trackerCache.get( i ) > trackerCache.get( i + 1 ) )
                {   // swap
                    int temp = trackerCache.get( i );
                    trackerCache.set( i, trackerCache.get( i + 1 ) );
                    trackerCache.set( i + 1, temp );
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
        for ( long i = 0; ids.hasNext(); i++ )
        {
            String id = (String) ids.next();
            long value = dataCache.get( i );
            if ( isCollision( value ) )
            {
                long val = strEncoder.encode( id );
                assert val == clearCollision( value );
                int strIndex = collisionStrings.length();
                collisionStrings.append( id );
                collisionCache.set( collisionIndex, i );
                collisionStringIndex.set( collisionIndex, strIndex );
                collisionIndex++;
            }
        }
    }

    private long binarySearch( long x, String strValue, boolean trackerIndex, long low, long high )
    {
        while ( low <= high )
        {
            long mid = (low + high) / 2;
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
                    return findFromCollisions( mid, strValue );
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

    private int findFromCollisions( long strIndex, String inString )
    {
        long index = strIndex;
        long val = dataCache.get( trackerCache.get( index ) );
        assert val == strEncoder.encode( inString );
        while ( unsignedCompare( val, dataCache.get( trackerCache.get( index - 1 ) ), CompareType.EQ ) )
        {
            index--;
        }
        long fromIndex = index;
        while ( unsignedCompare( val, dataCache.get( trackerCache.get( index + 1 ) ), CompareType.EQ ) )
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
            int from = 0, to = 0;
            from = collisionStringIndex.get( collisionVals[i] );
            if ( collisionVals[i] == collisionStringIndex.highestSetIndex() )
            {
                to = collisionStrings.length();
            }
            else
            {
                to = collisionStringIndex.get( collisionVals[i] + 1 );
            }
            String str = collisionStrings.substring( from, to );
            if ( inString.equals( str ) )
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
        collisionStringIndex.visitMemoryStats( visitor );
    }
}
