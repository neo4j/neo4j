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
import java.util.Iterator;
import java.util.List;

import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;

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
    private final IntArray trackerCache;
    private final LongArray dataCache;
    private Encoder encoder;
    private Radix radix;
    private final int processorsForSorting;
    private final LongArray collisionCache;
    private final IntArray collisionStringIndex;
    private final StringBuilder collisionStrings;
    private boolean readyForUse;
    private long[][] sortBuckets;
    private long size;

    /* OK, so this is a bit weird, but I made a decision to have auto-detection of whether or not the
     * supplied Strings are actually longs or strings, right in here. It's done like that because users
     * are probably oblivious to that fact and they perhaps don't care. We care because of the performance
     * opportunity to skip string encoding, and instead basically just parse the string as a long.
     *   This could of course be taken one step further, to skip the round-trip to String and go for
     * letting the input parse the id as a long right away. Although that would require the auto detection
     * to sit somewhere else. Look at this as a TODO to move this auto-detection closer to the input source. */
    private List<String> testPhaseStrings = new ArrayList<>();
    private final int testPhaseLimit = 200;
    private boolean encodingDecisionMade;

    public StringIdMapper( LongArrayFactory cacheFactory )
    {
        this( cacheFactory, CACHE_CHUNK_SIZE, Runtime.getRuntime().availableProcessors() - 1 );
    }

    public StringIdMapper( LongArrayFactory cacheFactory, int chunkSize, int processorsForSorting )
    {
        this.processorsForSorting = processorsForSorting;
        this.dataCache = newLongArray( cacheFactory, chunkSize );
        this.trackerCache = newIntArray( cacheFactory, chunkSize );
        this.encoder = new LongEncoder();
        this.radix = new Radix.Long();
        this.collisionCache = newLongArray( cacheFactory, chunkSize );
        this.collisionStringIndex = newIntArray( cacheFactory, chunkSize );
        this.collisionStrings = new StringBuilder();
    }

    private static IntArray newIntArray( LongArrayFactory cacheFactory, int chunkSize )
    {
        return new IntArray( cacheFactory, chunkSize, -1 );
    }

    private static LongArray newLongArray( LongArrayFactory cacheFactory, int chunkSize )
    {
        return cacheFactory.newDynamicLongArray( chunkSize, -1 );
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
        checkForStringValue( (String) stringValue );
        long code = encoder.encode( (String) stringValue );
        dataCache.set( id, code );
        radix.registerRadixOf( code );
        size++;
    }

    private void checkForStringValue( String s )
    {
        if ( encodingDecisionMade )
        {
            return;
        }

        try
        {
            Long.parseLong( s );
            // alright, it still looks like we're dealing with long values
            if ( testPhaseStrings.size() > testPhaseLimit )
            {
                // confirm long value and disable string check
                encodingDecisionMade = true;
            }
            else
            {
                testPhaseStrings.add( s );
            }
        }
        catch ( NumberFormatException e )
        {
            // so we're NOT dealing with long values, switch to string encoding
            encodingDecisionMade = true;
            radix = new Radix.String();
            encoder = new StringEncoder( 2 );

            // re-encode the previous test strings as strings
            if ( !testPhaseStrings.isEmpty() )
            {
                for ( int index = 0; index < testPhaseStrings.size(); index++ )
                {
                    long code = encoder.encode( testPhaseStrings.get( index ) );
                    dataCache.set( index, code );
                }
            }
        }
        finally
        {
            if ( encodingDecisionMade )
            {   // no point keeping this around
                testPhaseStrings = null;
            }
        }
    }

    @Override
    public boolean needsPreparation()
    {
        return true;
    }

    @Override
    public void prepare( Iterable<Object> ids )
    {
        synchronized ( this )
        {
            sortBuckets = new ParallelSort( radix, dataCache, trackerCache, processorsForSorting ).run();
        }
        if ( detectAndMarkCollisions() > 0 )
        {
            buildCollisionInfo( ids.iterator() );
        }
        readyForUse = true;
    }

    private int radixOf( long value )
    {
        return radix.calculator().radixOf( value );
    }

    private long binarySearch( String strValue )
    {
        long low = 0;
        long highestSetTrackerIndex = trackerCache.highestSetIndex();
        long high = highestSetTrackerIndex;
        long x = encoder.encode( strValue );
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
                            radixOf( dataCache.get( trackerCache.get( i ) ) ) + ":" +
                            radixOf( dataCache.get( trackerCache.get( i + 1 ) ) ) );
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
                long val = encoder.encode( id );
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
        assert val == encoder.encode( inString );
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
