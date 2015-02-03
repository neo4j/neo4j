/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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
import java.util.Map;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntObjectMap;
import org.neo4j.function.primitive.PrimitiveIntPredicate;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.unsafe.impl.batchimport.Utils.CompareType;
import org.neo4j.unsafe.impl.batchimport.cache.IntArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.LongBitsManipulator;
import org.neo4j.unsafe.impl.batchimport.cache.MemoryStatsVisitor;
import org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory;
import org.neo4j.unsafe.impl.batchimport.cache.idmapping.IdMapper;
import org.neo4j.unsafe.impl.batchimport.input.Group;

import static java.lang.Math.max;

import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;
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

    // Encoded values added in #put, in the order in which they are put. Indexes in the array are the actual node ids,
    // values are the encoded versions of the input ids.
    private final LongArray dataCache;

    // Ordering information about values in dataCache; the ordering of values in dataCache remains unchanged.
    // in prepare() this array is populated and changed along with how dataCache items "move around" so that
    // they end up sorted. Again, dataCache remains unchanged, only the ordering information is kept here.
    // Each index in trackerCache points to a dataCache index, where the value in dataCache contains the
    // encoded input id, used to match against the input id that is looked up during binary search.
    private final IntArray trackerCache;
    private final Encoder encoder;
    private final Radix radix;
    private final int processorsForSorting;
    private final LongArray collisionCache;
    private final List<Object> collisionValues = new ArrayList<>();
    private boolean readyForUse;
    private long[][] sortBuckets;
    private long size;
    private final List<IdGroup> idGroups = new ArrayList<>();

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
    public long get( Object inputId, Group group )
    {
        assert readyForUse;
        return binarySearch( inputId, group );
    }

    @Override
    public void put( Object inputId, long id, Group group )
    {
        // Check if we're now venturing into a new group. If so then end the previous group.
        int groupId = group.id();
        boolean newGroup = groupId >= idGroups.size();
        if ( newGroup )
        {
            assert groupId == idGroups.size() :
                "Nodes for any specific group must be added in sequence before adding nodes for any other group";
            endPreviousGroup();
        }

        // Encode and add the input id
        long code = encoder.encode( inputId );
        dataCache.set( id, code );
        radix.registerRadixOf( code );
        size++;

        // Create the new group
        if ( newGroup )
        {
            idGroups.add( new IdGroup( group, dataCache.highestSetIndex() ) );
        }
    }

    private void endPreviousGroup()
    {
        if ( !idGroups.isEmpty() )
        {
            idGroups.get( idGroups.size() - 1 ).setHighDataIndex( dataCache.highestSetIndex() );
        }
    }

    @Override
    public boolean needsPreparation()
    {
        return true;
    }

    @Override
    public void prepare( ResourceIterable<Object> ids )
    {
        endPreviousGroup();
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

    private long binarySearch( Object inputId, PrimitiveIntPredicate inGroup )
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

        long returnVal = binarySearch( x, inputId, low, high, inGroup );
        if ( returnVal == -1 )
        {
            low = 0;
            high = trackerCache.size() - 1;
            returnVal = binarySearch( x, inputId, low, high, inGroup );
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
        // This is currently the only way of discovering duplicate input ids, checked per group.
        PrimitiveIntObjectMap<Map<Object,Long>> collidedIds = Primitive.intObjectMap();

        for ( long i = 0; ids.hasNext(); i++ )
        {
            Object id = ids.next();
            long value = dataCache.get( i );
            if ( isCollision( value ) )
            {
                // Get hold of the group and duplicate detector for that group
                IdGroup group = groupOf( i );
                Map<Object,Long> idsForGroup = collidedIds.get( group.id() );
                if ( idsForGroup == null )
                {
                    collidedIds.put( group.id(), idsForGroup = new HashMap<>() );
                }

                // Check for duplicates in this group
                Long existingI = idsForGroup.put( id, i );
                if ( existingI != null )
                {
                    throw new IllegalStateException( "Id '" + id + "' is defined more than once in " +
                            group + ", at least at " + group.translate( existingI.longValue() ) + " and " +
                            group.translate( i ) );
                }

                // Store this collision input id for matching later in get()
                long val = encoder.encode( id );
                assert val == clearCollision( value );
                int collisionIndex = collisionValues.size();
                collisionValues.add( id );
                collisionCache.set( collisionIndex, i );
            }
        }
    }

    private IdGroup groupOf( long dataIndex )
    {
        for ( IdGroup idGroup : idGroups )
        {
            if ( idGroup.covers( dataIndex ) )
            {
                return idGroup;
            }
        }
        throw new IllegalArgumentException( "Strange, index " + dataIndex + " isn't included in a group" );
    }

    private long binarySearch( long x, Object inputId, long low, long high, PrimitiveIntPredicate inGroup )
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
                if ( isCollision( midValue ) )
                {
                    return findFromCollisions( mid, inputId, inGroup );
                }
                return inGroup.accept( groupOf( index ).id() ) ? index : -1;
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

    private long findFromCollisions( long index, Object inputId, PrimitiveIntPredicate inGroup )
    {
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

        // Find, hopefully one, match given the inputId and group matcher. This is the first step
        // where the DETECTOR is used.
        long result = findFromCollisions( fromIndex, toIndex, inGroup, inputId, CollisionHandler.DETECTOR );
        if ( result == CollisionHandler.COLLISION_MARK )
        {
            // If multiple matches were found we go and do the same find once more but using a different
            // handler. A handler that gathers information about the collision, information useful to the caller
            // and, in extension, the user. This information gathering is done as a second step to avoid
            // having to instantiate unnecessary state objects up-front if only a single match was found,
            // which is by far the most common case.
            CollisionHandler.Detective detective = new CollisionHandler.Detective( inputId );
            findFromCollisions( fromIndex, toIndex, inGroup, inputId, detective );
            throw detective.exception();
        }

        return result;
    }

    private long findFromCollisions( long fromIndex, long toIndex, PrimitiveIntPredicate inGroup, Object inputId,
            CollisionHandler resolver )
    {
        long found = -1;
        for ( long index = fromIndex; index <= toIndex; index++ )
        {
            int dataIndex = trackerCache.get( index );
            IdGroup group = groupOf( dataIndex );
            if ( inGroup.accept( group.id() ) )
            {
                // If we have more than Integer.MAX_VALUE collisions then I'd be darned.
                int collisionIndex = safeCastLongToInt( findIndex( collisionCache, dataIndex ) );
                Object value = collisionValues.get( collisionIndex );
                if ( inputId.equals( value ) )
                {
                    long foundIndex = trackerCache.get( index );
                    found = resolver.handle( found, foundIndex, group );
                    // continue checking so that we can throw the exception above, so that the lookup
                    // happens in a deterministic fashion.
                }
            }
        }
        return found;
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
        dataCache.visit( visitor );
        trackerCache.visit( visitor );
        collisionCache.visit( visitor );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + encoder + "," + radix + "]";
    }
}
