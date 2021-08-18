/*
 * Copyright (c) "Neo4j"
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
package org.neo4j.internal.id.indexed;

import org.eclipse.collections.api.list.primitive.LongList;

import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.io.pagecache.context.CursorContext;

import static org.neo4j.internal.id.indexed.FreeIdScanner.MAX_SLOT_SIZE;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_ID;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * A cache of IDs that are available for allocation from {@link IdGenerator#nextId(CursorContext)} and similar methods.
 * Available IDs are cached in their respective slots, based on the number of consecutive IDs they provide.
 */
class IdCache
{
    private final int[] slotSizes;
    private final ConcurrentLongQueue[] queues;
    private final AtomicInteger size = new AtomicInteger();
    private final int singleIdSlotIndex;

    IdCache( IdSlotDistribution.Slot... slots )
    {
        this.queues = new ConcurrentLongQueue[slots.length];
        this.slotSizes = new int[slots.length];
        for ( int slotIndex = 0; slotIndex < slots.length; slotIndex++ )
        {
            int slotSize = slotSizes[slotIndex] = slots[slotIndex].slotSize();
            int capacity = slots[slotIndex].capacity();
            checkArgument( slotSize <= MAX_SLOT_SIZE, "Max slot size is %d", MAX_SLOT_SIZE );
            checkArgument( slotIndex == 0 || slotSize > slotSizes[slotIndex - 1], "Slot sizes should be provided ordered from smaller to bigger" );
            queues[slotIndex] = new SpmcLongQueue( capacity );
        }
        singleIdSlotIndex = findSingleSlotIndex( slotSizes );
    }

    private static int findSingleSlotIndex( int[] slotSizes )
    {
        for ( int i = 0; i < slotSizes.length; i++ )
        {
            if ( slotSizes[i] == 1 )
            {
                return i;
            }
        }
        throw new IllegalArgumentException( "Must have a slot for single IDs" );
    }

    void offer( PendingIdQueue pendingItemsToCache, IndexedIdGenerator.Monitor monitor )
    {
        for ( int i = 0; i < slotSizes.length; i++ )
        {
            int slotIndex = i;
            LongList source = pendingItemsToCache.queues[slotIndex];
            ConcurrentLongQueue target = queues[slotIndex];
            source.forEach( id ->
            {
                if ( !target.offer( id ) )
                {
                    throw new IllegalStateException( "This really should not happen, we knew the max available space there were for caching ids" +
                            " and now the cache claims to have less than that?" );
                }
                size.incrementAndGet();
                monitor.cached( id, slotSizes[slotIndex] );
            } );
        }
    }

    long takeOrDefault( long defaultValue )
    {
        long id = queues[singleIdSlotIndex].takeOrDefault( defaultValue );
        if ( id != defaultValue )
        {
            size.decrementAndGet();
        }
        return id;
    }

    long takeOrDefault( long defaultValue, int numberOfIds, IdRangeConsumer wasteNotifier )
    {
        long id = defaultValue;
        for ( int slotIndex = lowestSlotIndexCapableOf( numberOfIds ); id == defaultValue && slotIndex < slotSizes.length; slotIndex++ )
        {
            id = queues[slotIndex].takeOrDefault( defaultValue );
            if ( id != NO_ID && slotSizes[slotIndex] != numberOfIds )
            {
                int waste = slotSizes[slotIndex] - numberOfIds;
                // We allocated an ID from a slot that was larger than was requested, leaving some of these IDs marked as reserved,
                // but no longer cached. If we do nothing then these additional IDs will remain unusable until restart.
                // Tell the ID scanner about the remainder so that it can pick it up the next time it does scan work.
                wasteNotifier.accept( id + numberOfIds, waste );
            }
        }
        if ( id != defaultValue )
        {
            this.size.decrementAndGet();
        }
        return id;
    }

    int availableSpaceById()
    {
        int space = 0;
        for ( int i = 0; i < slotSizes.length; i++ )
        {
            space += (queues[i].capacity() - queues[i].size()) * slotSizes[i];
        }
        return space;
    }

    int[] slotSizes()
    {
        return slotSizes;
    }

    IdSlotDistribution.Slot[] slotsByAvailableSpace()
    {
        IdSlotDistribution.Slot[] slots = new IdSlotDistribution.Slot[slotSizes.length];
        for ( int slotIndex = 0; slotIndex < slotSizes.length; slotIndex++ )
        {
            ConcurrentLongQueue queue = queues[slotIndex];
            slots[slotIndex] = new IdSlotDistribution.Slot( queue.capacity() - queue.size(), slotSizes[slotIndex] );
        }
        return slots;
    }

    void drain( IdRangeConsumer consumer )
    {
        for ( int i = 0; i < queues.length; i++ )
        {
            ConcurrentLongQueue queue = queues[i];
            int slotSize = slotSizes[i];
            long id;
            while ( (id = queue.takeOrDefault( -1 )) != -1 )
            {
                consumer.accept( id, slotSize );
                size.decrementAndGet();
            }
        }
    }

    int size()
    {
        return size.get();
    }

    boolean isFull()
    {
        for ( ConcurrentLongQueue queue : queues )
        {
            if ( queue.size() < queue.capacity() )
            {
                return false;
            }
        }
        return true;
    }

    private int lowestSlotIndexCapableOf( int numberOfIds )
    {
        for ( int slotIndex = 0; slotIndex < slotSizes.length; slotIndex++ )
        {
            if ( slotSizes[slotIndex] >= numberOfIds )
            {
                return slotIndex;
            }
        }
        throw new IllegalArgumentException( "Slot size " + numberOfIds + " too large" );
    }

    @Override
    public String toString()
    {
        StringBuilder builder = new StringBuilder( "IdCache{size:" + size + ", " );
        for ( int i = 0; i < slotSizes.length; i++ )
        {
            builder.append( slotSizes[i] ).append( ":" ).append( queues[i].size() ).append( ", " );
        }
        return builder.append( "}" ).toString();
    }

    interface IdRangeConsumer
    {
        void accept( long id, int size );
    }
}
