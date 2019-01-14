/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.io.pagecache.PageSwapper;

import static org.neo4j.helpers.Numbers.safeCastIntToShort;

/**
 * The SwapperSet maintains the set of allocated {@link PageSwapper}s, and their mapping to swapper ids.
 * These swapper ids are a limited resource, so they must eventually be reused as files are mapped and unmapped.
 * Before a swapper id can be reused, we have to make sure that there are no pages in the page cache, that
 * are bound to the old swapper id. To ensure this, we have to periodically {@link MuninnPageCache#vacuum(SwapperSet)}
 * the page cache. The vacuum process will then fully evict all pages that are bound to a page swapper id that
 * was freed before the start of the vacuum process.
 */
final class SwapperSet
{
    // The sentinel is used to reserve swapper id 0 as a special value.
    private static final SwapperMapping SENTINEL = new SwapperMapping( 0, null );
    // The tombstone is used as a marker to reserve allocation entries that have been freed, but not yet vacuumed.
    // An allocation cannot be reused until it has been vacuumed.
    private static final SwapperMapping TOMBSTONE = new SwapperMapping( 0, null );
    private static final int MAX_SWAPPER_ID = (1 << 21) - 1;
    private volatile SwapperMapping[] swapperMappings = new SwapperMapping[] { SENTINEL };
    private final PrimitiveIntSet free = Primitive.intSet();
    private final Object vacuumLock = new Object();
    private int freeCounter; // Used in `free`; Guarded by `this`

    /**
     * The mapping entry between a {@link PageSwapper} and its swapper id.
     */
    static final class SwapperMapping
    {
        public final int id;
        public final PageSwapper swapper;

        private SwapperMapping( int id, PageSwapper swapper )
        {
            this.id = id;
            this.swapper = swapper;
        }
    }

    /**
     * Get the {@link SwapperMapping} for the given swapper id.
     */
    SwapperMapping getAllocation( int id )
    {
        checkId( id );
        SwapperMapping swapperMapping = swapperMappings[id];
        if ( swapperMapping == null || swapperMapping == TOMBSTONE )
        {
            return null;
        }
        return swapperMapping;
    }

    private void checkId( int id )
    {
        if ( id == 0 )
        {
            throw new IllegalArgumentException( "0 is an invalid swapper id" );
        }
    }

    /**
     * Allocate a new swapper id for the given {@link PageSwapper}.
     */
    synchronized int allocate( PageSwapper swapper )
    {
        SwapperMapping[] swapperMappings = this.swapperMappings;

        // First look for an available freed slot.
        synchronized ( free )
        {
            if ( !free.isEmpty() )
            {
                int id = free.iterator().next();
                free.remove( id );
                swapperMappings[id] = new SwapperMapping( id, swapper );
                this.swapperMappings = swapperMappings; // Volatile store synchronizes-with loads in getters.
                return id;
            }
        }

        // No free slot was found above, so we extend the array to make room for a new slot.
        int id = swapperMappings.length;
        if ( id + 1 > MAX_SWAPPER_ID )
        {
            throw new IllegalStateException( "All swapper ids are allocated: " + MAX_SWAPPER_ID );
        }
        swapperMappings = Arrays.copyOf( swapperMappings, id + 1 );
        swapperMappings[id] = new SwapperMapping( id, swapper );
        this.swapperMappings = swapperMappings; // Volatile store synchronizes-with loads in getters.
        return id;
    }

    /**
     * Free the given swapper id, and return {@code true} if it is time for a
     * {@link MuninnPageCache#vacuum(SwapperSet)}, otherwise it returns {@code false}.
     */
    synchronized boolean free( int id )
    {
        checkId( id );
        SwapperMapping[] swapperMappings = this.swapperMappings;
        SwapperMapping current = swapperMappings[id];
        if ( current == null || current == TOMBSTONE )
        {
            throw new IllegalStateException(
                    "PageSwapper allocation id " + id + " is currently not allocated. Likely a double free bug." );
        }
        swapperMappings[id] = TOMBSTONE;
        this.swapperMappings = swapperMappings; // Volatile store synchronizes-with loads in getters.
        freeCounter++;
        if ( freeCounter == 20 )
        {
            freeCounter = 0;
            return true;
        }
        return false;
    }

    /**
     * Collect all freed page swapper ids, and pass them to the given callback, after which the freed ids will be
     * eligible for reuse.
     * This is done with careful synchronisation such that allocating and freeing of ids is allowed to mostly proceed
     * concurrently.
     */
    void vacuum( Consumer<IntPredicate> evictAllLoadedPagesCallback )
    {
        // We do this complicated locking to avoid blocking allocate() and free() as much as possible, while still only
        // allow a single thread to do vacuum at a time, and at the same time have consistent locking around the
        // set of free ids.
        synchronized ( vacuumLock )
        {
            // Collect currently free ids.
            PrimitiveIntSet freeIds = Primitive.intSet();
            SwapperMapping[] swapperMappings = this.swapperMappings;
            for ( int id = 0; id < swapperMappings.length; id++ )
            {
                SwapperMapping swapperMapping = swapperMappings[id];
                if ( swapperMapping == TOMBSTONE )
                {
                    freeIds.add( id );
                }
            }

            // Evict all of them without holding up the lock on the free id set. This allows allocate() and free() to
            // proceed concurrently with our eviction. This is safe because we know that the ids we are evicting cannot
            // possibly be reused until we remove them from the free id set, which we won't do until after we've evicted
            // all of their loaded pages.
            evictAllLoadedPagesCallback.accept( freeIds );

            // Finally, all of the pages that remained in memory with an unmapped swapper id have been evicted. We can
            // now safely allow those ids to be reused. Note, however, that free() might have been called while we were
            // doing this, so we can't just free.clear() the set; no, we have to explicitly remove only those specific
            // ids whose pages we evicted.
            synchronized ( this )
            {
                PrimitiveIntIterator itr = freeIds.iterator();
                while ( itr.hasNext() )
                {
                    int freeId = itr.next();
                    swapperMappings[freeId] = null;
                }
                this.swapperMappings = swapperMappings; // Volatile store synchronizes-with loads in getters.
            }
            synchronized ( free )
            {
                free.addAll( freeIds.iterator() );
            }
        }
    }

    synchronized int countAvailableIds()
    {
        // the max id is one less than the allowed count, but we subtract one for the reserved id 0
        int available = MAX_SWAPPER_ID;
        available -= swapperMappings.length; // ids that are allocated are not available
        available += free.size(); // add back the ids that are free to be reused
        return available;
    }
}
