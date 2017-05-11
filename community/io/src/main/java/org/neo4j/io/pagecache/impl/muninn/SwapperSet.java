/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.io.pagecache.impl.muninn;

import java.util.Arrays;
import java.util.function.Consumer;
import java.util.function.IntPredicate;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.io.pagecache.PageSwapper;

final class SwapperSet
{
    // The sentinel is used to reserve swapper id 0 as a special value.
    private static final Allocation SENTINEL = new Allocation( 0, null );
    // The tombstone is used as a marker to reserve allocation entries that have been freed, but not yet vacuumed.
    // An allocation cannot be reused until it has been vacuumed.
    private static final Allocation TOMBSTONE = new Allocation( 0, null );
    private static final int MAX_SWAPPER_ID = Short.MAX_VALUE;
    private volatile Allocation[] allocations = new Allocation[] { SENTINEL };
    private final PrimitiveIntSet free = Primitive.intSet();
    private final Object vacuumLock = new Object();
    private int freeCounter; // Used in `free`; Guarded by `this`

    public static final class Allocation
    {
        public final int id;
        public final PageSwapper swapper;

        private Allocation( int id, PageSwapper swapper )
        {
            this.id = id;
            this.swapper = swapper;
        }
    }

    public Allocation getAllocation( int id )
    {
        checkId( id );
        Allocation allocation = allocations[id];
        if ( allocation == null || allocation == TOMBSTONE )
        {
            return null;
        }
        return allocation;
    }

    private void checkId( int id )
    {
        if ( id == 0 )
        {
            throw new IllegalArgumentException( "0 is an invalid swapper id" );
        }
    }

    public synchronized int allocate( PageSwapper swapper )
    {
        Allocation[] allocations = this.allocations;

        // First look for an available freed slot.
        synchronized ( free )
        {
            if ( !free.isEmpty() )
            {
                int id = free.iterator().next();
                free.remove( id );
                allocations[id] = new Allocation( id, swapper );
                this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
                return id;
            }
        }

        // No free slot was found above, so we extend the array to make room for a new slot.
        int id = allocations.length;
        if ( id + 1 > MAX_SWAPPER_ID )
        {
            throw new IllegalStateException( "All swapper ids are allocated: " + MAX_SWAPPER_ID );
        }
        allocations = Arrays.copyOf( allocations, id + 1 );
        allocations[id] = new Allocation( id, swapper );
        this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
        return id;
    }

    public synchronized boolean free( int id )
    {
        checkId( id );
        Allocation[] allocations = this.allocations;
        Allocation current = allocations[id];
        if ( current == null || current == TOMBSTONE )
        {
            throw new IllegalStateException(
                    "PageSwapper allocation id " + id + " is currently not allocated. Likely a double free bug." );
        }
        allocations[id] = TOMBSTONE;
        this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
        freeCounter++;
        if ( freeCounter == 20 )
        {
            freeCounter = 0;
            return true;
        }
        return false;
    }

    public void vacuum( Consumer<IntPredicate> evictAllLoadedPagesCallback )
    {
        // We do this complicated locking to avoid blocking allocate() and free() as much as possible, while still only
        // allow a single thread to do vacuum at a time, and at the same time have consistent locking around the
        // set of free ids.
        synchronized ( vacuumLock )
        {
            // Collect currently free ids.
            PrimitiveIntSet freeIds = Primitive.intSet();
            Allocation[] allocations = this.allocations;
            for ( int id = 0; id < allocations.length; id++ )
            {
                Allocation allocation = allocations[id];
                if ( allocation == TOMBSTONE )
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
                    allocations[freeId] = null;
                }
                this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
            }
            synchronized ( free )
            {
                free.addAll( freeIds.iterator() );
            }
        }
    }
}
