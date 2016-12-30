/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import java.io.IOException;
import java.util.Arrays;
import java.util.function.IntConsumer;

import org.neo4j.collection.primitive.Primitive;
import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveIntSet;
import org.neo4j.io.pagecache.PageSwapper;

final class SwapperSet
{
    // The sentinel is used to reserve swapper id 0 as a special value.
    private static final Allocation SENTINEL = new Allocation( 0, 0, null );
    private volatile Allocation[] allocations = new Allocation[] { SENTINEL };
    private final PrimitiveIntSet free = Primitive.intSet();
    private final Object vacuumLock = new Object();

    private static final class Allocation
    {
        public final int id;
        public final int filePageSize;
        public final PageSwapper swapper;

        private Allocation( int id, int filePageSize, PageSwapper swapper )
        {
            this.id = id;
            this.filePageSize = filePageSize;
            this.swapper = swapper;
        }
    }

    public interface ApplyCall
    {
        void apply( PageSwapper swapper, int filePageSize ) throws IOException;
    }

    public PageSwapper getSwapper( int id )
    {
        checkId( id );
        return allocations[id].swapper;
    }

    public int getFilePageSize( int id )
    {
        checkId( id );
        return allocations[id].filePageSize;
    }

    public void apply( int id, ApplyCall call ) throws IOException
    {
        checkId( id );
        Allocation allocation = allocations[id];
        call.apply( allocation.swapper, allocation.filePageSize );
    }

    private void checkId( int id )
    {
        if ( id == 0 )
        {
            throw new IllegalArgumentException( "0 is an invalid swapper id" );
        }
    }

    public synchronized int allocate( PageSwapper swapper, int filePageSize )
    {
        Allocation[] allocations = this.allocations;

        // First look for an available slot.
        synchronized ( free )
        {
            for ( int i = 0; i < allocations.length; i++ )
            {
                if ( allocations[i] == null && !free.contains( i ) )
                {
                    // Found an available slot; there's no current allocation, and it is also not in the free set.
                    // We cannot reuse freed ids before they have been vacuumed. Vacuuming means that we ensure that
                    // there are no pages in the PageList that refers to these ids. The point is, that unmapping a file
                    // only ensures that all of its pages are flushed; not that they are evicted. Vacuuming is the
                    // eviction of the pages that are bound to the now unmapped file.
                    allocations[i] = new Allocation( i, filePageSize, swapper );
                    this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
                    return i;
                }
            }
        }

        // No free slot was found above, so we extend the array to make room for a new slot.
        int idx = allocations.length;
        allocations = Arrays.copyOf( allocations, idx + 1 );
        allocations[idx] = new Allocation( idx, filePageSize, swapper );
        this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
        return idx;
    }

    public synchronized void free( int id )
    {
        checkId( id );
        Allocation[] allocations = this.allocations;
        if ( allocations[id] == null )
        {
            throw new IllegalStateException(
                    "PageSwapper allocation id " + id + " is currently not allocated. Likely a double free bug." );
        }
        allocations[id] = null;
        synchronized ( free )
        {
            free.add( id );
        }
        this.allocations = allocations; // Volatile store synchronizes-with loads in getters.
    }

    public void vacuum( IntConsumer evictAllLoadedPagesCallback )
    {
        // We do this complicated locking to avoid blocking allocate() and free() as much as possible, while still only
        // allow a single thread to do vacuum at a time, and at the same time have consistent locking around the
        // set of free ids.
        synchronized ( vacuumLock )
        {
            // Collect currently free ids.
            int[] freeIds;
            synchronized ( free )
            {
                freeIds = new int[free.size()];
                PrimitiveIntIterator iterator = free.iterator();
                int index = 0;
                while ( iterator.hasNext() )
                {
                    freeIds[index] = iterator.next();
                    index++;
                }
            }

            // Evict all of them without holding up the lock on the free id set. This allows allocate() and free() to
            // proceed concurrently with our eviction. This is safe because we know that the ids we are evicting cannot
            // possibly be reused until we remove them from the free id set, which we won't do until after we've evicted
            // all of their loaded pages.
            for ( int freeId : freeIds )
            {
                evictAllLoadedPagesCallback.accept( freeId );
            }

            // Finally, all of the pages that remained in memory with an unmapped swapper id have been evicted. We can
            // now safely allow those ids to be reused. Note, however, that free() might have been called while we were
            // doing this, so we can't just free.clear() the set; no, we have to explicitly remove only those specific
            // ids whose pages we evicted.
            synchronized ( free )
            {
                for ( int freeId : freeIds )
                {
                    free.remove( freeId );
                }
            }
        }
    }
}
