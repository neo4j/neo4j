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
package org.neo4j.internal.id.indexed;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;

import static org.neo4j.internal.id.indexed.IdRange.IdState;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.FREE;

/**
 * Responsible for starting and managing scans of a {@link GBPTree}, populating a cache with free ids that gets discovered in the scan.
 * Ids which are placed into cache are also marked as reserved, i.e. not free anymore. This way those ids that were found in one "round"
 * of a scan will not be found in upcoming rounds and reserving ids becomes more of a batch operation.
 */
class FreeIdScanner implements Closeable
{
    private static final IdRangeKey LOW_KEY = new IdRangeKey( 0 );
    private static final IdRangeKey HIGH_KEY = new IdRangeKey( Long.MAX_VALUE );

    private final int idsPerEntry;
    private final GBPTree<IdRangeKey, IdRange> tree;
    private final ConcurrentLongQueue cache;
    private final AtomicBoolean atLeastOneIdOnFreelist;
    private final Supplier<ReuseMarker> reuseMarkerSupplier;
    private final long generation;
    private final ScanLock lock;
    private volatile Seeker<IdRangeKey, IdRange> scanner;
    private final long[] pendingItemsToCache;
    private int pendingItemsToCacheCursor;
    private int nextPosInRange;

    FreeIdScanner( int idsPerEntry, GBPTree<IdRangeKey, IdRange> tree, ConcurrentLongQueue cache, AtomicBoolean atLeastOneIdOnFreelist,
            Supplier<ReuseMarker> reuseMarkerSupplier, long generation, boolean strictlyPrioritizeFreelistOverHighId )
    {
        this.idsPerEntry = idsPerEntry;
        this.tree = tree;
        this.cache = cache;
        this.atLeastOneIdOnFreelist = atLeastOneIdOnFreelist;
        this.reuseMarkerSupplier = reuseMarkerSupplier;
        this.pendingItemsToCache = new long[cache.capacity()];
        this.generation = generation;
        this.lock = strictlyPrioritizeFreelistOverHighId ? ScanLock.lockyAndPessimistic() : ScanLock.lockFreeAndOptimistic();
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     */
    boolean tryLoadFreeIdsIntoCache()
    {
        if ( scanner == null && !atLeastOneIdOnFreelist.get() )
        {
            // If no scan is in progress (SeekCursor now sitting and waiting at some leaf in the free-list)
            // and if we have no reason to expect finding any free id from a scan then don't do it.
            return false;
        }

        if ( lock.tryLock() )
        {
            try
            {
                // A new scan is commencing, clear the queue to put ids in
                pendingItemsToCacheCursor = 0;
                // Get a snapshot of the size before we start. At the end of the scan the actual space available to fill with IDs
                // may be even bigger, but not smaller. This is important because we discover IDs, mark them as non-reusable
                // and then place them in the cache so IDs that wouldn't fit in the cache would need to be marked as reusable again,
                // which would be somewhat annoying.
                int maxItemsToCache = cache.capacity() - cache.size();

                // Find items to cache
                if ( maxItemsToCache > 0 && findSomeIdsToCache( maxItemsToCache ) )
                {
                    // Get a writer and mark the found ids as reserved
                    markIdsAsReserved();

                    // Place them in the cache so that allocation requests can see them
                    placeIdsInCache();
                    return true;
                }
            }
            catch ( IOException e )
            {
                throw new UncheckedIOException( e );
            }
            finally
            {
                lock.unlock();
            }
        }
        return false;
    }

    void clearCache()
    {
        lock.lock();
        try
        {
            // Since placing an id into the cache marks it as reserved, here when taking the ids out from the cache revert that by marking them as free again
            try ( ReuseMarker marker = reuseMarkerSupplier.get() )
            {
                long id;
                do
                {
                    id = cache.takeOrDefault( -1 );
                    if ( id != -1 )
                    {
                        marker.markFree( id );
                    }
                }
                while ( id != -1 );
            }
            atLeastOneIdOnFreelist.set( true );
        }
        finally
        {
            lock.unlock();
        }
    }

    private void placeIdsInCache()
    {
        for ( int i = 0; i < pendingItemsToCacheCursor; i++ )
        {
            if ( !cache.offer( pendingItemsToCache[i] ) )
            {
                throw new IllegalStateException( "This really should not happen, we knew the max available space there were for caching ids" +
                        " and now the cache claims to have less than that?" );
            }
        }
    }

    private void markIdsAsReserved()
    {
        try ( ReuseMarker marker = reuseMarkerSupplier.get() )
        {
            for ( int i = 0; i < pendingItemsToCacheCursor; i++ )
            {
                marker.markReserved( pendingItemsToCache[i] );
            }
        }
    }

    private boolean findSomeIdsToCache( int maxItemsToCache ) throws IOException
    {
        boolean startedNow = false;
        if ( scanner == null )
        {
            scanner = tree.seek( LOW_KEY, HIGH_KEY );
            startedNow = true;
        }

        // Continue scanning a bit forward...
        boolean seekerExhausted = false;

        // First check if the previous scan was aborted in the middle of the entry
        if ( !startedNow && nextPosInRange > 0 && nextPosInRange < idsPerEntry )
        {
            queueIdsFromTreeItem( scanner.key(), scanner.value(), nextPosInRange, maxItemsToCache );
        }

        // Then continue looking at additional entries
        while ( pendingItemsToCacheCursor < maxItemsToCache )
        {
            if ( !scanner.next() )
            {
                seekerExhausted = true;
                break;
            }
            queueIdsFromTreeItem( scanner.key(), scanner.value(), 0, maxItemsToCache );
        }
        boolean somethingWasCached = pendingItemsToCacheCursor > 0;
        if ( seekerExhausted )
        {
            scanner.close();
            scanner = null;
            if ( !somethingWasCached && startedNow )
            {
                // chill a bit until at least one id gets freed
                atLeastOneIdOnFreelist.set( false );
            }
        }
        return somethingWasCached;
    }

    private void queueIdsFromTreeItem( IdRangeKey key, IdRange range, int startPosInRange, int maxItemsToCache )
    {
        final long baseId = key.getIdRangeIdx() * idsPerEntry;
        final boolean differentGeneration = generation != range.getGeneration();

        for ( int i = startPosInRange; i < idsPerEntry && pendingItemsToCacheCursor < maxItemsToCache; i++ )
        {
            nextPosInRange = i + 1;
            final IdState state = range.getState( i );
            if ( state == FREE || (differentGeneration && state == DELETED) )
            {
                pendingItemsToCache[pendingItemsToCacheCursor++] = baseId + i;
            }
        }
    }

    @Override
    public void close() throws IOException
    {
        if ( scanner != null )
        {
            scanner.close();
        }
    }
}
