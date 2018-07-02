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

import org.neo4j.function.ThrowingSupplier;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.id.IdGenerator.ReuseMarker;

import static org.neo4j.internal.id.indexed.IdRange.IdState;
import static org.neo4j.internal.id.indexed.IdRange.IdState.DELETED;
import static org.neo4j.internal.id.indexed.IdRange.IdState.FREE;
import static org.neo4j.internal.id.indexed.IdRange.IdState.RESERVED;

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
    private final ThrowingSupplier<ReuseMarker, IOException> reuseMarkerSupplier;
    private final long generation;
    private final ScanLock lock;
    private volatile Seeker<IdRangeKey, IdRange> scanner;
    private final long[] pendingItemsToCache;
    private int pendingItemsToCacheCursor;
    private int maxItemsToCache;
    private int nextPosInRange;

    FreeIdScanner( int idsPerEntry, GBPTree<IdRangeKey, IdRange> tree, ConcurrentLongQueue cache, AtomicBoolean atLeastOneIdOnFreelist,
            ThrowingSupplier<ReuseMarker,IOException> reuseMarkerSupplier, long generation, boolean strictlyPrioritizeFreelistOverHighId )
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
     * @return {@code true} if there's a chance calling {@link #doSomeScanning()} may find any free ids.
     * It may have been that a previous scan was performed where no ids were found and no ids have been freed since.
     */
    boolean scanMightFindFreeIds()
    {
        // If a scan is already in progress (SeekCursor now sitting and waiting at some leaf in the free-list)
        // Or if we know that there have been at least one freed id that we haven't picked up yet.
        return scanner != null || atLeastOneIdOnFreelist.get();
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     */
    void doSomeScanning()
    {
        if ( lock.lock() )
        {
            try
            {
                // A new scan is commencing, clear the queue to put ids in
                pendingItemsToCacheCursor = 0;
                // Get a snapshot of the size before we start. At the end of the scan the actual space available to fill with IDs
                // may be even bigger, but not smaller. This is important because we discover IDs, mark them as non-reusable
                // and then place them in the cache so IDs that wouldn't fit in the cache would need to be marked as reusable again,
                // which would be somewhat annoying.
                maxItemsToCache = cache.capacity() - cache.size();

                // Find items to cache
                if ( maxItemsToCache > 0 && findSomeIdsToCache() )
                {
                    // Get a writer and mark the found ids as reserved
                    markIdsAsReserved();

                    // Place them in the cache so that allocation requests can see them
                    placeIdsInCache();
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

    private void markIdsAsReserved() throws IOException
    {
        try ( ReuseMarker marker = reuseMarkerSupplier.get() )
        {
            for ( int i = 0; i < pendingItemsToCacheCursor; i++ )
            {
                // TODO we could potentially make this a little more batchy where we could do one merge thing per tree entry
                //  instead of one per entry. Or we could have something like a writable seeker that could update these along the way.
                marker.markReserved( pendingItemsToCache[i] );
            }
        }
    }

    private boolean findSomeIdsToCache() throws IOException
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
            queueIdsFromTreeItem( scanner.key(), scanner.value(), nextPosInRange );
        }

        // Then continue looking at additional entries
        while ( pendingItemsToCacheCursor < pendingItemsToCache.length )
        {
            if ( !scanner.next() )
            {
                seekerExhausted = true;
                break;
            }
            queueIdsFromTreeItem( scanner.key(), scanner.value(), 0 );
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

    private void queueIdsFromTreeItem( IdRangeKey key, IdRange range, int startPosInRange )
    {
        final long baseId = key.getIdRangeIdx() * idsPerEntry;
        final boolean differentGeneration = generation != range.getGeneration();

        for ( int i = startPosInRange; i < range.size() && pendingItemsToCacheCursor < maxItemsToCache; i++ )
        {
            nextPosInRange = i + 1;
            final IdState state = range.getState( i );
            if ( state == FREE || (differentGeneration && (state == DELETED || state == RESERVED)) )
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
