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

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.id.indexed.IndexedIdGenerator.ReservedMarker;
import org.neo4j.io.pagecache.tracing.cursor.PageCursorTracer;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
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
    /**
     * Used as low bound of a new scan. Continuing a scan makes use of {@link #ongoingScanRangeIndex}.
     */
    private static final IdRangeKey LOW_KEY = new IdRangeKey( 0 );
    /**
     * Used as high bound for all scans.
     */
    private static final IdRangeKey HIGH_KEY = new IdRangeKey( Long.MAX_VALUE );

    private final int idsPerEntry;
    private final GBPTree<IdRangeKey, IdRange> tree;
    private final ConcurrentLongQueue cache;
    private final AtomicBoolean atLeastOneIdOnFreelist;
    private final MarkerProvider markerProvider;
    private final long generation;
    private final ScanLock lock;
    private final IndexedIdGenerator.Monitor monitor;
    /**
     * State for whether or not there's an ongoing scan, and if so where it should begin from. This is used in
     * {@link #findSomeIdsToCache(LinkedChunkLongArray, int, PageCursorTracer)} both to know where to initiate a scan from and to set it, if the cache got
     * full before scan completed, or set it to null of the scan ended. The actual {@link Seeker} itself is local to the scan method.
     */
    private Long ongoingScanRangeIndex;

    FreeIdScanner( int idsPerEntry, GBPTree<IdRangeKey,IdRange> tree, ConcurrentLongQueue cache, AtomicBoolean atLeastOneIdOnFreelist,
            MarkerProvider markerProvider, long generation, boolean strictlyPrioritizeFreelistOverHighId, IndexedIdGenerator.Monitor monitor )
    {
        this.idsPerEntry = idsPerEntry;
        this.tree = tree;
        this.cache = cache;
        this.atLeastOneIdOnFreelist = atLeastOneIdOnFreelist;
        this.markerProvider = markerProvider;
        this.generation = generation;
        this.lock = strictlyPrioritizeFreelistOverHighId ? ScanLock.lockyAndPessimistic() : ScanLock.lockFreeAndOptimistic();
        this.monitor = monitor;
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     */
    boolean tryLoadFreeIdsIntoCache( boolean awaitOngoing, PageCursorTracer cursorTracer )
    {
        if ( ongoingScanRangeIndex == null && !atLeastOneIdOnFreelist.get() )
        {
            // If no scan is in progress (SeekCursor now sitting and waiting at some leaf in the free-list)
            // and if we have no reason to expect finding any free id from a scan then don't do it.
            return false;
        }

        if ( scanLock( awaitOngoing ) )
        {
            try
            {
                // A new scan is commencing
                // Get a snapshot of the size before we start. At the end of the scan the actual space available to fill with IDs
                // may be even bigger, but not smaller. This is important because we discover IDs, mark them as non-reusable
                // and then place them in the cache so IDs that wouldn't fit in the cache would need to be marked as reusable again,
                // which would be somewhat annoying.
                int maxItemsToCache = cache.capacity() - cache.size();
                if ( maxItemsToCache > 0 )
                {
                    // Find items to cache
                    LinkedChunkLongArray pendingItemsToCache = new LinkedChunkLongArray( min( maxItemsToCache, max( 256, cache.capacity() / 10 ) ) );
                    if ( findSomeIdsToCache( pendingItemsToCache, maxItemsToCache, cursorTracer ) )
                    {
                        // Get a writer and mark the found ids as reserved
                        markIdsAsReserved( pendingItemsToCache, cursorTracer );

                        // Place them in the cache so that allocation requests can see them
                        placeIdsInCache( pendingItemsToCache );
                        return true;
                    }
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

    private boolean scanLock( boolean awaitOngoing )
    {
        if ( awaitOngoing )
        {
            lock.lock();
            return true;
        }
        return lock.tryLock();
    }

    void clearCache( PageCursorTracer cursorTracer )
    {
        lock.lock();
        try
        {
            // Restart scan from the beginning after cache is cleared
            ongoingScanRangeIndex = null;

            // Since placing an id into the cache marks it as reserved, here when taking the ids out from the cache revert that by marking them as free again
            try ( ReservedMarker marker = markerProvider.getMarker( cursorTracer ) )
            {
                long id;
                do
                {
                    id = cache.takeOrDefault( -1 );
                    if ( id != -1 )
                    {
                        marker.markUnreserved( id );
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

    private void placeIdsInCache( LinkedChunkLongArray pendingItemsToCache )
    {
        pendingItemsToCache.accept( id ->
        {
            if ( !cache.offer( id ) )
            {
                throw new IllegalStateException( "This really should not happen, we knew the max available space there were for caching ids" +
                        " and now the cache claims to have less than that?" );
            }
            monitor.cached( id );
        } );
    }

    private void markIdsAsReserved( LinkedChunkLongArray pendingItemsToCache, PageCursorTracer cursorTracer )
    {
        try ( ReservedMarker marker = markerProvider.getMarker( cursorTracer ) )
        {
            pendingItemsToCache.accept( marker::markReserved );
        }
    }

    private boolean findSomeIdsToCache( LinkedChunkLongArray pendingItemsToCache, int maxItemsToCache, PageCursorTracer cursorTracer ) throws IOException
    {
        boolean startedNow = ongoingScanRangeIndex == null;
        IdRangeKey from = ongoingScanRangeIndex == null ? LOW_KEY : new IdRangeKey( ongoingScanRangeIndex );
        boolean seekerExhausted = false;
        try ( Seeker<IdRangeKey,IdRange> scanner = tree.seek( from, HIGH_KEY, cursorTracer ) )
        {
            // Continue scanning until the cache is full or there's nothing more to scan
            while ( pendingItemsToCache.size() < maxItemsToCache )
            {
                if ( !scanner.next() )
                {
                    seekerExhausted = true;
                    break;
                }
                queueIdsFromTreeItem( scanner.key(), scanner.value(), pendingItemsToCache, maxItemsToCache );
            }
            // If there's more left to scan "this round" then make a note of it so that we start from this place the next time
            ongoingScanRangeIndex = seekerExhausted ? null : scanner.key().getIdRangeIdx();
        }

        boolean somethingWasCached = pendingItemsToCache.size() > 0;
        if ( seekerExhausted )
        {
            if ( !somethingWasCached && startedNow )
            {
                // chill a bit until at least one id gets freed
                atLeastOneIdOnFreelist.set( false );
            }
        }
        return somethingWasCached;
    }

    private void queueIdsFromTreeItem( IdRangeKey key, IdRange range, LinkedChunkLongArray pendingItemsToCache, int maxItemsToCache )
    {
        final long baseId = key.getIdRangeIdx() * idsPerEntry;
        final boolean differentGeneration = generation != range.getGeneration();

        for ( int i = 0; i < idsPerEntry && pendingItemsToCache.size() < maxItemsToCache; i++ )
        {
            final IdState state = range.getState( i );
            if ( state == FREE || (differentGeneration && state == DELETED) )
            {
                pendingItemsToCache.add( baseId + i );
            }
        }
    }

    @Override
    public void close() throws IOException
    {   // nothing to close
    }
}
