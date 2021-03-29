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

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;

import java.io.Closeable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicBoolean;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.indexed.IndexedIdGenerator.InternalMarker;
import org.neo4j.io.pagecache.context.CursorContext;

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;
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
    static final int MAX_SLOT_SIZE = 128;

    private final int idsPerEntry;
    private final GBPTree<IdRangeKey, IdRange> tree;
    private final IdRangeLayout layout;
    private final IdCache cache;
    private final AtomicBoolean atLeastOneIdOnFreelist;
    private final MarkerProvider markerProvider;
    private final long generation;
    private final ScanLock lock;
    private final IndexedIdGenerator.Monitor monitor;
    /**
     * Manages IDs (ranges of IDs, really) that gets skipped when allocations are made from high ID.
     * This happens because one ID range cannot cross a page boundary. Allocators will populate this queue
     * and the thread getting the scan lock will consume it at a later point.
     */
    private final ConcurrentLinkedQueue<Long> queuedSkippedHighIds = new ConcurrentLinkedQueue<>();
    /**
     * Manages IDs (range of IDs, really) that sometimes gets temporarily wasted when an allocation is made
     * from the cache where the ID range size is somewhere between two slot sizes. The remainder of the ID range
     * will be queued in this queue and consumed by the thread getting the scan lock at a later point.
     */
    private final ConcurrentLinkedQueue<Long> queuedWastedCachedIds = new ConcurrentLinkedQueue<>();
    /**
     * State for whether or not there's an ongoing scan, and if so where it should begin from. This is used in
     * {@link #findSomeIdsToCache(PendingIdQueue, MutableLongList, MutableInt, CursorContext)}  both to know where to initiate a scan from and to
     * set it, if the cache got full before scan completed, or set it to null of the scan ended. The actual {@link Seeker} itself is local to the scan method.
     */
    private Long ongoingScanRangeIndex;

    FreeIdScanner( int idsPerEntry, GBPTree<IdRangeKey,IdRange> tree, IdRangeLayout layout, IdCache cache, AtomicBoolean atLeastOneIdOnFreelist,
            MarkerProvider markerProvider, long generation, boolean strictlyPrioritizeFreelistOverHighId, IndexedIdGenerator.Monitor monitor )
    {
        this.idsPerEntry = idsPerEntry;
        this.tree = tree;
        this.layout = layout;
        this.cache = cache;
        this.atLeastOneIdOnFreelist = atLeastOneIdOnFreelist;
        this.markerProvider = markerProvider;
        this.generation = generation;
        this.lock = strictlyPrioritizeFreelistOverHighId ? ScanLock.lockyAndPessimistic() : ScanLock.lockFreeAndOptimistic();
        this.monitor = monitor;
    }

    boolean tryLoadFreeIdsIntoCache( boolean awaitOngoing, CursorContext cursorContext )
    {
        return tryLoadFreeIdsIntoCache( awaitOngoing, false, cursorContext );
    }

    /**
     * Do a batch of scanning, either start a new scan from the beginning if none is active, or continue where a previous scan
     * paused. In this call free ids can be discovered and placed into the ID cache. IDs are marked as reserved before placed into cache.
     */
    boolean tryLoadFreeIdsIntoCache( boolean awaitOngoing, boolean forceScan, CursorContext cursorContext )
    {
        if ( ongoingScanRangeIndex != null && !forceScan && !thereAreLikelyFreeIdsToFind() )
        {
            // If no scan is in progress (SeekCursor now sitting and waiting at some leaf in the free-list)
            // and if we have no reason to expect finding any free id from a scan then don't do it.
            return false;
        }

        if ( scanLock( awaitOngoing ) )
        {
            try
            {
                markQueuedSkippedHighIdsAsFree( cursorContext );

                if ( atLeastOneIdOnFreelist.get() )
                {
                    // A new scan is commencing
                    // Get a snapshot of cache size before we start. At the end of the scan the actual space available to fill with IDs
                    // may be even bigger, but not smaller. This is important because we discover IDs, mark them as non-reusable
                    // and then place them in the cache so IDs that wouldn't fit in the cache would need to be marked as reusable again,
                    // which would be somewhat annoying.
                    MutableInt availableSpaceById = new MutableInt( cache.availableSpaceById() );
                    if ( availableSpaceById.intValue() > 0 )
                    {
                        // Find items to cache
                        PendingIdQueue pendingIdQueue = new PendingIdQueue( cache.slotsByAvailableSpace() );
                        // While we're at it have a look at the wasted IDs from handing out cached IDs smaller than their slot size
                        cacheWastedIds( pendingIdQueue, cursorContext );
                        MutableLongList pendingItemsToReserve = LongLists.mutable.empty();
                        if ( findSomeIdsToCache( pendingIdQueue, pendingItemsToReserve, availableSpaceById, cursorContext ) )
                        {
                            // Get a writer and mark the found ids as reserved
                            markIdsAsReserved( pendingItemsToReserve, cursorContext );

                            // Place them in the cache so that allocation requests can see them
                            cache.offer( pendingIdQueue, monitor );
                            return true;
                        }
                    }
                }
                else if ( !queuedWastedCachedIds.isEmpty() )
                {
                    markWastedIdsAsUnreserved( cursorContext );
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

    private void cacheWastedIds( PendingIdQueue pendingIdQueue, CursorContext cursorContext )
    {
        consumeQueuedIds( queuedWastedCachedIds, ( marker, id, size ) ->
        {
            int accepted = pendingIdQueue.offer( id, size );
            if ( accepted < size )
            {
                // A part or the whole range will not make it to the cache. Take the long route and mark this ID as unreserved
                // so that it will make it back into the cache at a later point instead.
                marker.markUnreserved( id + accepted, size - accepted );
            }
        }, cursorContext );
    }

    private void consumeQueuedIds( ConcurrentLinkedQueue<Long> queue, QueueConsumer consumer, CursorContext cursorContext )
    {
        if ( !queue.isEmpty() )
        {
            // There may be a race here which will result in ids that gets queued right when we flip missed here, but they will be picked
            // up on the next restart. It should be rare. And to introduce locking or synchronization to prevent it may not be worth it.
            try ( InternalMarker marker = markerProvider.getMarker( cursorContext ) )
            {
                Long idAndSize;
                while ( (idAndSize = queue.poll()) != null )
                {
                    long id = idFromCombinedId( idAndSize );
                    int size = numberOfIdsFromCombinedId( idAndSize );
                    consumer.accept( marker, id, size );
                }
            }
        }
    }

    private boolean thereAreLikelyFreeIdsToFind()
    {
        return atLeastOneIdOnFreelist.get() || !queuedSkippedHighIds.isEmpty() || !queuedWastedCachedIds.isEmpty();
    }

    private void markQueuedSkippedHighIdsAsFree( CursorContext cursorContext )
    {
        consumeQueuedIds( queuedSkippedHighIds, IdGenerator.Marker::markFree, cursorContext );
    }

    private void markWastedIdsAsUnreserved( CursorContext cursorContext )
    {
        consumeQueuedIds( queuedWastedCachedIds, IndexedIdGenerator.InternalMarker::markUnreserved, cursorContext );
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

    void clearCache( CursorContext cursorContext )
    {
        lock.lock();
        try
        {
            // Restart scan from the beginning after cache is cleared
            ongoingScanRangeIndex = null;

            // Since placing an id into the cache marks it as reserved, here when taking the ids out from the cache revert that by marking them as unreserved
            try ( InternalMarker marker = markerProvider.getMarker( cursorContext ) )
            {
                cache.drain( marker::markUnreserved );
            }
            atLeastOneIdOnFreelist.set( true );
        }
        finally
        {
            lock.unlock();
        }
    }

    void queueSkippedHighId( long id, int numberOfIds )
    {
        queuedSkippedHighIds.offer( combinedIdAndNumberOfIds( id, numberOfIds, false ) );
    }

    void queueWastedCachedId( long id, int numberOfIds )
    {
        queuedWastedCachedIds.offer( combinedIdAndNumberOfIds( id, numberOfIds, false ) );
    }

    private void markIdsAsReserved( MutableLongList pendingItemsToCache, CursorContext cursorContext )
    {
        try ( InternalMarker marker = markerProvider.getMarker( cursorContext ) )
        {
            pendingItemsToCache.forEach( item ->
            {
                long startId = idFromCombinedId( item );
                int numberOfIds = numberOfIdsFromCombinedId( item );
                marker.markReserved( startId, numberOfIds );
            } );
        }
    }

    private boolean findSomeIdsToCache( PendingIdQueue pendingIdQueue, MutableLongList pendingItemsToCache, MutableInt availableSpaceById,
            CursorContext cursorContext ) throws IOException
    {
        boolean startedNow = ongoingScanRangeIndex == null;
        IdRangeKey from = ongoingScanRangeIndex == null ? LOW_KEY : new IdRangeKey( ongoingScanRangeIndex );
        boolean seekerExhausted = false;
        try ( Seeker<IdRangeKey,IdRange> scanner = tree.seek( from, HIGH_KEY, cursorContext ) )
        {
            // Continue scanning until the cache is full or there's nothing more to scan
            while ( availableSpaceById.intValue() > 0 )
            {
                if ( !scanner.next() )
                {
                    seekerExhausted = true;
                    break;
                }
                queueIdsFromTreeItem( scanner.key(), scanner.value(), pendingIdQueue, pendingItemsToCache, availableSpaceById );
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

    private void queueIdsFromTreeItem( IdRangeKey key, IdRange range, PendingIdQueue pendingIdQueue, MutableLongList pendingItemsToCache,
            MutableInt availableSpaceById )
    {
        final long baseId = key.getIdRangeIdx() * idsPerEntry;
        final boolean differentGeneration = generation != range.getGeneration();

        int firstFreeI = -1;
        for ( int i = 0; i < idsPerEntry && availableSpaceById.intValue() > 0; i++ )
        {
            final IdState state = range.getState( i );
            boolean isFree = state == FREE || (differentGeneration && state == DELETED);
            if ( isFree )
            {
                if ( firstFreeI == -1 )
                {
                    firstFreeI = i;
                }
            }
            else if ( firstFreeI != -1 )
            {
                queueId( pendingIdQueue, pendingItemsToCache, availableSpaceById, baseId, firstFreeI, i );
                firstFreeI = -1;
            }
        }

        if ( firstFreeI != -1 )
        {
            queueId( pendingIdQueue, pendingItemsToCache, availableSpaceById, baseId, firstFreeI, idsPerEntry );
        }
    }

    private void queueId( PendingIdQueue pendingIdQueue, MutableLongList pendingItemsToCache, MutableInt availableSpaceById, long baseId, int firstFreeI,
            int i )
    {
        long startId = baseId + firstFreeI;
        int freeSlotSize = i - firstFreeI;

        assert layout.idRangeIndex( startId ) == layout.idRangeIndex( startId + freeSlotSize - 1 );

        int accepted = pendingIdQueue.offer( startId, freeSlotSize );
        if ( accepted > 0 )
        {
            pendingItemsToCache.add( combinedIdAndNumberOfIds( startId, accepted, false ) );
            availableSpaceById.addAndGet( -accepted );
        }
    }

    @Override
    public void close() throws IOException
    {   // nothing to close
    }

    private interface QueueConsumer
    {
        void accept( InternalMarker marker, long id, int size );
    }
}
