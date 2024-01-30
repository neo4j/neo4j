/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.internal.id.indexed;

import static java.util.Collections.emptySet;
import static org.eclipse.collections.impl.block.factory.Comparators.naturalOrder;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.GBPTree.NO_HEADER_READER;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.index.internal.gbptree.StructureWriteLog.structureWriteLog;
import static org.neo4j.internal.id.IdValidator.assertIdWithinMaxCapacity;
import static org.neo4j.internal.id.IdValidator.hasReservedIdInRange;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.PageCacheOpenOptions.MULTI_VERSIONED;

import java.io.IOException;
import java.io.PrintStream;
import java.io.UncheckedIOException;
import java.nio.file.NoSuchFileException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
import org.apache.commons.lang3.mutable.MutableLong;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.collection.PrimitiveLongResourceCollections;
import org.neo4j.collection.PrimitiveLongResourceIterator;
import org.neo4j.common.EmptyDependencyResolver;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.GBPTreeVisitor;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.index.internal.gbptree.MultiRootGBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.index.internal.gbptree.TreeNodeLayoutFactory;
import org.neo4j.index.internal.gbptree.ValueHolder;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.id.FreeIds;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.IdSlotDistribution;
import org.neo4j.internal.id.IdType;
import org.neo4j.internal.id.IdValidator;
import org.neo4j.internal.id.range.ArrayBasedRange;
import org.neo4j.internal.id.range.ContinuousIdRange;
import org.neo4j.internal.id.range.PageIdRange;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.util.Preconditions;

/**
 * At the heart of this free-list sits a {@link GBPTree}, containing all deleted and freed ids. The tree is used as a bit-set and since it's
 * sorted then it can be later extended to allocate multiple consecutive ids. Another design feature of this free-list is that it's crash-safe,
 * that is if the {@link #transactionalMarker(CursorContext)} is only used for applying committed data.
 */
public class IndexedIdGenerator implements IdGenerator {
    public interface Monitor extends AutoCloseable {
        void opened(long highestWrittenId, long highId);

        @Override
        void close();

        void allocatedFromHigh(long allocatedId, int numberOfIds);

        void allocatedFromReused(long allocatedId, int numberOfIds);

        void cached(long cachedId, int numberOfIds);

        void markedAsUsed(long markedId, int numberOfIds);

        void markedAsDeleted(long markedId, int numberOfIds);

        void markedAsFree(long markedId, int numberOfIds);

        void markedAsReserved(long markedId, int numberOfIds);

        void markedAsUnreserved(long markedId, int numberOfIds);

        void markSessionDone();

        void normalized(long idRange);

        void bridged(long bridgedId, long numberOfIds);

        void checkpoint(long highestWrittenId, long highId);

        void clearingCache();

        void clearedCache();

        void skippedIdsAtHighId(long firstSkippedId, int numberOfIds);

        void skippedIdsAtAllocation(long firstWastedId, int numberOfIds);

        class Adapter implements Monitor {
            @Override
            public void opened(long highestWrittenId, long highId) {}

            @Override
            public void allocatedFromHigh(long allocatedId, int numberOfIds) {}

            @Override
            public void allocatedFromReused(long allocatedId, int numberOfIds) {}

            @Override
            public void cached(long cachedId, int numberOfIds) {}

            @Override
            public void markedAsUsed(long markedId, int numberOfIds) {}

            @Override
            public void markedAsDeleted(long markedId, int numberOfIds) {}

            @Override
            public void markedAsFree(long markedId, int numberOfIds) {}

            @Override
            public void markedAsReserved(long markedId, int numberOfIds) {}

            @Override
            public void markedAsUnreserved(long markedId, int numberOfIds) {}

            @Override
            public void markSessionDone() {}

            @Override
            public void normalized(long idRange) {}

            @Override
            public void bridged(long bridgedId, long numberOfIds) {}

            @Override
            public void checkpoint(long highestWrittenId, long highId) {}

            @Override
            public void clearingCache() {}

            @Override
            public void clearedCache() {}

            @Override
            public void skippedIdsAtHighId(long firstSkippedId, int numberOfIds) {}

            @Override
            public void skippedIdsAtAllocation(long firstWastedId, int numberOfIds) {}

            @Override
            public void close() {}
        }
    }

    public static final Monitor NO_MONITOR = new Monitor.Adapter();

    /**
     * Represents the absence of an id in the id cache.
     */
    static final long NO_ID = -1;

    /**
     * Number of ids per entry in the GBPTree.
     */
    public static final int IDS_PER_ENTRY = 128;

    /**
     * Used for id generators that generally has low activity.
     * 2^8 == 256 and one ID takes up 8B, which results in a memory usage of 256 * 8 = ~2k memory
     */
    static final int SMALL_CACHE_CAPACITY = 1 << 8;

    /**
     * Used for id generators that generally has high activity.
     * 2^16 == 65536 and one ID takes up 8B, which results in a memory usage of 65536 * 8 = ~524k memory
     * But that is the maximum the ID cache can occupy - the cache is divided into chunks, each one 1024 IDs
     * so when there's nothing or very little in the cache it will only occupy ~8k memory
     */
    static final int LARGE_CACHE_CAPACITY = 1 << 13;

    /**
     * First generation the tree entries will start at. Generation will be incremented each time an IndexedIdGenerator is opened,
     * i.e. not for every checkpoint. Generation is used to do lazy normalization of id states, so that DELETED ids from a previous generation
     * looks like FREE in the current session. Updates to tree items (except for recovery) will reset the generation that of the current session.
     */
    private static final long STARTING_GENERATION = 1;

    /**
     * {@link GBPTree} for storing and accessing the id states.
     */
    private final GBPTree<IdRangeKey, IdRange> tree;

    /**
     * Cache of free ids to be handed out from {@link #nextId(CursorContext)}. Populated by {@link FreeIdScanner}.
     */
    private final IdCache cache;

    /**
     * {@link IdType} that this id generator covers.
     */
    private final IdType idType;

    /**
     * Number of ids per {@link IdRange} in the {@link GBPTree}.
     */
    private final int idsPerEntry;

    /**
     * Cache low-watermark when to trigger {@link FreeIdScanner} for refill.
     */
    private final int cacheOptimisticRefillThreshold;

    /**
     * Note about contention: Calls to commitMarker() should be worksync'ed externally and will therefore not contend.
     * This lock is about guarding for calls to reuseMarker(), which comes in at arbitrary times outside transactions.
     */
    private final Lock transactionalMarkerLock = new ReentrantLock();

    /**
     * {@link GBPTree} {@link Layout} for this id generator.
     */
    private final IdRangeLayout layout;

    /**
     * Scans the stored ids and places into cache for quick access in {@link #nextId(CursorContext)}.
     */
    private final FreeIdScanner scanner;

    /**
     * High id of this id generator (and to some extent the store this covers).
     */
    private final AtomicLong highId = new AtomicLong();

    /**
     * Maximum id that this id generator can allocate.
     */
    private final long maxId;

    /**
     * Means of communicating that there are stored free ids that {@link FreeIdScanner} could pick up.
     * It's typically incremented by {@link IdRangeMarker} and compared in {@link FreeIdScanner}.
     */
    private final AtomicInteger freeIdsNotifier = new AtomicInteger();

    /**
     * Current generation of this id generator. Generation is used to normalize id states so that a deleted id of a previous generation
     * can be seen as free in the current generation. Generation is bumped on restart.
     */
    private final long generation;

    /**
     * Internal state kept between constructor and {@link #start(FreeIds, CursorContext)},
     * whether or not to rebuild the id generator from the supplied {@link FreeIds}.
     */
    private final boolean needsRebuild;

    /**
     * Highest ever written id in this id generator. This is used to not lose track of ids allocated off of high id that are not committed.
     * See more in {@link IdRangeMarker}.
     */
    private final AtomicLong highestWrittenId = new AtomicLong();

    private final FileSystemAbstraction fileSystem;
    private final Path path;

    /**
     * {@code false} after construction and before a call to {@link IdGenerator#start(FreeIds, CursorContext)},
     * where false means that operations made this freelist
     * is to be treated as recovery operations. After a call to {@link IdGenerator#start(FreeIds, CursorContext)}
     * the operations are to be treated as normal operations.
     */
    private volatile boolean started;

    private final IdRangeMerger defaultMerger;
    private final IdRangeMerger recoveryMerger;
    private final boolean readOnly;
    private final IdSlotDistribution slotDistribution;
    private final PageCacheTracer pageCacheTracer;
    private final boolean useDirectToCache;
    private final CursorContextFactory contextFactory;

    private final Monitor monitor;
    private final boolean strictlyPrioritizeFreelist;
    private final int biggestSlotSize;

    private final Set<Long> lockedPageRanges;

    public IndexedIdGenerator(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path path,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            IdType idType,
            boolean allowLargeIdCaches,
            LongSupplier initialHighId,
            long maxId,
            boolean readOnly,
            Config config,
            String databaseName,
            CursorContextFactory contextFactory,
            Monitor monitor,
            ImmutableSet<OpenOption> openOptions,
            IdSlotDistribution slotDistribution,
            PageCacheTracer tracer,
            boolean allocationEnabled,
            boolean useDirectToCache) {
        this.fileSystem = fileSystem;
        this.path = path;
        this.readOnly = readOnly;
        this.contextFactory = contextFactory;
        this.slotDistribution = slotDistribution;
        this.pageCacheTracer = tracer;
        this.useDirectToCache = useDirectToCache;
        int cacheCapacity = idType.highActivity() && allowLargeIdCaches ? LARGE_CACHE_CAPACITY : SMALL_CACHE_CAPACITY;
        this.idType = idType;
        IdSlotDistribution.Slot[] slots = slotDistribution.slots(cacheCapacity);
        this.cache = new IdCache(slots);
        this.biggestSlotSize = Arrays.stream(slots)
                .map(IdSlotDistribution.Slot::slotSize)
                .max(naturalOrder())
                .orElseThrow();
        this.maxId = maxId;
        this.monitor = monitor;
        this.lockedPageRanges = isMultiVersioned(openOptions) ? ConcurrentHashMap.newKeySet() : emptySet();
        this.defaultMerger = new IdRangeMerger(false, monitor);
        this.recoveryMerger = new IdRangeMerger(true, monitor);

        this.idsPerEntry = slotDistribution.idsPerEntry();
        this.layout = new IdRangeLayout(idsPerEntry);
        HeaderReader header = new HeaderReader();
        this.tree = instantiateTree(
                pageCache,
                path,
                header,
                recoveryCleanupWorkCollector,
                readOnly,
                databaseName,
                contextFactory,
                openOptions,
                config);

        // Why do we need to check STARTING_GENERATION here, we never write that value to header?
        // Good question! Because we need to be backwards compatible.
        // GBPTree used to write header in constructor and at that point we could end up in this scenario:
        // 1. start on existing store, but with missing .id file so that it gets created
        //    (at this point we used to write STARTING_GENERATION to header)
        // 2. rebuild will happen in start(), but perhaps the db was shut down or killed before or during start()
        // 3. next startup would have said that it wouldn't need rebuild
        this.needsRebuild = !header.wasRead || header.generation == STARTING_GENERATION;
        if (!needsRebuild) {
            // This id generator exists, use the values from its header
            this.highId.set(header.highId);
            this.highestWrittenId.set(header.highestWrittenId);
            this.generation = header.generation + 1;
            Preconditions.checkState(
                    this.idsPerEntry == header.idsPerEntry,
                    "ID generator was opened with a different idsPerEntry:%s than what it was created with:%s",
                    header.idsPerEntry,
                    idsPerEntry);
            // Let's optimistically assume that there may be some free ids in here. This will ensure that a scan
            // is triggered on first request
            this.freeIdsNotifier.incrementAndGet();
        } else {
            // We're creating this file, so set initial values
            this.highId.set(initialHighId.getAsLong());
            this.highestWrittenId.set(highId.get() - 1);
            this.generation = STARTING_GENERATION + 1;
        }
        monitor.opened(highestWrittenId.get(), highId.get());

        this.strictlyPrioritizeFreelist = !isMultiVersioned(openOptions)
                && config.get(GraphDatabaseInternalSettings.strictly_prioritize_id_freelist);
        this.cacheOptimisticRefillThreshold = strictlyPrioritizeFreelist ? 0 : cacheCapacity / 4;
        this.scanner = new FreeIdScanner(
                idsPerEntry,
                tree,
                layout,
                cache,
                freeIdsNotifier,
                this::contextualMarker,
                generation,
                strictlyPrioritizeFreelist,
                monitor,
                allocationEnabled,
                useDirectToCache);
    }

    private GBPTree<IdRangeKey, IdRange> instantiateTree(
            PageCache pageCache,
            Path path,
            HeaderReader headerReader,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly,
            String databaseName,
            CursorContextFactory contextFactory,
            ImmutableSet<OpenOption> openOptions,
            Config config) {
        try {
            return new GBPTree<>(
                    pageCache,
                    fileSystem,
                    path,
                    layout,
                    MultiRootGBPTree.NO_MONITOR,
                    headerReader,
                    recoveryCleanupWorkCollector,
                    readOnly,
                    openOptions.newWithout(MULTI_VERSIONED),
                    databaseName,
                    "Indexed ID generator",
                    contextFactory,
                    pageCacheTracer,
                    EmptyDependencyResolver.EMPTY_RESOLVER,
                    TreeNodeLayoutFactory.getInstance(),
                    structureWriteLog(fileSystem, path, config));
        } catch (TreeFileNotFoundException e) {
            throw new IllegalStateException(
                    "Id generator file could not be found, most likely this database needs to be recovered, file:"
                            + path,
                    e);
        }
    }

    @Override
    public long nextId(CursorContext cursorContext) {
        do {
            // If strictly prioritizing the freelist then the method below will block on the current scan,
            // if there's any ongoing, otherwise it will not block.
            checkRefillCache(cursorContext);
            long id = cache.takeOrDefault(NO_ID);
            if (id != NO_ID) {
                monitor.allocatedFromReused(id, 1);
                return id;
            }
            // If strictly prioritizing the freelist then stay in this loop until either there's an available
            // free ID or there are no more to be found. The loop will not be busy-wait given the blocking
            // nature of the scan in this scenario.
        } while (strictlyPrioritizeFreelist && scanner.hasMoreFreeIds(false));

        // There was no ID in the cache. This could be that either there are no free IDs in here (the typical case),
        // or a benign race where the cache ran out of IDs and it's very soon filled with more IDs from an ongoing
        // scan. We have made the decision to prioritise performance and so we don't just sit here waiting for an
        // ongoing scan to find IDs (fast as it may be, although it can be I/O bound) so we allocate from highId
        // instead. This make highId slide a little even if there actually are free ids available,
        // but this should be a fairly rare event.
        long id;
        do {
            id = highId.getAndIncrement();
            assertIdWithinMaxCapacity(idType, id, maxId);
        } while (IdValidator.isReservedId(id));
        monitor.allocatedFromHigh(id, 1);
        return id;
    }

    @Override
    public PageIdRange nextPageRange(CursorContext cursorContext, int idsPerPage) {
        checkRefillCache(cursorContext);
        long[] reusedIds = cache.drainRange(idsPerPage);
        if (reusedIds.length > 0) {
            // we have some reuse ids available that we allocated from cache
            // now we need to make sure that range is not yet used by any other concurrent allocator
            // so we check if it's not yet locked before returning it, otherwise we mark those ids unallocated and
            // fallback to new range
            Arrays.sort(reusedIds);
            var range = PageIdRange.wrap(reusedIds, idsPerPage);
            if (lockedPageRanges.add(range.pageId())) {
                return range;
            } else {
                // we mark optimistically allocated range as unallocated and fallback to new ids
                try (var marker = lockAndInstantiateMarker(false, false, cursorContext)) {
                    range.unallocate(marker);
                }
            }
        }

        long currentHighId;
        long requestSize;
        do {
            currentHighId = highId.getAcquire();
            requestSize = idsPerPage - currentHighId % idsPerPage;
        } while (!highId.weakCompareAndSetRelease(currentHighId, currentHighId + requestSize));
        assertIdWithinMaxCapacity(idType, currentHighId + idsPerPage, maxId);
        monitor.allocatedFromHigh(currentHighId, (int) requestSize);
        var pageIdRange = hasReservedIdInRange(currentHighId, currentHighId + idsPerPage)
                ? rangeWithoutReservedId(idsPerPage, currentHighId)
                : new ContinuousIdRange(currentHighId, (int) requestSize, idsPerPage);
        lockedPageRanges.add(pageIdRange.pageId());
        return pageIdRange;
    }

    @Override
    public void releasePageRange(PageIdRange range, CursorContext context) {
        if (range.hasNext()) {
            try (var marker = transactionalMarker(context)) {
                range.unallocate(marker);
            }
        }
        lockedPageRanges.remove(range.pageId());
    }

    @Override
    public long nextConsecutiveIdRange(int numberOfIds, boolean favorSamePage, CursorContext cursorContext) {
        if (numberOfIds <= biggestSlotSize) {
            // TODO to fill cache in a do-while would be preferrable here too, but slightly harder since the scanner
            //  may say that there are more free IDs, but there may not actually be more free IDs of the given
            //  numberOfIds
            checkRefillCache(cursorContext);
            long id = cache.takeOrDefault(NO_ID, numberOfIds, monitor, scanner::queueWastedCachedId);
            if (id != NO_ID) {
                monitor.allocatedFromReused(id, numberOfIds);
                return id;
            }
        }

        long readHighId;
        long endId;
        int skipped;
        long id;
        do {
            readHighId = highId.get();
            id = readHighId;
            endId = readHighId + numberOfIds - 1;
            skipped = 0;
            if (favorSamePage && layout.idRangeIndex(readHighId) != layout.idRangeIndex(endId)) {
                // The page boundary was crossed. Go to the beginning of the next page
                long newId = layout.idRangeIndex(endId) * idsPerEntry;
                skipped = (int) (newId - id);
                id = newId;
                endId = id + numberOfIds - 1;
            }
            assertIdWithinMaxCapacity(idType, endId, maxId);
        } while (!highId.compareAndSet(readHighId, endId + 1) || hasReservedIdInRange(id, endId + 1));
        monitor.allocatedFromHigh(id, numberOfIds);
        if (skipped > 0) {
            // Tell FreeIdScanner about this temporary waste?
            // What happens with this "waste" if we do nothing?
            // The IDs will be marked as deleted by the "ID gap bridging", but not marked as free because it's not
            // allowed because of this scenario:
            //  - T1 allocates ID 10 from highId
            //  - T2 allocates ID 11 from highId
            //  - T2 commits and bridges the gap that it sees for ID 10
            //  - T1 commits
            // If the bridging of ID 10 would also mark it as free then there's a chance that the ID scanner would pick
            // up 10 and hand it out
            // before T1 commits.
            // OK, so the skipped IDs will be marked as deleted, but will never be picked up by the ID scanner in this
            // generation/session
            // and therefore temporarily wasted until next restart.
            var accepted = cache.offer(readHighId, skipped, monitor);
            if (accepted < skipped) {
                scanner.queueSkippedHighId(readHighId + accepted, skipped - accepted);
            }
            monitor.skippedIdsAtHighId(id, numberOfIds);
        }
        return id;
    }

    @Override
    public TransactionalMarker transactionalMarker(CursorContext cursorContext) {
        if (!started && needsRebuild) {
            // If we're in recovery and know that we're building the id generator from scratch after recovery has
            // completed then don't make any updates
            return NOOP_MARKER;
        }

        var realMarker =
                lockAndInstantiateMarker(true, useDirectToCache && !scanner.allocationEnabled(), cursorContext);
        if (!useDirectToCache) {
            return realMarker;
        }
        return new TransactionalMarker.Delegate(realMarker) {
            @Override
            public void markDeletedAndFree(long id, int numberOfIds) {
                realMarker.markDeleted(id, numberOfIds);
                feedDirectlyToCache(id, numberOfIds, realMarker);
            }
        };
    }

    @Override
    public ContextualMarker contextualMarker(CursorContext cursorContext) {
        if (!allocationEnabled()) {
            throw new IllegalStateException(
                    "This ID generator has allocation disabled, which means it should not need to do contextual updates");
        }
        if (!started && needsRebuild) {
            // If we're in recovery and know that we're building the id generator from scratch after recovery has
            // completed then don't make any updates
            return NOOP_MARKER;
        }

        var realMarker = instantiateMarker(null, true, false, cursorContext);
        if (!useDirectToCache) {
            return realMarker;
        }
        return new ContextualMarker.Delegate(realMarker) {
            @Override
            public void markFree(long id, int numberOfIds) {
                feedDirectlyToCache(id, numberOfIds, realMarker);
            }
        };
    }

    private void feedDirectlyToCache(long id, int numberOfIds, IdRangeMarker realMarker) {
        // Take a short-cut if possible, which is to place the free ID into cache right away
        // The ones that gets accepted... let's not mark those at all. This has two benefits:
        // - Reduce mark calls for free
        // - Avoid a race where an ID would be added to cache and before we would mark it as free/reserved
        //   the ID would be allocated and committed and race with our updates here
        // We just have to make sure that these IDs gets marked as free/reserved or similar if leaving
        // the cache w/o getting marked as used, e.g. for:
        // - rollback
        // - only a subset of the ID gets allocated
        // - clear cache
        var accepted = cache.offer(id, numberOfIds, monitor);

        // Mark those that weren't added to the cache as free (the "slow" route)
        if (accepted < numberOfIds) {
            realMarker.markFree(id + accepted, numberOfIds - accepted);
        }
    }

    IdRangeMarker lockAndInstantiateMarker(boolean bridgeIdGaps, boolean deleteAlsoFrees, CursorContext cursorContext) {
        transactionalMarkerLock.lock();
        try {
            return instantiateMarker(transactionalMarkerLock, bridgeIdGaps, deleteAlsoFrees, cursorContext);
        } catch (Exception e) {
            transactionalMarkerLock.unlock();
            throw e;
        }
    }

    private IdRangeMarker instantiateMarker(
            Lock lock, boolean bridgeIdGaps, boolean deleteAlsoFrees, CursorContext cursorContext) {
        try {
            return new IdRangeMarker(
                    idsPerEntry,
                    layout,
                    tree.writer(cursorContext),
                    lock,
                    started ? defaultMerger : recoveryMerger,
                    started,
                    freeIdsNotifier,
                    generation,
                    highestWrittenId,
                    bridgeIdGaps,
                    deleteAlsoFrees,
                    monitor);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        closeAllUnchecked(tree, monitor);
    }

    @Override
    public long getHighId() {
        return highId.get();
    }

    @Override
    public void setHighId(long newHighId) {
        // Apparently there's this thing where there's a check that highId is only set if it's higher than the current
        // highId,
        // i.e. highId cannot be set to something lower than it already is. This check is done in the store
        // implementation.
        // But can we rely on it always guarding this, and can this even happen at all? Anyway here's a simple guard for
        // not setting it to something lower.
        long expect;
        do {
            expect = highId.get();
        } while (newHighId > expect && !highId.compareAndSet(expect, newHighId));
    }

    @Override
    public void start(FreeIds freeIdsForRebuild, CursorContext cursorContext) throws IOException {
        if (needsRebuild) {
            assertNotReadOnly();
            // This id generator was created right now, it needs to be populated with all free ids from its owning store
            // so that it's in sync
            var visitsDeletedIds = freeIdsForRebuild.visitsDeletedIds();
            try (IdRangeMarker idRangeMarker = lockAndInstantiateMarker(!visitsDeletedIds, true, cursorContext)) {
                // We can mark the ids as free right away since this is before started which means we get the very
                // liberal merger
                var highestId = freeIdsForRebuild.accept((id, numberOfIds) -> {
                    if (visitsDeletedIds) {
                        idRangeMarker.markDeleted(id, numberOfIds);
                    } else {
                        idRangeMarker.markUsed(id, numberOfIds);
                    }
                });
                highId.set(highestId + 1);
                highestWrittenId.set(highestId);
            }
        }

        started = true;

        // After potentially recovery has been run and everything is prepared to get going let's call maintenance,
        // which will fill the ID buffers right away before any request comes to the db.
        maintenance(cursorContext);
    }

    @Override
    public void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) {
        tree.checkpoint(
                new HeaderWriter(highId::get, highestWrittenId::get, generation, idsPerEntry),
                flushEvent,
                cursorContext);
        monitor.checkpoint(highestWrittenId.get(), highId.get());
    }

    @Override
    public void maintenance(CursorContext cursorContext) {
        if (started && !cache.isFull() && !readOnly) {
            // We're just helping other allocation requests and avoiding unwanted sliding of highId here
            scanner.tryLoadFreeIdsIntoCache(true, true, cursorContext);
        }
    }

    private void checkRefillCache(CursorContext cursorContext) {
        if (cache.size() <= cacheOptimisticRefillThreshold) {
            // We're just helping other allocation requests and avoiding unwanted sliding of highId here
            scanner.tryLoadFreeIdsIntoCache(strictlyPrioritizeFreelist, false, cursorContext);
        }
    }

    @Override
    public void clearCache(boolean allocationEnabled, CursorContext cursorContext) {
        if (!readOnly) {
            // Make the scanner clear it because it needs to coordinate with the scan lock
            monitor.clearingCache();
            scanner.clearCache(allocationEnabled, cursorContext);
            monitor.clearedCache();
        }
    }

    @Override
    public boolean allocationEnabled() {
        return scanner.allocationEnabled();
    }

    @Override
    public IdType idType() {
        return idType;
    }

    @Override
    public boolean hasOnlySingleIds() {
        return slotDistribution.maxSlotSize() == 1;
    }

    @Override
    public long getHighestPossibleIdInUse() {
        return getHighId() - 1;
    }

    @Override
    public long getUnusedIdCount() throws IOException {
        // This is an expensive operation but at the time of writing it's only used
        // for estimating free store space for metrics, computed every 10 minutes.
        return PrimitiveLongResourceCollections.count(notUsedIdsIterator());
    }

    /**
     * A peculiar being this one. It's for the import case where all records are written w/o even touching the id generator.
     * When all have been written the id generator is told that it should consider highest written where it's at right now
     * So that it won't mark all ids as deleted on the first write (the id bridging).
     */
    @Override
    public void markHighestWrittenAtHighId() {
        assertNotReadOnly();
        this.highestWrittenId.set(highId.get() - 1);
    }

    @Override
    public long getHighestWritten() {
        return highestWrittenId.get();
    }

    public Path path() {
        return path;
    }

    /**
     * Reads contents of a header in an existing {@link IndexedIdGenerator}.
     *
     * @param path        {@link Path} pointing to the id generator.
     * @return {@link Optional} with the data embedded inside the {@link HeaderReader} if the id generator existed and the header was read correctly, otherwise
     * {@link Optional#empty()}.
     */
    private static Optional<HeaderReader> readHeader(
            FileSystemAbstraction fileSystem, Path path, ImmutableSet<OpenOption> openOptions) {
        HeaderReader headerReader = new HeaderReader();
        return MultiRootGBPTree.readHeader(fileSystem, path, headerReader, openOptions);
    }

    /**
     * Dumps the contents of an {@link IndexedIdGenerator} as human-readable text.
     *
     * @param pageCache  {@link PageCache} to map id generator in.
     * @param path       {@link Path} pointing to the id generator.
     * @throws IOException if the file was missing or some other I/O error occurred.
     */
    public static void dump(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path path,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            boolean onlySummary,
            ImmutableSet<OpenOption> openOptions,
            PrintStream out)
            throws IOException {
        HeaderReader header = readHeader(fileSystem, path, openOptions)
                .orElseThrow(() -> new NoSuchFileException(path.toAbsolutePath().toString()));
        IdRangeLayout layout = new IdRangeLayout(header.idsPerEntry);
        try (GBPTree<IdRangeKey, IdRange> tree = new GBPTree<>(
                pageCache,
                fileSystem,
                path,
                layout,
                MultiRootGBPTree.NO_MONITOR,
                NO_HEADER_READER,
                immediate(),
                true,
                openOptions.newWithout(MULTI_VERSIONED),
                DEFAULT_DATABASE_NAME,
                "Indexed ID generator",
                contextFactory,
                pageCacheTracer)) {
            out.println(header);
            if (onlySummary) {
                MutableLong numDeletedNotFreed = new MutableLong();
                MutableLong numDeletedAndFreed = new MutableLong();
                out.println("Calculating summary...");
                try (var cursorContext = contextFactory.create("IndexDump")) {
                    tree.visit(
                            new GBPTreeVisitor.Adaptor<>() {
                                @Override
                                public void value(ValueHolder<IdRange> value) {
                                    for (int i = 0; i < header.idsPerEntry; i++) {
                                        IdRange.IdState state = value.value.getState(i);
                                        if (state == IdRange.IdState.FREE) {
                                            numDeletedAndFreed.increment();
                                        } else if (state == IdRange.IdState.DELETED) {
                                            numDeletedNotFreed.increment();
                                        }
                                    }
                                }
                            },
                            cursorContext);
                }

                out.println();
                out.println("Number of IDs deleted and available for reuse: " + numDeletedAndFreed);
                out.println("Number of IDs deleted, but not yet available for reuse: " + numDeletedNotFreed);
                out.printf(
                        "NOTE: A deleted ID not yet available for reuse is buffered until all transactions that were open%n"
                                + "at the time of its deletion have been closed, or the database is restarted%n");
            } else {
                try (var cursorContext = contextFactory.create("IndexDump")) {
                    tree.visit(
                            new GBPTreeVisitor.Adaptor<>() {
                                private IdRangeKey key;

                                @Override
                                public void key(IdRangeKey key, boolean isLeaf, long offloadId) {
                                    this.key = key;
                                }

                                @Override
                                public void value(ValueHolder<IdRange> value) {
                                    long rangeIndex = key.getIdRangeIdx();
                                    int idsPerEntry = layout.idsPerEntry();
                                    out.printf(
                                            "%s [rangeIndex: %d, i.e. IDs:%d-%d]%n",
                                            value.value,
                                            rangeIndex,
                                            rangeIndex * idsPerEntry,
                                            (rangeIndex + 1) * idsPerEntry - 1);
                                }
                            },
                            cursorContext);
                }
            }
            out.println(header);
        }
    }

    @Override
    public PrimitiveLongResourceIterator notUsedIdsIterator() throws IOException {
        return notUsedIdsIterator(0, getHighId());
    }

    @Override
    public PrimitiveLongResourceIterator notUsedIdsIterator(long fromIdInclusive, long toIdExclusive)
            throws IOException {
        return conditionalIdIterator(fromIdInclusive, toIdExclusive, state -> IdRange.IdState.USED != state);
    }

    @Override
    public PrimitiveLongResourceIterator freeIdsIterator() throws IOException {
        return conditionalIdIterator(0, getHighId(), state -> IdRange.IdState.FREE == state);
    }

    private PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator conditionalIdIterator(
            long fromIdInclusive, long toIdExclusive, Predicate<IdRange.IdState> idStatePredicate) throws IOException {
        Preconditions.checkArgument(fromIdInclusive <= toIdExclusive, "From Id needs to be lesser than toId");
        long fromRange = fromIdInclusive / idsPerEntry;
        long toRange = (toIdExclusive / idsPerEntry) + 1;
        CursorContext context = contextFactory.create("FreeIdIterator");
        Seeker<IdRangeKey, IdRange> scanner = tree.seek(new IdRangeKey(fromRange), new IdRangeKey(toRange), context);
        // If fromIdInclusive == toIdExclusive then we're checking existence of a single ID
        long compareToIdExclusive = fromIdInclusive == toIdExclusive ? toIdExclusive + 1 : toIdExclusive;

        return new PrimitiveLongResourceCollections.AbstractPrimitiveLongBaseResourceIterator(
                () -> closeAllUnchecked(scanner, context)) {

            private IdRangeKey currentKey;
            private IdRange currentRange;
            private int nextIndex = fromIdInclusive == toIdExclusive ? (int) (fromIdInclusive % idsPerEntry) : 0;
            private boolean reachedEnd;

            @Override
            protected boolean fetchNext() {
                try {
                    while (!reachedEnd) {
                        if (currentRange == null) {
                            if (!scanner.next()) {
                                reachedEnd = true;
                                return false;
                            }
                            currentRange = scanner.value();
                            currentKey = scanner.key();
                        }
                        while (nextIndex < idsPerEntry) {
                            int index = nextIndex++;
                            long id = currentKey.getIdRangeIdx() * idsPerEntry + index;
                            if (id >= compareToIdExclusive) {
                                return false;
                            }
                            if ((id >= fromIdInclusive) && idStatePredicate.test(currentRange.getState(index))) {
                                return next(id);
                            }
                        }
                        currentRange = null;
                        nextIndex = 0;
                    }

                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
                return false;
            }
        };
    }

    @Override
    public boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        return tree.consistencyCheck(reporterFactory, contextFactory, numThreads, progressMonitorFactory);
    }

    private void assertNotReadOnly() {
        Preconditions.checkState(!readOnly, "ID generator '%s' is read-only", path);
    }

    private static PageIdRange rangeWithoutReservedId(int idsPerPage, long currentHighId) {
        long[] ids = new long[idsPerPage - 1];
        for (int i = 0; i < ids.length; i++) {
            long value = currentHighId++;
            if (!IdValidator.isReservedId(value)) {
                ids[i] = value;
            }
        }
        return new ArrayBasedRange(ids, idsPerPage);
    }

    private static boolean isMultiVersioned(ImmutableSet<OpenOption> openOptions) {
        return openOptions.contains(MULTI_VERSIONED);
    }
}
