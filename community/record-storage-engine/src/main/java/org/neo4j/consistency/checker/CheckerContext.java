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
package org.neo4j.consistency.checker;

import static org.neo4j.internal.helpers.Format.duration;

import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import org.neo4j.common.EntityType;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.consistency.checker.ParallelExecution.ThrowingRunnable;
import org.neo4j.consistency.checking.ConsistencyFlags;
import org.neo4j.consistency.checking.cache.CacheAccess;
import org.neo4j.consistency.checking.index.IndexAccessors;
import org.neo4j.consistency.report.ConsistencyReport;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.logging.InternalLog;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.time.Stopwatch;
import org.neo4j.token.TokenHolders;

class CheckerContext {
    final NeoStores neoStores;
    final IndexAccessors indexAccessors;
    final ConsistencyFlags consistencyFlags;
    final IndexSizes indexSizes;
    final ParallelExecution execution;
    final ConsistencyReport.Reporter reporter;
    final CacheAccess cacheAccess;
    final TokenHolders tokenHolders;
    final RecordLoading recordLoader;
    final CountsState observedCounts;
    final EntityBasedMemoryLimiter limiter;
    final ProgressMonitorFactory.MultiPartBuilder progress;
    final TokenNameLookup tokenNameLookup;
    final PageCache pageCache;
    final MemoryTracker memoryTracker;
    final long highNodeId;
    final long highRelationshipId;
    final IndexAccessor nodeLabelIndex;
    final IndexAccessor relationshipTypeIndex;
    final CursorContextFactory contextFactory;
    final FreeIdCache propertyFreeIdCache;

    private final AtomicBoolean cancelled;
    private final InternalLog log;
    private final boolean verbose;

    CheckerContext(
            NeoStores neoStores,
            IndexAccessors indexAccessors,
            ParallelExecution execution,
            ConsistencyReport.Reporter reporter,
            CacheAccess cacheAccess,
            TokenHolders tokenHolders,
            RecordLoading recordLoader,
            CountsState observedCounts,
            EntityBasedMemoryLimiter limiter,
            ProgressMonitorFactory.MultiPartBuilder progress,
            PageCache pageCache,
            MemoryTracker memoryTracker,
            InternalLog log,
            boolean verbose,
            ConsistencyFlags consistencyFlags,
            CursorContextFactory contextFactory) {
        this(
                neoStores,
                indexAccessors,
                execution,
                reporter,
                cacheAccess,
                tokenHolders,
                recordLoader,
                observedCounts,
                new FreeIdCache(neoStores.getPropertyStore().getIdGenerator()),
                limiter,
                progress,
                pageCache,
                memoryTracker,
                log,
                verbose,
                new AtomicBoolean(),
                consistencyFlags,
                contextFactory);
    }

    private CheckerContext(
            NeoStores neoStores,
            IndexAccessors indexAccessors,
            ParallelExecution execution,
            ConsistencyReport.Reporter reporter,
            CacheAccess cacheAccess,
            TokenHolders tokenHolders,
            RecordLoading recordLoader,
            CountsState observedCounts,
            FreeIdCache propertyFreeIdCache,
            EntityBasedMemoryLimiter limiter,
            ProgressMonitorFactory.MultiPartBuilder progress,
            PageCache pageCache,
            MemoryTracker memoryTracker,
            InternalLog log,
            boolean verbose,
            AtomicBoolean cancelled,
            ConsistencyFlags consistencyFlags,
            CursorContextFactory contextFactory) {
        this.neoStores = neoStores;
        this.highNodeId = neoStores.getNodeStore().getIdGenerator().getHighId();
        this.highRelationshipId =
                neoStores.getRelationshipStore().getIdGenerator().getHighId();
        this.indexAccessors = indexAccessors;
        this.nodeLabelIndex = indexAccessors.nodeLabelIndex();
        this.relationshipTypeIndex = indexAccessors.relationshipTypeIndex();
        this.log = log;
        this.verbose = verbose;
        this.consistencyFlags = consistencyFlags;
        this.contextFactory = contextFactory;
        this.indexSizes =
                new IndexSizes(execution, indexAccessors, highNodeId, highRelationshipId, contextFactory, limiter);
        this.execution = execution;
        this.reporter = reporter;
        this.cacheAccess = cacheAccess;
        this.tokenHolders = tokenHolders;
        this.recordLoader = recordLoader;
        this.observedCounts = observedCounts;
        this.limiter = limiter;
        this.progress = progress;
        this.cancelled = cancelled;
        this.tokenNameLookup = tokenHolders.lookupWithIds();
        this.pageCache = pageCache;
        this.memoryTracker = memoryTracker;
        this.propertyFreeIdCache = propertyFreeIdCache;
    }

    CheckerContext withoutReporting() {
        return new CheckerContext(
                neoStores,
                indexAccessors,
                execution,
                ConsistencyReport.NO_REPORT,
                cacheAccess,
                tokenHolders,
                recordLoader,
                observedCounts,
                propertyFreeIdCache,
                limiter,
                progress,
                pageCache,
                memoryTracker,
                log,
                verbose,
                cancelled,
                consistencyFlags,
                contextFactory);
    }

    void initialize() throws Exception {
        debug(limiter.toString());
        timeOperation("Initialize index sizes", indexSizes::initialize, false);
        timeOperation("Initialize free-id cache for properties", propertyFreeIdCache::initialize, false);
        if (verbose) {
            debugPrintIndexes(indexSizes.largeIndexes(EntityType.NODE), "considered large node indexes");
            debugPrintIndexes(indexSizes.smallIndexes(EntityType.NODE), "considered small node indexes");
            debugPrintIndexes(
                    indexSizes.largeIndexes(EntityType.RELATIONSHIP), "considered large relationship indexes");
            debugPrintIndexes(
                    indexSizes.smallIndexes(EntityType.RELATIONSHIP), "considered small relationship indexes");
        }
    }

    void initializeRange() {
        observedCounts.clearDynamicNodeLabelsCache();
    }

    private void debugPrintIndexes(List<IndexDescriptor> indexes, String description) {
        if (!indexes.isEmpty()) {
            debug("These are %s (%d):", description, indexes.size());
            indexes.forEach(index -> debug("  %s", index));
        }
    }

    private void timeOperation(String description, ThrowingRunnable action, boolean linePadding) throws Exception {
        Stopwatch stopwatch = Stopwatch.start();
        try {
            debug(linePadding, "STARTED %s", description);
            action.doRun();
            debug(
                    linePadding,
                    "COMPLETED %s, took %s",
                    description,
                    duration(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
        } catch (Exception e) {
            debug(linePadding, "FAILED %s after %s", description, duration(stopwatch.elapsed(TimeUnit.MILLISECONDS)));
            throw e;
        }
    }

    boolean isCancelled() {
        return cancelled.get();
    }

    void cancel() {
        cancelled.set(true);
    }

    void runIfAllowed(Checker checker, LongRange range) throws Exception {
        if (!isCancelled() && checker.shouldBeChecked(consistencyFlags)) {
            timeOperation(
                    checker.toString(),
                    () -> checker.check(
                            range,
                            EntityBasedMemoryLimiter.isFirst(range),
                            limiter.isLast(range, checker.isNodeBasedCheck()),
                            memoryTracker),
                    true);
        }
    }

    void paddedDebug(String format, Object... params) {
        debug(true, format, params);
    }

    void debug(String format, Object... params) {
        debug(false, format, params);
    }

    private void debug(boolean linePadded, String format, Object... params) {
        if (verbose) {
            log.info((linePadded ? "%n" : "") + format, params);
        }
    }

    void error(String format, Object... params) {
        if (verbose) {
            log.error(format, params);
        }
    }

    ProgressListener progressReporter(Checker checker, String name, long totalCount) {
        int nbrRanges =
                checker.isNodeBasedCheck() ? limiter.numberOfNodeRanges() : limiter.numberOfRelationshipRanges();
        return roundInsensitiveProgressReporter(checker, name, totalCount * nbrRanges);
    }

    ProgressListener roundInsensitiveProgressReporter(Checker checker, String name, long totalCount) {
        if (!checker.shouldBeChecked(consistencyFlags)) {
            return ProgressListener.NONE;
        }
        return progress.progressForPart(name, totalCount);
    }

    boolean isVerbose() {
        return verbose;
    }
}
