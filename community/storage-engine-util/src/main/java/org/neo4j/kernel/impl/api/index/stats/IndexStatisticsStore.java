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
package org.neo4j.kernel.impl.api.index.stats;

import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.kernel.impl.api.index.stats.IndexStatisticsKey.TYPE_SAMPLE;
import static org.neo4j.kernel.impl.api.index.stats.IndexStatisticsKey.TYPE_USAGE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.BiConsumer;
import java.util.function.Function;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.TreeFileNotFoundException;
import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.CommonDatabaseStores;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUsageStats;
import org.neo4j.kernel.impl.index.schema.ConsistencyCheckable;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

/**
 * A simple store for keeping index statistics counts, like number of updates, index size, number of unique values a.s.o.
 * These values aren't updated transactionally and so the data is just kept in memory and flushed to a {@link GBPTree} on every checkpoint.
 * Neither reads, writes nor checkpoints block each other.
 *
 * The store is accessible after {@link #init()} has been called.
 */
public class IndexStatisticsStore extends LifecycleAdapter
        implements IndexStatisticsVisitor.Visitable, ConsistencyCheckable, IndexUsageStatsConsumer {
    private static final IndexStatisticsValue EMPTY_STATISTICS = new IndexStatisticsValue();

    private final PageCache pageCache;
    private final FileSystemAbstraction fileSystem;
    private final Path path;
    private final RecoveryCleanupWorkCollector recoveryCleanupWorkCollector;
    private final String databaseName;
    private final PageCacheTracer pageCacheTracer;
    private final IndexStatisticsLayout layout;
    private final boolean readOnly;
    private GBPTree<IndexStatisticsKey, IndexStatisticsValue> tree;
    // Let IndexStatisticsValue be immutable in this map so that checkpoint doesn't have to coordinate with concurrent
    // writers. It's assumed that the data in this map will be so small that everything can just be in it always.
    private final ConcurrentHashMap<IndexStatisticsKey, IndexStatisticsValue> cache = new ConcurrentHashMap<>();

    public IndexStatisticsStore(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            DatabaseLayout databaseLayout,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        this(
                pageCache,
                fileSystem,
                databaseLayout.pathForStore(CommonDatabaseStores.INDEX_STATISTICS),
                recoveryCleanupWorkCollector,
                readOnly,
                databaseLayout.getDatabaseName(),
                contextFactory,
                pageCacheTracer,
                openOptions);
    }

    public IndexStatisticsStore(
            PageCache pageCache,
            FileSystemAbstraction fileSystem,
            Path path,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            boolean readOnly,
            String databaseName,
            CursorContextFactory contextFactory,
            PageCacheTracer pageCacheTracer,
            ImmutableSet<OpenOption> openOptions)
            throws IOException {
        this.pageCache = pageCache;
        this.fileSystem = fileSystem;
        this.path = path;
        this.recoveryCleanupWorkCollector = recoveryCleanupWorkCollector;
        this.databaseName = databaseName;
        this.pageCacheTracer = pageCacheTracer;
        this.layout = new IndexStatisticsLayout();
        this.readOnly = readOnly;
        initTree(contextFactory, openOptions);
    }

    private void initTree(CursorContextFactory contextFactory, ImmutableSet<OpenOption> openOptions)
            throws IOException {
        try {
            tree = new GBPTree<>(
                    pageCache,
                    fileSystem,
                    path,
                    layout,
                    GBPTree.NO_MONITOR,
                    GBPTree.NO_HEADER_READER,
                    recoveryCleanupWorkCollector,
                    readOnly,
                    openOptions.newWithout(PageCacheOpenOptions.MULTI_VERSIONED),
                    databaseName,
                    "Statistics store",
                    contextFactory,
                    pageCacheTracer);
            try (var cursorContext = contextFactory.create("indexStatisticScan")) {
                scanTree(cache::put, cursorContext);
            }
        } catch (TreeFileNotFoundException e) {
            throw new IllegalStateException(
                    "Index statistics store file could not be found, most likely this database needs to be recovered, file:"
                            + path,
                    e);
        }
    }

    /**
     * Incrementally add usage stats. If no previous usage statistics for given index exist, the added stats will be used
     * as is. If there is a current value, the added stats will be added to it like this:
     * - lastRead: use the highest time value (most resent in time) between current and added
     * - readCount: use the sum of current and added count
     * - trackedSince: use the lowest value (oldest in time) between current and added
     */
    @Override
    public void addUsageStats(long indexId, IndexUsageStats added) {
        var newValue = new IndexStatisticsValue();
        newValue.set(IndexStatisticsValue.INDEX_USAGE_LAST_READ, added.lastRead());
        newValue.set(IndexStatisticsValue.INDEX_USAGE_READ_COUNT, added.readCount());
        newValue.set(IndexStatisticsValue.INDEX_USAGE_TRACKED_SINCE, added.trackedSince());
        cache.compute(new IndexStatisticsKey(indexId, TYPE_USAGE), (key, currentValue) -> {
            if (currentValue != null) {
                newValue.set(
                        IndexStatisticsValue.INDEX_USAGE_READ_COUNT,
                        added.readCount() + currentValue.get(IndexStatisticsValue.INDEX_USAGE_READ_COUNT));
                newValue.set(
                        IndexStatisticsValue.INDEX_USAGE_TRACKED_SINCE,
                        Long.min(
                                currentValue.get(IndexStatisticsValue.INDEX_USAGE_TRACKED_SINCE),
                                added.trackedSince()));
                newValue.set(
                        IndexStatisticsValue.INDEX_USAGE_LAST_READ,
                        Long.max(currentValue.get(IndexStatisticsValue.INDEX_USAGE_LAST_READ), added.lastRead()));
            }
            return newValue;
        });
    }

    /**
     * If there is no recorded value for the given index, return empty IndexUsageStats (all values are 0).
     * How to interpret the result:
     * <pre>
     * if (stats.trackedSince() == 0) {
     *     // Values are missing
     * } else if (stats.lastRead() == 0) {
     *     // No reads registered yet
     * } else {
     *     // Real values exists for all fields
     * }
     * </pre>
     */
    public IndexUsageStats usageStats(long indexId) {
        return get(
                indexId,
                TYPE_USAGE,
                stats -> new IndexUsageStats(
                        stats.get(IndexStatisticsValue.INDEX_USAGE_LAST_READ),
                        stats.get(IndexStatisticsValue.INDEX_USAGE_READ_COUNT),
                        stats.get(IndexStatisticsValue.INDEX_USAGE_TRACKED_SINCE)));
    }

    public IndexSample indexSample(long indexId) {
        return get(
                indexId,
                TYPE_SAMPLE,
                stats -> new IndexSample(
                        stats.get(IndexStatisticsValue.INDEX_SAMPLE_INDEX_SIZE),
                        stats.get(IndexStatisticsValue.INDEX_SAMPLE_UNIQUE_VALUES),
                        stats.get(IndexStatisticsValue.INDEX_SAMPLE_SIZE),
                        stats.get(IndexStatisticsValue.INDEX_SAMPLE_UPDATES_COUNT)));
    }

    private <T> T get(long indexId, byte type, Function<IndexStatisticsValue, T> converter) {
        var value = cache.getOrDefault(new IndexStatisticsKey(indexId, type), EMPTY_STATISTICS);
        return converter.apply(value);
    }

    public void setSampleStats(long indexId, IndexSample sample) {
        var value = new IndexStatisticsValue();
        value.set(IndexStatisticsValue.INDEX_SAMPLE_UNIQUE_VALUES, sample.uniqueValues());
        value.set(IndexStatisticsValue.INDEX_SAMPLE_SIZE, sample.sampleSize());
        value.set(IndexStatisticsValue.INDEX_SAMPLE_UPDATES_COUNT, sample.updates());
        value.set(IndexStatisticsValue.INDEX_SAMPLE_INDEX_SIZE, sample.indexSize());
        cache.put(new IndexStatisticsKey(indexId, TYPE_SAMPLE), value);
    }

    public void removeIndex(long indexId) {
        cache.remove(new IndexStatisticsKey(indexId, TYPE_SAMPLE));
        cache.remove(new IndexStatisticsKey(indexId, TYPE_USAGE));
    }

    public void incrementIndexUpdates(long indexId, long delta) {
        cache.computeIfPresent(new IndexStatisticsKey(indexId, TYPE_SAMPLE), (key, existing) -> {
            var copy = existing.copy();
            copy.set(
                    IndexStatisticsValue.INDEX_SAMPLE_UPDATES_COUNT,
                    copy.get(IndexStatisticsValue.INDEX_SAMPLE_UPDATES_COUNT) + delta);
            return copy;
        });
    }

    @Override
    public void visit(IndexStatisticsVisitor visitor, CursorContext cursorContext) {
        try {
            scanTree(
                    (key, value) -> {
                        switch (key.getType()) {
                            case TYPE_SAMPLE -> visitor.visitSampleStatistics(
                                    key.getIndexId(),
                                    value.get(IndexStatisticsValue.INDEX_SAMPLE_UNIQUE_VALUES),
                                    value.get(IndexStatisticsValue.INDEX_SAMPLE_SIZE),
                                    value.get(IndexStatisticsValue.INDEX_SAMPLE_UPDATES_COUNT),
                                    value.get(IndexStatisticsValue.INDEX_SAMPLE_INDEX_SIZE));
                            case TYPE_USAGE -> visitor.visitUsageStatistics(
                                    key.getIndexId(),
                                    value.get(IndexStatisticsValue.INDEX_USAGE_LAST_READ),
                                    value.get(IndexStatisticsValue.INDEX_USAGE_READ_COUNT),
                                    value.get(IndexStatisticsValue.INDEX_USAGE_TRACKED_SINCE));
                            default -> throw new IllegalArgumentException("Unknown key type for " + key);
                        }
                    },
                    cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public void checkpoint(FileFlushEvent flushEvent, CursorContext cursorContext) throws IOException {
        // There's an assumption that there will never be concurrent calls to checkpoint. This is guarded outside.
        clearTree(cursorContext);
        writeCacheContentsIntoTree(cursorContext);
        tree.checkpoint(flushEvent, cursorContext);
    }

    @Override
    public boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        return tree.consistencyCheck(reporterFactory, contextFactory, numThreads, progressMonitorFactory);
    }

    private void scanTree(BiConsumer<IndexStatisticsKey, IndexStatisticsValue> consumer, CursorContext cursorContext)
            throws IOException {
        var high = layout.newKey();
        var low = layout.newKey();
        layout.initializeAsHighest(high);
        layout.initializeAsLowest(low);
        try (var seek = tree.seek(low, high, cursorContext)) {
            while (seek.next()) {
                var key = layout.copyKey(seek.key(), new IndexStatisticsKey());
                var value = seek.value().copy();
                consumer.accept(key, value);
            }
        }
    }

    private void clearTree(CursorContext cursorContext) throws IOException {
        // Read all keys from the tree, we can't do this while having a writer since it will grab write lock on pages
        List<IndexStatisticsKey> keys = new ArrayList<>(cache.size());
        scanTree((key, value) -> keys.add(key), cursorContext);

        // Remove all those read keys
        try (Writer<IndexStatisticsKey, IndexStatisticsValue> writer =
                tree.writer(W_BATCHED_SINGLE_THREADED, cursorContext)) {
            for (IndexStatisticsKey key : keys) {
                // Idempotent operation
                writer.remove(key);
            }
        }
    }

    private void writeCacheContentsIntoTree(CursorContext cursorContext) throws IOException {
        try (Writer<IndexStatisticsKey, IndexStatisticsValue> writer =
                tree.writer(W_BATCHED_SINGLE_THREADED, cursorContext)) {
            for (var entry : cache.entrySet()) {
                writer.put(entry.getKey(), entry.getValue());
            }
        }
    }

    public Path storeFile() {
        return path;
    }

    @Override
    public void shutdown() throws IOException {
        if (tree != null) {
            tree.close();
        }
    }
}
