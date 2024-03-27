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
package org.neo4j.kernel.impl.index.schema;

import static org.neo4j.index.internal.gbptree.DataTree.W_BATCHED_SINGLE_THREADED;
import static org.neo4j.internal.kernel.api.IndexQueryConstraints.unconstrained;
import static org.neo4j.internal.kernel.api.PropertyIndexQuery.exact;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.index.schema.IndexUsageTracker.NO_USAGE_TRACKER;
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;
import static org.neo4j.storageengine.api.IndexEntryUpdate.add;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.Subject;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.storable.Value;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>> extends NativeIndex<KEY>
        implements IndexAccessor {
    private final NativeIndexUpdater<KEY> singleUpdater;
    private final NativeIndexHeaderWriter headerWriter;

    NativeIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<KEY> layout,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly) {
        super(databaseIndexContext, layout, indexFiles, descriptor, openOptions, readOnly);
        singleUpdater = new NativeIndexUpdater<>(
                layout.newKey(),
                indexUpdateIgnoreStrategy(),
                new ThrowingConflictDetector<>(true, descriptor.schema().entityType()));
        headerWriter = new NativeIndexHeaderWriter(BYTE_ONLINE);
    }

    @Override
    public void drop() {
        tree.setDeleteOnClose(true);
        closeTree();
        indexFiles.clear();
    }

    @Override
    public NativeIndexUpdater<KEY> newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        assertOpen();
        assertWritable();
        try {
            if (parallel) {
                return new NativeIndexUpdater<>(
                                layout.newKey(),
                                indexUpdateIgnoreStrategy(),
                                new ThrowingConflictDetector<>(
                                        true, descriptor.schema().entityType()))
                        .initialize(tree.writer(cursorContext));
            } else {
                return singleUpdater.initialize(tree.writer(W_BATCHED_SINGLE_THREADED, cursorContext));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void insertFrom(
            IndexAccessor other,
            LongToLongFunction entityIdConverter,
            boolean valueUniqueness,
            IndexEntryConflictHandler conflictHandler,
            LongPredicate entityFilter,
            int threads,
            JobScheduler jobScheduler,
            ProgressListener progress)
            throws IndexEntryConflictException {
        var o = (NativeIndexAccessor<KEY>) other;
        var readers = o.newAllEntriesValueReader(threads, NULL_CONTEXT);
        try {
            List<JobHandle<?>> handles = new ArrayList<>();
            var updaterFlags = readers.length == 1 ? W_BATCHED_SINGLE_THREADED : 0;
            for (var reader : readers) {
                handles.add(jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK,
                        new JobMonitoringParams(Subject.AUTH_DISABLED, databaseName, "insertFrom"),
                        () -> {
                            var merger = new ConflictDetectingValueMerger<KEY, Value[]>(!valueUniqueness) {
                                @Override
                                void doReportConflict(long existingNodeId, long addedNodeId, Value[] toReport)
                                        throws IndexEntryConflictException {
                                    switch (conflictHandler.indexEntryConflict(existingNodeId, addedNodeId, toReport)) {
                                        case THROW -> throw new IndexEntryConflictException(
                                                descriptor.schema().entityType(),
                                                existingNodeId,
                                                addedNodeId,
                                                toReport);
                                        case DELETE -> {
                                            /*then just skip it*/
                                        }
                                    }
                                }
                            };
                            try (var updater = new NativeIndexUpdater<>(
                                                    layout.newKey(), indexUpdateIgnoreStrategy(), merger)
                                            .initialize(tree.writer(updaterFlags, NULL_CONTEXT));
                                    var localProgress = progress.threadLocalReporter()) {
                                while (reader.hasNext()) {
                                    var entityId = reader.next();
                                    if (entityFilter == null || entityFilter.test(entityId)) {
                                        if (entityIdConverter != null) {
                                            entityId = entityIdConverter.applyAsLong(entityId);
                                        }
                                        updater.process(add(entityId, descriptor, reader.values()));
                                    }
                                    localProgress.add(1);
                                }
                            }
                            return null;
                        }));
            }

            var e = awaitCompletionOfAll(handles);
            if (e instanceof IndexEntryConflictException exception) {
                throw exception;
            } else if (e instanceof RuntimeException exception) {
                throw exception;
            } else if (e != null) {
                throw new RuntimeException(e);
            }
        } finally {
            IOUtils.closeAllUnchecked(readers);
        }
    }

    @Override
    public void validate(
            IndexAccessor other,
            boolean valueUniqueness,
            IndexEntryConflictHandler conflictHandler,
            LongPredicate entityFilter,
            int threads,
            JobScheduler jobScheduler) {
        var o = (NativeIndexAccessor<KEY>) other;
        var readers = o.newAllEntriesValueReader(threads, NULL_CONTEXT);
        try {
            List<JobHandle<?>> handles = new ArrayList<>();
            for (var fromReader : readers) {
                handles.add(jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK,
                        new JobMonitoringParams(Subject.AUTH_DISABLED, databaseName, "insertFrom"),
                        () -> {
                            try (var reader = newValueReader(NO_USAGE_TRACKER)) {
                                var propertyKeyIds = descriptor.schema().getPropertyIds();
                                while (fromReader.hasNext()) {
                                    var entityId = fromReader.next();
                                    var values = fromReader.values();
                                    var queries = new PropertyIndexQuery[values.length];
                                    for (var i = 0; i < queries.length; i++) {
                                        queries[i] = exact(propertyKeyIds[i], values[i]);
                                    }
                                    try (var client = new NodeValueIterator()) {
                                        reader.query(client, QueryContext.NULL_CONTEXT, unconstrained(), queries);
                                        if (client.hasNext()) {
                                            var existingEntityId = client.next();
                                            conflictHandler.indexEntryConflict(existingEntityId, entityId, values);
                                        }
                                    }
                                }
                            }
                            return null;
                        }));
            }

            var e = awaitCompletionOfAll(handles);
            if (e instanceof RuntimeException exception) {
                throw exception;
            } else if (e != null) {
                throw new RuntimeException(e);
            }
        } finally {
            IOUtils.closeAllUnchecked(readers);
        }
    }

    private Throwable awaitCompletionOfAll(List<JobHandle<?>> handles) {
        Throwable e = null;
        for (var handle : handles) {
            try {
                handle.get();
            } catch (ExecutionException ex) {
                e = Exceptions.chain(e, ex.getCause());
            } catch (InterruptedException ex) {
                e = Exceptions.chain(e, ex);
            }
        }
        return e;
    }

    /**
     * {@link IndexUpdateIgnoreStrategy Ignore strategy} to be used by index updater.
     * Sub-classes are expected to override this method if they want to use something
     * other than {@link IndexUpdateIgnoreStrategy#NO_IGNORE}.
     * @return {@link IndexUpdateIgnoreStrategy} to be used by index updater.
     */
    protected IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy() {
        return IndexUpdateIgnoreStrategy.NO_IGNORE;
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
        tree.checkpoint(headerWriter, flushEvent, cursorContext);
    }

    @Override
    public void refresh() {
        // not required in this implementation
    }

    @Override
    public void close() {
        closeTree();
    }

    @Override
    public abstract ValueIndexReader newValueReader(IndexUsageTracker usageTracker);

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        return new NativeAllEntriesReader<>(tree, layout, fromIdInclusive, toIdExclusive, cursorContext);
    }

    @Override
    public long estimateNumberOfEntries(CursorContext cursorContext) {
        try {
            return tree.estimateNumberOfEntriesInTree(cursorContext);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (TreeInconsistencyException e) {
            return UNKNOWN_NUMBER_OF_ENTRIES;
        }
    }

    @Override
    public long sizeInBytes() {
        return tree.sizeInBytes();
    }

    @Override
    public IndexEntriesReader[] newAllEntriesValueReader(int partitions, CursorContext cursorContext) {
        KEY lowest = layout.newKey();
        lowest.initialize(Long.MIN_VALUE);
        lowest.initValuesAsLowest();
        KEY highest = layout.newKey();
        highest.initialize(Long.MAX_VALUE);
        highest.initValuesAsHighest();
        try {
            List<KEY> partitionEdges = tree.partitionedSeek(lowest, highest, partitions, cursorContext);
            Collection<IndexEntriesReader> readers = new ArrayList<>();
            for (int i = 0; i < partitionEdges.size() - 1; i++) {
                Seeker<KEY, NullValue> seeker =
                        tree.seek(partitionEdges.get(i), partitionEdges.get(i + 1), cursorContext);
                readers.add(new IndexEntriesReader() {
                    @Override
                    public long next() {
                        return seeker.key().getEntityId();
                    }

                    @Override
                    public boolean hasNext() {
                        try {
                            return seeker.next();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }

                    @Override
                    public Value[] values() {
                        return seeker.key().asValues();
                    }

                    @Override
                    public void close() {
                        try {
                            seeker.close();
                        } catch (IOException e) {
                            throw new UncheckedIOException(e);
                        }
                    }
                });
            }
            return readers.toArray(IndexEntriesReader[]::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }
}
