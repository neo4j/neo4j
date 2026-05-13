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
import static org.neo4j.kernel.impl.index.schema.NativeIndexPopulator.BYTE_ONLINE;
import static org.neo4j.storageengine.api.EagerValueIndexEntryUpdate.add;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.Subject;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.index.internal.gbptree.TreeInconsistencyException;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.kernel.api.PropertyIndexQuery;
import org.neo4j.internal.kernel.api.QueryContext;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.IOUtils;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.scheduler.JobHandles;
import org.neo4j.scheduler.JobMonitoringParams;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.values.storable.Value;

public abstract class NativeIndexAccessor<KEY extends NativeIndexKey<KEY>> extends NativeIndex<KEY>
        implements IndexAccessor {
    private final NativeIndexUpdater<KEY> singleUpdater;
    private final NativeIndexHeaderWriter headerWriter;
    protected final LogProvider logProvider;
    protected final TokenNameLookup tokenNameLookup;

    NativeIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexLayout<KEY> layout,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            LogProvider logProvider,
            TokenNameLookup tokenNameLookup) {
        super(databaseIndexContext, layout, indexFiles, descriptor, openOptions, readOnly);
        this.tokenNameLookup = tokenNameLookup;
        singleUpdater = new NativeIndexUpdater<>(
                layout.newKey(),
                indexUpdateIgnoreStrategy(),
                new ThrowingConflictDetector<>(true, descriptor.schema(), tokenNameLookup));
        headerWriter = new NativeIndexHeaderWriter(BYTE_ONLINE);
        this.logProvider = logProvider;
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
                                        !descriptor.isUnique() || mode.includeEntityIdInUniqueness(),
                                        descriptor.schema(),
                                        tokenNameLookup))
                        .initialize(tree.writer(cursorContext));
            } else {
                assert mode.includeEntityIdInUniqueness();
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
        //noinspection unchecked
        NativeIndexAccessor<KEY> o = (NativeIndexAccessor<KEY>) other;
        IndexEntriesReader[] readers = o.newAllEntriesValueReader(threads, NULL_CONTEXT);
        try {
            List<JobHandle<Void>> handles = new ArrayList<>();
            int updaterFlags = readers.length == 1 ? W_BATCHED_SINGLE_THREADED : 0;
            for (IndexEntriesReader reader : readers) {
                handles.add(jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK,
                        new JobMonitoringParams(Subject.AUTH_DISABLED, databaseName, "insertFrom"),
                        () -> {
                            var merger = new ConflictDetectingValueMerger<KEY, Value[]>(!valueUniqueness) {
                                @Override
                                void doReportConflict(long existingNodeId, long addedNodeId, Value[] toReport)
                                        throws IndexEntryConflictException {
                                    switch (conflictHandler.indexEntryConflict(existingNodeId, addedNodeId, toReport)) {
                                        case THROW ->
                                            throw IndexEntryConflictException.indexEntryConflict(
                                                    descriptor.schema(),
                                                    existingNodeId,
                                                    addedNodeId,
                                                    tokenNameLookup,
                                                    toReport);
                                        case DELETE -> {
                                            /*then just skip it*/
                                        }
                                    }
                                }
                            };
                            try (NativeIndexUpdater<KEY> updater = new NativeIndexUpdater<>(
                                                    layout.newKey(), indexUpdateIgnoreStrategy(), merger)
                                            .initialize(tree.writer(updaterFlags, NULL_CONTEXT));
                                    ProgressListener localProgress = progress.threadLocalReporter()) {
                                while (reader.hasNext()) {
                                    long entityId = reader.next();
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
            JobHandles.getAllResults(handles, IndexEntryConflictException.class, RuntimeException::new);
        } finally {
            IOUtils.closeAllUnchecked(readers);
        }
    }

    @Override
    public void validate(
            IndexAccessor other,
            boolean valueUniqueness,
            IndexEntryConflictHandler conflictHandler,
            int threads,
            JobScheduler jobScheduler) {
        //noinspection unchecked
        NativeIndexAccessor<KEY> o = (NativeIndexAccessor<KEY>) other;
        IndexEntriesReader[] readers = o.newAllEntriesValueReader(threads, NULL_CONTEXT);
        try {
            List<JobHandle<Void>> handles = new ArrayList<>();
            for (IndexEntriesReader fromReader : readers) {
                handles.add(jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK,
                        new JobMonitoringParams(Subject.AUTH_DISABLED, databaseName, "insertFrom"),
                        () -> {
                            try (ValueIndexReader reader = newValueReader(IndexUsageTracking.NO_USAGE_TRACKING)) {
                                int[] propertyKeyIds = descriptor.schema().getPropertyIds();
                                while (fromReader.hasNext()) {
                                    long entityId = fromReader.next();
                                    Value[] values = fromReader.values();
                                    PropertyIndexQuery[] queries = new PropertyIndexQuery[values.length];
                                    for (int i = 0; i < queries.length; i++) {
                                        queries[i] = exact(propertyKeyIds[i], values[i]);
                                    }
                                    try (NodeValueIterator client = new NodeValueIterator()) {
                                        reader.query(
                                                client,
                                                QueryContext.NULL_CONTEXT,
                                                CursorContext.NULL_CONTEXT,
                                                unconstrained(),
                                                queries);
                                        if (client.hasNext()) {
                                            long existingEntityId = client.next();
                                            conflictHandler.indexEntryConflict(existingEntityId, entityId, values);
                                        }
                                    }
                                }
                            }
                            return null;
                        }));
            }
            JobHandles.getAllResults(handles, RuntimeException.class, RuntimeException::new);
        } finally {
            IOUtils.closeAllUnchecked(readers);
        }
    }

    @Override
    public void validateShards(
            Iterable<IndexAccessor> otherShards,
            boolean valueUniqueness,
            ShardedIndexEntryConflictHandler conflictHandler,
            int threads,
            JobScheduler jobScheduler) {
        List<NativeIndexAccessor<KEY>> allShards = new ArrayList<>();
        allShards.add(this);
        for (IndexAccessor shard : otherShards) {
            //noinspection unchecked
            allShards.add((NativeIndexAccessor<KEY>) shard);
        }

        try {
            List<JobHandle<Void>> handles = new ArrayList<>();
            List<KEY> partitionEdges = tree.partitionedSeek(lowestKey(), highestKey(), threads, NULL_CONTEXT);
            for (int p = 0; p < partitionEdges.size() - 1; p++) {
                KEY from = layout.copyKey(partitionEdges.get(p));
                KEY to = layout.copyKey(partitionEdges.get(p + 1));
                handles.add(jobScheduler.schedule(
                        Group.INDEX_POPULATION_WORK,
                        new JobMonitoringParams(Subject.AUTH_DISABLED, databaseName, "validateShard"),
                        () -> {
                            IndexValueIterator[] readers = new IndexValueIterator[allShards.size()];
                            for (int i = 0; i < allShards.size(); i++) {
                                NativeIndexAccessor<KEY> accessor = allShards.get(i);
                                Seeker<KEY, NullValue> seeker = accessor.tree.seek(from, to, NULL_CONTEXT);
                                readers[i] = new IndexValueIterator(accessor, new NativeIndexEntriesReader(seeker));
                            }
                            validateShardData(conflictHandler, readers);
                            return null;
                        }));
            }
            JobHandles.getAllResults(handles, RuntimeException.class, RuntimeException::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private static void validateShardData(
            ShardedIndexEntryConflictHandler conflictHandler, IndexValueIterator[] readers) {
        try {
            while (true) {
                int lowestShardIndex = -1;
                for (int shardIndex = 0; shardIndex < readers.length; shardIndex++) {
                    IndexValueIterator reader = readers[shardIndex];
                    if (reader.isExhausted()) {
                        continue;
                    }

                    if (lowestShardIndex == -1) {
                        lowestShardIndex = shardIndex;
                    } else {
                        int comparison = reader.reader.compareCurrentValues(readers[lowestShardIndex].reader);
                        if (comparison < 0) {
                            lowestShardIndex = shardIndex;
                        }
                    }
                }
                if (lowestShardIndex == -1) {
                    break;
                }

                for (int shardIndex = 0; shardIndex < readers.length; shardIndex++) {
                    IndexValueIterator reader = readers[shardIndex];
                    if (reader.isExhausted() || shardIndex == lowestShardIndex) {
                        continue;
                    }
                    IndexValueIterator lowest = readers[lowestShardIndex];
                    if (reader.reader.compareCurrentValues(lowest.reader) == 0) {
                        conflictHandler.indexEntryConflict(
                                lowest.currentEntityId,
                                lowest.accessor,
                                reader.currentEntityId,
                                reader.accessor,
                                reader.reader.values());
                        reader.next();
                    }
                }
                readers[lowestShardIndex].next();
            }
        } finally {
            IOUtils.closeAllUnchecked(readers);
        }
    }

    /**
     * {@link IndexUpdateIgnoreStrategy Ignore strategy} to be used by index updater.
     * Sub-classes are expected to override this method if they want to use something
     * other than {@link IndexUpdateIgnoreStrategy#NO_IGNORE}.
     *
     * @return {@link IndexUpdateIgnoreStrategy} to be used by index updater.
     */
    protected IndexUpdateIgnoreStrategy indexUpdateIgnoreStrategy() {
        return IndexUpdateIgnoreStrategy.NO_IGNORE;
    }

    @Override
    public void force(FileFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext) {
        tree.checkpoint(headerWriter, flushEvent, asyncBlockAccessor, cursorContext);
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
    public abstract ValueIndexReader newValueReader(IndexUsageTracking usageTracker);

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
        try {
            List<KEY> partitionEdges = tree.partitionedSeek(lowestKey(), highestKey(), partitions, cursorContext);
            Collection<IndexEntriesReader> readers = new ArrayList<>();
            for (int i = 0; i < partitionEdges.size() - 1; i++) {
                Seeker<KEY, NullValue> seeker = tree.seek(
                        layout.copyKey(partitionEdges.get(i)),
                        layout.copyKey(partitionEdges.get(i + 1)),
                        cursorContext);
                readers.add(new NativeIndexEntriesReader(seeker));
            }
            return readers.toArray(IndexEntriesReader[]::new);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexEntriesReader newAllEntriesValueReader(Value[] from, Value[] to, CursorContext cursorContext) {
        KEY fromKey = layout.newKey();
        fromKey.initialize(Long.MIN_VALUE);
        if (from == null) {
            fromKey.initValuesAsLowest();
        } else {
            for (int i = 0; i < from.length; i++) {
                fromKey.initFromValue(i, from[i], NativeIndexKey.Inclusion.NEUTRAL);
            }
        }

        KEY toKey = layout.newKey();
        toKey.initialize(Long.MIN_VALUE);
        if (to == null) {
            toKey.initValuesAsHighest();
        } else {
            for (int i = 0; i < to.length; i++) {
                toKey.initFromValue(i, to[i], NativeIndexKey.Inclusion.NEUTRAL);
            }
        }

        try {
            return new NativeIndexEntriesReader(tree.seek(fromKey, toKey, cursorContext));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private KEY highestKey() {
        KEY highest = layout.newKey();
        highest.initialize(Long.MAX_VALUE);
        highest.initValuesAsHighest();
        return highest;
    }

    private KEY lowestKey() {
        KEY lowest = layout.newKey();
        lowest.initialize(Long.MIN_VALUE);
        lowest.initValuesAsLowest();
        return lowest;
    }

    private class NativeIndexEntriesReader implements IndexEntriesReader {
        private final Seeker<KEY, NullValue> seeker;

        public NativeIndexEntriesReader(Seeker<KEY, NullValue> seeker) {
            this.seeker = seeker;
        }

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
        public int compareCurrentValues(IndexEntriesReader other) {
            //noinspection unchecked
            NativeIndexEntriesReader otherReader = (NativeIndexEntriesReader) other;
            return layout.compareValue(seeker.key(), otherReader.seeker.key());
        }

        @Override
        public void close() {
            try {
                seeker.close();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private static class IndexValueIterator implements AutoCloseable {
        private final IndexAccessor accessor;
        private final IndexEntriesReader reader;

        private boolean isExhausted;
        private long currentEntityId;

        IndexValueIterator(IndexAccessor accessor, IndexEntriesReader reader) {
            this.accessor = accessor;
            this.reader = reader;
            next();
        }

        boolean isExhausted() {
            return isExhausted;
        }

        void next() {
            if (isExhausted) {
                return;
            }

            if (reader.hasNext()) {
                currentEntityId = reader.next();
            } else {
                isExhausted = true;
            }
        }

        @Override
        public void close() {
            reader.close();
        }
    }
}
