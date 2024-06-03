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
package org.neo4j.kernel.api.impl.index;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.LongPredicate;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.apache.lucene.document.Document;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.neo4j.annotations.documented.ReporterFactory;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.helpers.progress.ProgressMonitorFactory;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.SchemaDescriptorSupplier;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.impl.schema.LuceneIndexReaderAcquisitionException;
import org.neo4j.kernel.api.impl.schema.reader.LuceneAllEntriesIndexAccessorReader;
import org.neo4j.kernel.api.impl.schema.writer.LuceneIndexWriter;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntriesReader;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.kernel.impl.index.schema.IndexUpdateIgnoreStrategy;
import org.neo4j.kernel.impl.index.schema.IndexUsageTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public abstract class AbstractLuceneIndexAccessor<READER extends ValueIndexReader, INDEX extends DatabaseIndex<READER>>
        implements IndexAccessor {
    protected final LuceneIndexWriter writer;
    protected final INDEX luceneIndex;
    protected final IndexDescriptor descriptor;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;

    protected AbstractLuceneIndexAccessor(
            INDEX luceneIndex, IndexDescriptor descriptor, IndexUpdateIgnoreStrategy ignoreStrategy) {
        this.writer = luceneIndex.isPermanentlyOnly() ? null : luceneIndex.getIndexWriter();
        this.luceneIndex = luceneIndex;
        this.descriptor = descriptor;
        this.ignoreStrategy = ignoreStrategy;
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        if (luceneIndex.isReadOnly()) {
            throw new UnsupportedOperationException("Can't create index updater while database is in read only mode.");
        }
        return getIndexUpdater(mode);
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
        if (entityIdConverter != null) {
            throw new UnsupportedOperationException("Unable to modify document IDs");
        }

        var o = (AbstractLuceneIndexAccessor<READER, INDEX>) other;
        try {
            o.luceneIndex.accessClosedDirectories(writer::addDirectory);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
        refresh();

        if (entityFilter != null) {
            // If there's a filter then merge the index and then remove those that should be filtered out
            // TODO come on, make this parallel! Use the threads arg
            try (var reader = newAllEntriesValueReader(1, CursorContext.NULL_CONTEXT)[0];
                    var updater = newUpdater(IndexUpdateMode.ONLINE, CursorContext.NULL_CONTEXT, false)) {
                while (reader.hasNext()) {
                    var candidate = reader.next();
                    if (!entityFilter.test(candidate)) {
                        var values = new Value[descriptor.schema().getPropertyIds().length];
                        for (int i = 0; i < values.length; i++) {
                            values[i] = Values.stringValue("");
                        }
                        updater.process(IndexEntryUpdate.remove(candidate, descriptor, values));
                        progress.add(1);
                    }
                }
            }
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
        throw new UnsupportedOperationException();
    }

    protected abstract IndexUpdater getIndexUpdater(IndexUpdateMode mode);

    @Override
    public void drop() {
        if (luceneIndex.isReadOnly()) {
            throw new UnsupportedOperationException("Can't drop index while database is in read only mode.");
        }
        luceneIndex.drop();
    }

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
        try {
            // We never change status of read-only indexes.
            if (!luceneIndex.isReadOnly()) {
                luceneIndex.markAsOnline();
            }
            luceneIndex.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void refresh() {
        try {
            luceneIndex.maybeRefreshBlocking();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void close() {
        try {
            luceneIndex.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public READER newValueReader(IndexUsageTracker usageTracker) {
        try {
            return luceneIndex.getIndexReader(usageTracker);
        } catch (IOException e) {
            throw new LuceneIndexReaderAcquisitionException("Can't acquire index reader", e);
        }
    }

    protected BoundedIterable<Long> newAllEntriesReader(
            ToLongFunction<Document> entityIdReader, long fromIdInclusive, long toIdExclusive) {
        return new LuceneAllEntriesIndexAccessorReader(
                luceneIndex.allDocumentsReader(), entityIdReader, fromIdInclusive, toIdExclusive);
    }

    public IndexEntriesReader[] newAllEntriesValueReader(ToLongFunction<Document> entityIdReader, int numPartitions) {
        LuceneAllDocumentsReader allDocumentsReader = luceneIndex.allDocumentsReader();
        List<Iterator<Document>> partitions = allDocumentsReader.partition(numPartitions);
        AtomicInteger closeCount = new AtomicInteger(partitions.size());
        List<IndexEntriesReader> readers = partitions.stream()
                .map(partitionDocuments -> new PartitionIndexEntriesReader(
                        closeCount, allDocumentsReader, entityIdReader, partitionDocuments))
                .collect(Collectors.toList());
        return readers.toArray(IndexEntriesReader[]::new);
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() throws IOException {
        return luceneIndex.snapshotFiles();
    }

    @Override
    public boolean consistencyCheck(
            ReporterFactory reporterFactory,
            CursorContextFactory contextFactory,
            int numThreads,
            ProgressMonitorFactory progressMonitorFactory) {
        final LuceneIndexConsistencyCheckVisitor visitor =
                reporterFactory.getClass(LuceneIndexConsistencyCheckVisitor.class);
        final boolean isConsistent = luceneIndex.isValid();
        if (!isConsistent) {
            visitor.isInconsistent(descriptor);
        }
        return isConsistent;
    }

    @Override
    public long estimateNumberOfEntries(CursorContext ignored) {
        return luceneIndex.allDocumentsReader().maxCount();
    }

    private static class PartitionIndexEntriesReader implements IndexEntriesReader {
        private final AtomicInteger closeCount;
        private final LuceneAllDocumentsReader allDocumentsReader;
        private final ToLongFunction<Document> entityIdReader;
        private final Iterator<Document> partitionDocuments;

        PartitionIndexEntriesReader(
                AtomicInteger closeCount,
                LuceneAllDocumentsReader allDocumentsReader,
                ToLongFunction<Document> entityIdReader,
                Iterator<Document> partitionDocuments) {
            this.closeCount = closeCount;
            this.allDocumentsReader = allDocumentsReader;
            this.entityIdReader = entityIdReader;
            this.partitionDocuments = partitionDocuments;
        }

        @Override
        public Value[] values() {
            return null;
        }

        @Override
        public void close() {
            // Since all these (sub-range) readers come from the one LuceneAllDocumentsReader it will have to remain
            // open until the last reader is closed
            if (closeCount.decrementAndGet() == 0) {
                try {
                    allDocumentsReader.close();
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }

        @Override
        public long next() {
            return entityIdReader.applyAsLong(partitionDocuments.next());
        }

        @Override
        public boolean hasNext() {
            return partitionDocuments.hasNext();
        }
    }

    protected abstract class AbstractLuceneIndexUpdater implements IndexUpdater {
        private final boolean idempotent;
        private final Runnable refreshAction;

        private boolean hasChanges;

        protected AbstractLuceneIndexUpdater(boolean idempotent, boolean refresh) {
            this(idempotent, refresh ? AbstractLuceneIndexAccessor.this::refresh : () -> {});
        }

        protected AbstractLuceneIndexUpdater(boolean idempotent, Runnable refreshAction) {
            this.idempotent = idempotent;
            this.refreshAction = refreshAction;
        }

        @Override
        public void close() {
            if (hasChanges) {
                refreshAction.run();
            }
        }

        @Override
        public void process(IndexEntryUpdate<?> update) {
            assert update.indexKey().schema().equals(descriptor.schema());
            final var valueUpdate = asValueUpdate(update);

            // ignoreStrategy set update to null; ignore update
            if (valueUpdate == null) {
                return;
            }

            final var entityId = valueUpdate.getEntityId();
            final var values = valueUpdate.values();
            final var updateMode = valueUpdate.updateMode();
            switch (updateMode) {
                case ADDED -> {
                    if (idempotent) {
                        addIdempotent(entityId, values);
                    } else {
                        add(entityId, values);
                    }
                }
                case CHANGED -> change(entityId, values);
                case REMOVED -> remove(entityId);
            }

            hasChanges = true;
        }

        @Override
        public <INDEX_KEY extends SchemaDescriptorSupplier> ValueIndexEntryUpdate<INDEX_KEY> asValueUpdate(
                IndexEntryUpdate<INDEX_KEY> update) {
            final var valueUpdate = IndexUpdater.super.asValueUpdate(update);
            return !ignoreStrategy.ignore(valueUpdate) ? ignoreStrategy.toEquivalentUpdate(valueUpdate) : null;
        }

        protected abstract void addIdempotent(long entityId, Value[] values);

        protected abstract void add(long entityId, Value[] values);

        protected abstract void change(long entityId, Value[] values);

        protected abstract void remove(long entityId);
    }
}
