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
import static org.neo4j.internal.helpers.collection.Iterators.asResourceIterator;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.function.IntFunction;
import java.util.function.LongPredicate;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.common.EntityType;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.index.internal.gbptree.Seeker;
import org.neo4j.internal.helpers.collection.BoundedIterable;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexAccessor;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.api.index.TokenIndexReader;
import org.neo4j.kernel.api.index.ValueIndexReader;
import org.neo4j.kernel.impl.api.index.IndexUpdateMode;
import org.neo4j.scheduler.JobScheduler;

public class TokenIndexAccessor extends TokenIndex implements IndexAccessor {
    private final EntityType entityType;
    private final NativeIndexHeaderWriter headerWriter = new NativeIndexHeaderWriter(ONLINE);

    public TokenIndexAccessor(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexDescriptor descriptor,
            RecoveryCleanupWorkCollector recoveryCleanupWorkCollector,
            ImmutableSet<OpenOption> openOptions,
            boolean readOnly,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        super(databaseIndexContext, indexFiles, descriptor, openOptions, readOnly, indexingBehaviour);

        entityType = descriptor.schema().entityType();
        instantiateTree(recoveryCleanupWorkCollector);
        instantiateUpdater();
    }

    @Override
    public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
        assertTreeOpen();
        assertWritable();
        try {
            if (parallel) {
                TokenIndexUpdater parallelUpdater = new TokenIndexUpdater(1_000, idLayout);
                return parallelUpdater.initialize(index.writer(cursorContext), true);
            } else {
                return singleUpdater.initialize(index.writer(W_BATCHED_SINGLE_THREADED, cursorContext), false);
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
            ProgressListener progress) {
        throw new UnsupportedOperationException();
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

    @Override
    public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
        index.checkpoint(headerWriter, flushEvent, cursorContext);
    }

    @Override
    public void refresh() {
        // not required in this implementation
    }

    @Override
    public void close() {
        closeResources();
    }

    @Override
    public ValueIndexReader newValueReader(IndexUsageTracker usageTracker) {
        throw new UnsupportedOperationException("Not applicable for token indexes ");
    }

    @Override
    public TokenIndexReader newTokenReader(IndexUsageTracker usageTracker) {
        assertTreeOpen();
        return new DefaultTokenIndexReader(index, usageTracker, idLayout);
    }

    @Override
    public BoundedIterable<EntityTokenRange> newAllEntriesTokenReader(
            long fromEntityId, long toEntityId, CursorContext cursorContext) {
        IntFunction<Seeker<TokenScanKey, TokenScanValue>> seekProvider = tokenId -> {
            try {
                return index.seek(
                        new TokenScanKey(tokenId, idLayout.rangeOf(fromEntityId)),
                        new TokenScanKey(tokenId, idLayout.rangeOf(toEntityId) + 1),
                        cursorContext);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        };

        int highestTokenId = findHighestTokenId(cursorContext);
        return new NativeAllEntriesTokenScanReader(seekProvider, highestTokenId, entityType, idLayout);
    }

    private int findHighestTokenId(CursorContext cursorContext) {
        try (var cursor = index.seek(
                new TokenScanKey(Integer.MAX_VALUE, Long.MAX_VALUE), new TokenScanKey(0, -1), cursorContext)) {
            if (cursor.next()) {
                return cursor.key().tokenId;
            }
            return -1;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public BoundedIterable<Long> newAllEntriesValueReader(
            long fromIdInclusive, long toIdExclusive, CursorContext cursorContext) {
        // This is just used for consistency checker and token indexes are not consistency checked the same way (not yet
        // anyway).
        throw new UnsupportedOperationException("Not applicable for token indexes");
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return asResourceIterator(iterator(indexFiles.getStoreFile()));
    }

    @Override
    public long estimateNumberOfEntries(CursorContext cursorContext) {
        // This is just used for consistency checker and token indexes are not consistency checked the same way (not yet
        // anyway).
        throw new UnsupportedOperationException("Not applicable for token indexes");
    }

    @Override
    public long sizeInBytes() {
        return index.sizeInBytes();
    }

    @Override
    public void drop() {
        index.setDeleteOnClose(true);
        closeResources();
        indexFiles.clear();
    }
}
