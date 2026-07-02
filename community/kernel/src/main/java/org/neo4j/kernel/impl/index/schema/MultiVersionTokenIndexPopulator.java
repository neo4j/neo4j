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
import static org.neo4j.kernel.impl.index.schema.TokenIndex.FAILED;
import static org.neo4j.kernel.impl.index.schema.TokenIndex.ONLINE;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Collection;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.IOUtils;
import org.neo4j.io.async.AsyncBlockAccessor;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.index.IndexEntryConflictHandler;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.kernel.impl.api.index.PhaseTracker;
import org.neo4j.kernel.impl.index.schema.MultiVersionTokenIndexUpdateStorage.Reader;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.TokenIndexEntryUpdate;
import org.neo4j.util.Preconditions;

public class MultiVersionTokenIndexPopulator implements IndexPopulator {
    private final TokenIndex tokenIndex;
    private final FileSystemAbstraction fs;
    private final MemoryTracker memoryTracker;
    private final IndexDescriptor descriptor;
    private MultiVersionTokenIndexUpdateStorage external;

    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    private volatile boolean scanCompleted;

    MultiVersionTokenIndexPopulator(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour,
            MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
        this.tokenIndex =
                new TokenIndex(databaseIndexContext, indexFiles, descriptor, openOptions, false, indexingBehaviour);
        this.fs = databaseIndexContext.fileSystem;
        this.descriptor = descriptor;
    }

    @Override
    public synchronized void create() {
        assertNotDropped();
        assertNotClosed();

        tokenIndex.indexFiles.clear();
        tokenIndex.instantiateTree(RecoveryCleanupWorkCollector.immediate());
        tokenIndex.instantiateUpdater();
        tokenIndex.indexFiles.ensureDirectoryExist();
        Path storeFile = tokenIndex.indexFiles.getStoreFile().baseSegment();
        Path externalUpdatesFile = storeFile.resolveSibling(storeFile.getFileName() + ".ext");
        external = new MultiVersionTokenIndexUpdateStorage(fs, externalUpdatesFile, memoryTracker);
    }

    @Override
    public synchronized void drop() {
        try {
            if (tokenIndex.index != null) {
                tokenIndex.index.setDeleteOnClose(true);
            }
            tokenIndex.closeResources();
            tokenIndex.indexFiles.clear();
        } finally {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add(Collection<? extends IndexEntryUpdate> updates, CursorContext cursorContext) {
        try (TokenIndexUpdater updater = tokenIndex.singleUpdater.initialize(
                context -> tokenIndex.index.writer(W_BATCHED_SINGLE_THREADED, context), false, cursorContext)) {
            for (IndexEntryUpdate update : updates) {
                updater.process(update);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        if (scanCompleted) {
            // update index in place
            try {
                return tokenIndex.singleUpdater.initialize(
                        context -> tokenIndex.index.writer(W_BATCHED_SINGLE_THREADED, context), false, cursorContext);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        // collect updates in order to apply them later
        return new IndexUpdater() {
            @Override
            public void process(IndexEntryUpdate update) {

                TokenIndexEntryUpdate tokenIndexEntryUpdate = (TokenIndexEntryUpdate) update;
                try {
                    external.add(
                            tokenIndexEntryUpdate.getEntityId(),
                            tokenIndexEntryUpdate.added(),
                            tokenIndexEntryUpdate.removed(),
                            cursorContext.getVersionContext().committingTransactionId());
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }

            @Override
            public void close() {}
        };
    }

    @Override
    public void scanCompleted(
            PhaseTracker phaseTracker,
            PopulationWorkScheduler populationWorkScheduler,
            IndexEntryConflictHandler conflictHandler,
            CursorContext cursorContext) {
        scanCompleted = true;
        try (CursorContext localContext = cursorContext.createUnboundedReadRelatedContext("TOKEN_POPULATION");
                TokenIndexUpdater updater = tokenIndex.singleUpdater.initialize(
                        context -> tokenIndex.index.writer(W_BATCHED_SINGLE_THREADED, context), false, localContext)) {
            try (Reader updatesReader = external.reader()) {
                while (updatesReader.next()) {
                    localContext.getVersionContext().initWrite(updatesReader.version);
                    TokenIndexEntryUpdate versionedUpdate = TokenIndexEntryUpdate.tokenChange(
                            updatesReader.entityId, descriptor, updatesReader.removed, updatesReader.added);
                    updater.process(versionedUpdate);
                }
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public synchronized void close(boolean populationCompletedSuccessfully, CursorContext cursorContext) {
        Preconditions.checkState(
                !(populationCompletedSuccessfully && failureBytes != null),
                "Can't mark index as online after it has been marked as failure");

        try {
            assertNotDropped();
            AsyncBlockAccessor asyncBlockAccessor = AsyncBlockAccessor.EMPTY_ASYNC_BLOCK_ACCESSOR;
            if (populationCompletedSuccessfully) {
                // Successful and completed population
                tokenIndex.assertTreeOpen();
                try (FileFlushEvent flushEvent = tokenIndex.pageCacheTracer.beginFileFlush()) {
                    flushTreeAndMarkAs(ONLINE, flushEvent, asyncBlockAccessor, cursorContext);
                }
            } else if (failureBytes != null) {
                // Failed population
                ensureTreeInstantiated();
                try (FileFlushEvent flushEvent = tokenIndex.pageCacheTracer.beginFileFlush()) {
                    markTreeAsFailed(flushEvent, asyncBlockAccessor, cursorContext);
                }
            }
            // else cancelled population. Here we simply close the tree w/o checkpointing it and it will look like
            // POPULATING state on next open
        } finally {
            IOUtils.closeAllUnchecked(external, tokenIndex::closeResources);
            closed = true;
        }
    }

    private void flushTreeAndMarkAs(
            byte state, FileFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext) {
        tokenIndex.index.checkpoint(
                pageCursor -> pageCursor.putByte(state), flushEvent, asyncBlockAccessor, cursorContext);
    }

    private void markTreeAsFailed(
            FileFlushEvent flushEvent, AsyncBlockAccessor asyncBlockAccessor, CursorContext cursorContext) {
        Preconditions.checkState(
                failureBytes != null, "markAsFailed hasn't been called, populator not actually failed?");
        tokenIndex.index.checkpoint(
                new FailureHeaderWriter(failureBytes, FAILED), flushEvent, asyncBlockAccessor, cursorContext);
    }

    @Override
    public void markAsFailed(String failure) {
        failureBytes = failure.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void includeSample(IndexEntryUpdate update) {
        // We don't do sampling for token indexes since that information is available in other ways.
    }

    @Override
    public IndexSample sample(CursorContext cursorContext) {
        throw new UnsupportedOperationException("Token indexes does not support index sampling");
    }

    private void assertNotDropped() {
        Preconditions.checkState(!dropped, "Populator has already been dropped.");
    }

    private void assertNotClosed() {
        Preconditions.checkState(!closed, "Populator has already been closed.");
    }

    private void ensureTreeInstantiated() {
        if (tokenIndex.index == null) {
            tokenIndex.instantiateTree(RecoveryCleanupWorkCollector.ignore());
        }
    }

    @Override
    public ResourceIterator<Path> snapshotFiles() {
        return tokenIndex.snapshotFiles();
    }
}
