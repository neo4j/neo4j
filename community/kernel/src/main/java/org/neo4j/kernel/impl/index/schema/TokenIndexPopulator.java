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

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.OpenOption;
import java.util.Collection;
import org.eclipse.collections.api.set.ImmutableSet;
import org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.internal.schema.StorageEngineIndexingBehaviour;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexPopulator;
import org.neo4j.kernel.api.index.IndexSample;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.util.Preconditions;

public class TokenIndexPopulator extends TokenIndex implements IndexPopulator {
    private byte[] failureBytes;
    private boolean dropped;
    private boolean closed;

    TokenIndexPopulator(
            DatabaseIndexContext databaseIndexContext,
            IndexFiles indexFiles,
            IndexDescriptor descriptor,
            ImmutableSet<OpenOption> openOptions,
            StorageEngineIndexingBehaviour indexingBehaviour) {
        super(databaseIndexContext, indexFiles, descriptor, openOptions, false, indexingBehaviour);
    }

    @Override
    public synchronized void create() {
        assertNotDropped();
        assertNotClosed();

        indexFiles.clear();
        instantiateTree(RecoveryCleanupWorkCollector.immediate());
        instantiateUpdater();
    }

    @Override
    public synchronized void drop() {
        try {
            if (index != null) {
                index.setDeleteOnClose(true);
            }
            closeResources();
            indexFiles.clear();
        } finally {
            dropped = true;
            closed = true;
        }
    }

    @Override
    public void add(Collection<? extends IndexEntryUpdate<?>> updates, CursorContext cursorContext)
            throws IndexEntryConflictException {
        try (TokenIndexUpdater updater =
                singleUpdater.initialize(index.writer(W_BATCHED_SINGLE_THREADED, cursorContext), false)) {
            for (IndexEntryUpdate<?> update : updates) {
                updater.process(update);
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public IndexUpdater newPopulatingUpdater(CursorContext cursorContext) {
        try {
            return singleUpdater.initialize(index.writer(W_BATCHED_SINGLE_THREADED, cursorContext), false);
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
            if (populationCompletedSuccessfully) {
                // Successful and completed population
                assertTreeOpen();
                try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                    flushTreeAndMarkAs(ONLINE, flushEvent, cursorContext);
                }
            } else if (failureBytes != null) {
                // Failed population
                ensureTreeInstantiated();
                try (var flushEvent = pageCacheTracer.beginFileFlush()) {
                    markTreeAsFailed(flushEvent, cursorContext);
                }
            }
            // else cancelled population. Here we simply close the tree w/o checkpointing it and it will look like
            // POPULATING state on next open
        } finally {
            closeResources();
            closed = true;
        }
    }

    private void flushTreeAndMarkAs(byte state, FileFlushEvent flushEvent, CursorContext cursorContext) {
        index.checkpoint(pageCursor -> pageCursor.putByte(state), flushEvent, cursorContext);
    }

    private void markTreeAsFailed(FileFlushEvent flushEvent, CursorContext cursorContext) {
        Preconditions.checkState(
                failureBytes != null, "markAsFailed hasn't been called, populator not actually failed?");
        index.checkpoint(new FailureHeaderWriter(failureBytes, FAILED), flushEvent, cursorContext);
    }

    @Override
    public void markAsFailed(String failure) {
        failureBytes = failure.getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void includeSample(IndexEntryUpdate<?> update) {
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
        if (index == null) {
            instantiateTree(RecoveryCleanupWorkCollector.ignore());
        }
    }
}
