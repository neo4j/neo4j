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
package org.neo4j.storageengine.util;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;
import org.neo4j.exceptions.KernelException;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.internal.helpers.collection.NestingIterator;
import org.neo4j.internal.schema.IndexDescriptor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.IndexUpdateListener;
import org.neo4j.storageengine.api.IndexUpdatesListener;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

public class IndexUpdatesWorkSync {
    private final WorkSync<IndexUpdateListener, IndexUpdatesWork> workSync;
    private final IndexUpdateListener listener;
    private final boolean parallelApply;

    /**
     * @param parallelApply if {@code false} the updates from multiple concurrent applying transactions are work-synced where one thread
     * will end up applying all the updates. Otherwise if {@code true} each thread will apply their updates itself, with the "parallel" note
     * passed down to the updaters to arrange for this fact.
     */
    public IndexUpdatesWorkSync(IndexUpdateListener listener, boolean parallelApply) {
        this.listener = listener;
        this.parallelApply = parallelApply;
        this.workSync = parallelApply ? null : new WorkSync<>(listener);
    }

    public Batch newBatch(CursorContext cursorContext) {
        return new Batch(cursorContext);
    }

    public class Batch implements IndexUpdatesListener {
        private final List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates = new ArrayList<>();
        private final CursorContext cursorContext;
        private List<IndexEntryUpdate<IndexDescriptor>> singleUpdates;
        private AsyncApply apply;

        public Batch(CursorContext cursorContext) {
            this.cursorContext = cursorContext;
        }

        @Override
        public void indexUpdates(Iterable<IndexEntryUpdate<IndexDescriptor>> indexUpdates) {
            updates.add(indexUpdates);
        }

        @Override
        public void indexUpdate(IndexEntryUpdate<IndexDescriptor> indexUpdate) {
            if (singleUpdates == null) {
                singleUpdates = new ArrayList<>();
            }
            singleUpdates.add(indexUpdate);
        }

        private void addSingleUpdates() {
            if (singleUpdates != null) {
                updates.add(singleUpdates);
            }
        }

        @Override
        public void close() throws IOException {
            try {
                if (apply == null) {
                    apply();
                }
                apply.await();
            } catch (ExecutionException e) {
                throw wrapExecutionException(e);
            }
        }

        private IOException wrapExecutionException(ExecutionException e) {
            return e.getCause() instanceof IOException ioe ? ioe : new IOException(e.getCause());
        }

        private void apply() throws IOException, ExecutionException {
            addSingleUpdates();
            if (!updates.isEmpty()) {
                if (parallelApply) {
                    // Just skip the work-sync if this is parallel apply and instead update straight in
                    try {
                        listener.applyUpdates(combinedUpdates(updates), cursorContext, true);
                    } catch (KernelException e) {
                        throw new IOException(e);
                    }
                } else {
                    workSync.apply(new IndexUpdatesWork(combinedUpdates(updates), cursorContext));
                }
            }
            apply = AsyncApply.EMPTY;
        }

        @Override
        public void applyAsync() throws IOException {
            if (apply != null) {
                throw new IllegalStateException("Already applied");
            }

            if (!parallelApply) {
                addSingleUpdates();
                apply = !updates.isEmpty()
                        ? workSync.applyAsync(new IndexUpdatesWork(combinedUpdates(updates), cursorContext))
                        : AsyncApply.EMPTY;
                return;
            }
            try {
                apply();
            } catch (ExecutionException e) {
                throw wrapExecutionException(e);
            }
        }
    }

    /**
     * Combines index updates from multiple transactions into one bigger job.
     */
    private static class IndexUpdatesWork implements Work<IndexUpdateListener, IndexUpdatesWork> {
        record OneWork(Iterable<IndexEntryUpdate<IndexDescriptor>> updates, CursorContext cursorContext) {}

        private final List<OneWork> works = new ArrayList<>(1);

        IndexUpdatesWork(Iterable<IndexEntryUpdate<IndexDescriptor>> updates, CursorContext cursorContext) {
            works.add(new OneWork(updates, cursorContext));
        }

        @Override
        public IndexUpdatesWork combine(IndexUpdatesWork work) {
            works.addAll(work.works);
            return this;
        }

        @Override
        public void apply(IndexUpdateListener material) {
            try {
                for (OneWork work : works) {
                    material.applyUpdates(work.updates, work.cursorContext, false);
                }
            } catch (IOException | KernelException e) {
                throw new UnderlyingStorageException(e);
            }
        }
    }

    private static Iterable<IndexEntryUpdate<IndexDescriptor>> combinedUpdates(
            List<Iterable<IndexEntryUpdate<IndexDescriptor>>> updates) {
        return () -> new NestingIterator<>(updates.iterator()) {
            @Override
            protected Iterator<IndexEntryUpdate<IndexDescriptor>> createNestedIterator(
                    Iterable<IndexEntryUpdate<IndexDescriptor>> item) {
                return item.iterator();
            }
        };
    }
}
