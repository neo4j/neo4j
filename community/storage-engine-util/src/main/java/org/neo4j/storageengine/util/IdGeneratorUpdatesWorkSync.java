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

import static org.neo4j.internal.id.IdUtils.combinedIdAndNumberOfIds;
import static org.neo4j.internal.id.IdUtils.idFromCombinedId;
import static org.neo4j.internal.id.IdUtils.numberOfIdsFromCombinedId;
import static org.neo4j.internal.id.IdUtils.usedFromCombinedId;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.util.concurrent.AsyncApply;
import org.neo4j.util.concurrent.Work;
import org.neo4j.util.concurrent.WorkSync;

/**
 * Convenience for updating one or more {@link IdGenerator} in a concurrent fashion. Supports applying in batches, e.g. multiple transactions
 * in one go, see {@link #newBatch(CursorContext)}.
 */
public class IdGeneratorUpdatesWorkSync {
    public static final String ID_GENERATOR_BATCH_APPLIER_TAG = "idGeneratorBatchApplier";

    private final Map<IdGenerator, WorkSync<IdGenerator, IdGeneratorUpdateWork>> workSyncMap = new HashMap<>();
    private final boolean alwaysFreeOnDelete;

    public IdGeneratorUpdatesWorkSync() {
        this(false);
    }

    public IdGeneratorUpdatesWorkSync(boolean alwaysFreeOnDelete) {
        this.alwaysFreeOnDelete = alwaysFreeOnDelete;
    }

    public void add(IdGenerator idGenerator) {
        this.workSyncMap.put(idGenerator, new WorkSync<>(idGenerator));
    }

    public Batch newBatch(CursorContext cursorContext) {
        return new Batch(cursorContext);
    }

    public class Batch implements IdUpdateListener {
        private final Map<IdGenerator, ChangedIds> idUpdatesMap = new HashMap<>();
        private final CursorContext cursorContext;

        protected Batch(CursorContext cursorContext) {
            this.cursorContext = cursorContext;
        }

        @Override
        public void markIdAsUsed(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            idUpdatesMap.computeIfAbsent(idGenerator, this::createChangedIds).addUsedId(id, size);
        }

        @Override
        public void markIdAsUnused(IdGenerator idGenerator, long id, int size, CursorContext cursorContext) {
            idUpdatesMap.computeIfAbsent(idGenerator, this::createChangedIds).addUnusedId(id, size);
        }

        public AsyncApply applyAsync() {
            // Run through the id changes and apply them, or rather apply them asynchronously.
            // This allows multiple concurrent threads applying batches of transactions to help each other out so that
            // there's a higher chance that changes to different id types can be applied in parallel.
            if (idUpdatesMap.isEmpty()) {
                return AsyncApply.EMPTY;
            }
            applyInternal();
            return this::awaitApply;
        }

        public void apply() throws ExecutionException {
            if (!idUpdatesMap.isEmpty()) {
                applyInternal();
                awaitApply();
            }
        }

        private void awaitApply() throws ExecutionException {
            // Wait for all id updates to complete
            for (Map.Entry<IdGenerator, ChangedIds> idChanges : idUpdatesMap.entrySet()) {
                ChangedIds unit = idChanges.getValue();
                unit.awaitApply();
            }
        }

        private void applyInternal() {
            for (Map.Entry<IdGenerator, ChangedIds> idChanges : idUpdatesMap.entrySet()) {
                ChangedIds unit = idChanges.getValue();
                unit.applyAsync(workSyncMap.get(idChanges.getKey()));
            }
        }

        @Override
        public void close() throws Exception {
            apply();
        }

        private ChangedIds createChangedIds(IdGenerator ignored) {
            return new ChangedIds(alwaysFreeOnDelete, cursorContext);
        }
    }

    private static class ChangedIds {
        // The order in which IDs come in, used vs. unused must be kept and therefore all IDs must reside in the same
        // list
        private final MutableLongList ids = LongLists.mutable.empty();
        private final boolean freeOnDelete;
        private final CursorContext cursorContext;
        private AsyncApply asyncApply;

        ChangedIds(boolean freeOnDelete, CursorContext cursorContext) {
            this.freeOnDelete = freeOnDelete;
            this.cursorContext = cursorContext;
        }

        private void addUsedId(long id, int numberOfIds) {
            ids.add(combinedIdAndNumberOfIds(id, numberOfIds, true));
        }

        private void addUnusedId(long id, int numberOfIds) {
            ids.add(combinedIdAndNumberOfIds(id, numberOfIds, false));
        }

        void accept(IdGenerator.TransactionalMarker visitor) {
            ids.forEach(combined -> {
                long id = idFromCombinedId(combined);
                int slots = numberOfIdsFromCombinedId(combined);
                if (usedFromCombinedId(combined)) {
                    visitor.markUsed(id, slots);
                } else {
                    if (freeOnDelete) {
                        visitor.markDeletedAndFree(id, slots);
                    } else {
                        visitor.markDeleted(id, slots);
                    }
                }
            });
        }

        void applyAsync(WorkSync<IdGenerator, IdGeneratorUpdateWork> workSync) {
            asyncApply = workSync.applyAsync(new IdGeneratorUpdateWork(this));
        }

        void awaitApply() throws ExecutionException {
            asyncApply.await();
        }
    }

    private static class IdGeneratorUpdateWork implements Work<IdGenerator, IdGeneratorUpdateWork> {
        private final List<ChangedIds> changeList = new ArrayList<>();

        IdGeneratorUpdateWork(ChangedIds changes) {
            this.changeList.add(changes);
        }

        @Override
        public IdGeneratorUpdateWork combine(IdGeneratorUpdateWork work) {
            changeList.addAll(work.changeList);
            return this;
        }

        @Override
        public void apply(IdGenerator idGenerator) {
            for (ChangedIds changes : this.changeList) {
                // work units are applied in parallel and shouldn't share the same context
                try (var marker = idGenerator.transactionalMarker(
                        changes.cursorContext.createRelatedContext(ID_GENERATOR_BATCH_APPLIER_TAG))) {
                    changes.accept(marker);
                }
            }
        }
    }
}
