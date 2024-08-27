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
package org.neo4j.internal.recordstorage;

import static org.neo4j.util.Preconditions.checkState;

import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.neo4j.io.IOUtils;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageEngineTransaction;

/**
 * Performs record-local consistency checking when applying commands. For simplicity the checking is done after records have been applied,
 * since doing it before would require more conditions and even distinction between recovery and online, where recovery would be even trickier.
 *
 * Currently only relationship chain checking is checked.
 */
class ConsistencyCheckingApplierFactory implements TransactionApplierFactory {
    private final NeoStores neoStores;

    ConsistencyCheckingApplierFactory(NeoStores neoStores) {
        this.neoStores = neoStores;
    }

    @Override
    public TransactionApplier startTx(StorageEngineTransaction transaction, BatchContext batchContext) {
        return new ConsistencyCheckingApplier(neoStores, transaction.cursorContext(), batchContext.memoryTracker());
    }

    static class ConsistencyCheckingApplier extends TransactionApplier.Adapter {
        private final MutableLongSet touchedRelationshipIds = LongSets.mutable.empty();
        private final RecordRelationshipScanCursor cursor;
        private final RecordRelationshipScanCursor otherCursor;
        private final CachedStoreCursors storeCursors;

        ConsistencyCheckingApplier(NeoStores neoStores, CursorContext cursorContext, MemoryTracker memoryTracker) {
            RelationshipStore relationshipStore = neoStores.getRelationshipStore();
            storeCursors = new CachedStoreCursors(neoStores, cursorContext);
            cursor = new RecordRelationshipScanCursor(relationshipStore, cursorContext, storeCursors, memoryTracker);
            otherCursor =
                    new RecordRelationshipScanCursor(relationshipStore, cursorContext, storeCursors, memoryTracker);
        }

        @Override
        public boolean visitRelationshipCommand(Command.RelationshipCommand command) {
            touchedRelationshipIds.add(command.getKey());
            return false;
        }

        @Override
        public void close() throws Exception {
            MutableLongIterator ids = touchedRelationshipIds.longIterator();
            while (ids.hasNext()) {
                long id = ids.next();
                checkRelationship(id);
            }
            IOUtils.closeAll(cursor, otherCursor, storeCursors);
        }

        private void checkRelationship(long id) {
            cursor.single(id);
            if (!cursor.next()) {
                // It was deleted, so don't check it
                return;
            }

            checkPrevPointer(cursor, cursor.isFirstInFirstChain(), cursor.getFirstPrevRel(), cursor.getFirstNode());
            checkPrevPointer(cursor, cursor.isFirstInSecondChain(), cursor.getSecondPrevRel(), cursor.getSecondNode());
            checkNextPointer(cursor, cursor.getFirstNextRel(), cursor.getFirstNode());
            checkNextPointer(cursor, cursor.getSecondNextRel(), cursor.getSecondNode());
        }

        private void checkPrevPointer(
                RecordRelationshipScanCursor cursor, boolean firstInChain, long prevRel, long node) {
            if (firstInChain) {
                // It's first in chain, nothing more to check
                return;
            }

            otherCursor.single(prevRel);
            checkState(otherCursor.next(), "%s prev refers to unused %s", cursor, otherCursor);
            checkState(
                    otherCursor.getFirstNode() == node || otherCursor.getSecondNode() == node,
                    "%s prev refers to %s which is a relationship between other nodes",
                    cursor,
                    otherCursor);
            long nextRel =
                    otherCursor.getFirstNode() == node ? otherCursor.getFirstNextRel() : otherCursor.getSecondNextRel();
            checkState(nextRel == cursor.getId(), "%s prev refers to %s that doesn't refer back", cursor, otherCursor);
        }

        private void checkNextPointer(RecordRelationshipScanCursor cursor, long nextRel, long node) {
            if (Record.NULL_REFERENCE.is(nextRel)) {
                // It's last in chain, nothing more to check
                return;
            }

            otherCursor.single(nextRel);
            checkState(otherCursor.next(), "%s next refers to unused %s", cursor, otherCursor);
            checkState(
                    otherCursor.getFirstNode() == node || otherCursor.getSecondNode() == node,
                    "%s next refers to %s which is a relationship between other nodes",
                    cursor,
                    otherCursor);
            long prevRel =
                    otherCursor.getFirstNode() == node ? otherCursor.getFirstPrevRel() : otherCursor.getSecondPrevRel();
            checkState(prevRel == cursor.getId(), "%s next refers to %s that doesn't refer back", cursor, otherCursor);
        }
    }
}
