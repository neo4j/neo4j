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

import static org.neo4j.kernel.impl.index.schema.NativeIndexKey.Inclusion.NEUTRAL;

import org.neo4j.index.internal.gbptree.Writer;
import org.neo4j.io.IOUtils;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.storageengine.api.IndexEntryUpdate;
import org.neo4j.storageengine.api.ValueIndexEntryUpdate;
import org.neo4j.values.storable.Value;

class NativeIndexUpdater<KEY extends NativeIndexKey<KEY>> implements IndexUpdater {
    private final KEY treeKey;
    private final IndexUpdateIgnoreStrategy ignoreStrategy;
    private final ConflictDetectingValueMerger<KEY, Value[]> conflictDetectingValueMerger;
    private Writer<KEY, NullValue> writer;

    private boolean closed = true;

    NativeIndexUpdater(
            KEY treeKey,
            IndexUpdateIgnoreStrategy ignoreStrategy,
            ConflictDetectingValueMerger<KEY, Value[]> conflictDetectingValueMerger) {
        this.treeKey = treeKey;
        this.ignoreStrategy = ignoreStrategy;
        this.conflictDetectingValueMerger = conflictDetectingValueMerger;
    }

    NativeIndexUpdater<KEY> initialize(Writer<KEY, NullValue> writer) {
        if (!closed) {
            throw new IllegalStateException("Updater still open");
        }

        this.writer = writer;
        closed = false;
        return this;
    }

    @Override
    public void process(IndexEntryUpdate<?> update) throws IndexEntryConflictException {
        assertOpen();
        ValueIndexEntryUpdate<?> valueUpdate = asValueUpdate(update);
        processUpdate(treeKey, valueUpdate, writer, conflictDetectingValueMerger, ignoreStrategy);
    }

    @Override
    public void close() {
        closed = true;
        IOUtils.closeAllUnchecked(writer);
    }

    private void assertOpen() {
        if (closed) {
            throw new IllegalStateException("Updater has been closed");
        }
    }

    @Override
    public void yield() {
        writer.yield();
    }

    static <KEY extends NativeIndexKey<KEY>> void processUpdate(
            KEY treeKey,
            ValueIndexEntryUpdate<?> update,
            Writer<KEY, NullValue> writer,
            ConflictDetectingValueMerger<KEY, Value[]> conflictDetectingValueMerger,
            IndexUpdateIgnoreStrategy ignoreStrategy)
            throws IndexEntryConflictException {
        switch (update.updateMode()) {
            case REMOVED:
                processRemove(treeKey, update.getEntityId(), update.values(), writer, ignoreStrategy);
                break;
            case CHANGED:
                processRemove(treeKey, update.getEntityId(), update.beforeValues(), writer, ignoreStrategy);
                // fallthrough
            case ADDED:
                processAdd(
                        treeKey,
                        update.getEntityId(),
                        update.values(),
                        writer,
                        conflictDetectingValueMerger,
                        ignoreStrategy);
                break;
            default:
                throw new IllegalArgumentException();
        }
    }

    private static <KEY extends NativeIndexKey<KEY>> void processRemove(
            KEY treeKey,
            long entityId,
            Value[] values,
            Writer<KEY, NullValue> writer,
            IndexUpdateIgnoreStrategy ignoreStrategy) {
        if (ignoreStrategy.ignore(values)) {
            return;
        }
        // todo Do we need to verify that we actually removed something at all?
        // todo Difference between online and recovery?
        initializeKeyFromUpdate(treeKey, entityId, values);
        writer.remove(treeKey);
    }

    private static <KEY extends NativeIndexKey<KEY>> void processAdd(
            KEY treeKey,
            long entityId,
            Value[] values,
            Writer<KEY, NullValue> writer,
            ConflictDetectingValueMerger<KEY, Value[]> conflictDetectingValueMerger,
            IndexUpdateIgnoreStrategy ignoreStrategy)
            throws IndexEntryConflictException {
        if (ignoreStrategy.ignore(values)) {
            return;
        }
        initializeKeyFromUpdate(treeKey, entityId, values);
        conflictDetectingValueMerger.controlConflictDetection(treeKey);
        writer.merge(treeKey, NullValue.INSTANCE, conflictDetectingValueMerger);
        conflictDetectingValueMerger.checkConflict(values);
    }

    static <KEY extends NativeIndexKey<KEY>> void initializeKeyFromUpdate(KEY treeKey, long entityId, Value[] values) {
        treeKey.initialize(entityId);
        for (int i = 0; i < values.length; i++) {
            treeKey.initFromValue(i, values[i], NEUTRAL);
        }
    }
}
