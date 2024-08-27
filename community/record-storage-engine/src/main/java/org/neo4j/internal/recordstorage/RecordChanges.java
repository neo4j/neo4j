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

import static org.neo4j.collection.trackable.HeapTrackingCollections.newLongObjectMap;
import static org.neo4j.memory.HeapEstimator.shallowSizeOfInstance;

import java.util.Collection;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.util.LocalIntCounter;

/**
 * Manages changes to records in a transaction. Before/after state is supported as well as deciding when to make a record heavy and when to consider it changed
 * for inclusion in the transaction as a command.
 *
 * @param <RECORD>     type of record
 * @param <ADDITIONAL> additional payload
 */
public class RecordChanges<RECORD extends AbstractBaseRecord, ADDITIONAL> implements RecordAccess<RECORD, ADDITIONAL> {
    public static final long SHALLOW_SIZE =
            shallowSizeOfInstance(RecordChanges.class) + shallowSizeOfInstance(LocalIntCounter.class);

    private final MutableLongObjectMap<RecordProxy<RECORD, ADDITIONAL>> recordChanges;
    private final Loader<RECORD, ADDITIONAL> loader;
    private final MutableInt changeCounter;
    private final MemoryTracker memoryTracker;
    private final LoadMonitor loadMonitor;
    private final StoreCursors storeCursors;

    public static <RECORD extends AbstractBaseRecord, ADDITIONAL> RecordChanges<RECORD, ADDITIONAL> create(
            Loader<RECORD, ADDITIONAL> loader,
            MutableInt globalCounter,
            MemoryTracker memoryTracker,
            LoadMonitor loadMonitor,
            StoreCursors storeCursors) {
        memoryTracker.allocateHeap(SHALLOW_SIZE);
        return new RecordChanges<>(loader, globalCounter, memoryTracker, loadMonitor, storeCursors);
    }

    private RecordChanges(
            Loader<RECORD, ADDITIONAL> loader,
            MutableInt globalCounter,
            MemoryTracker memoryTracker,
            LoadMonitor loadMonitor,
            StoreCursors storeCursors) {
        this.loader = loader;
        this.recordChanges = newLongObjectMap(memoryTracker);
        this.changeCounter = new LocalIntCounter(globalCounter);
        this.memoryTracker = memoryTracker;
        this.loadMonitor = loadMonitor;
        this.storeCursors = storeCursors;
    }

    @Override
    public String toString() {
        return "RecordChanges{" + "recordChanges=" + recordChanges + '}';
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getIfLoaded(long key) {
        return recordChanges.get(key);
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad(long key, ADDITIONAL additionalData, RecordLoad load) {
        RecordProxy<RECORD, ADDITIONAL> result = recordChanges.get(key);
        if (result == null) {
            RECORD record = loader.load(key, additionalData, load, memoryTracker);
            memoryTracker.allocateHeap(RecordChange.SHALLOW_SIZE);
            result = new RecordChange(key, record, false, additionalData);
        }
        return result;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> setRecord(
            long key, RECORD record, ADDITIONAL additionalData, CursorContext cursorContext) {
        memoryTracker.allocateHeap(RecordChange.SHALLOW_SIZE);
        var recordChange = new RecordChange(key, record, false, additionalData);
        recordChanges.put(key, recordChange);
        return recordChange;
    }

    @Override
    public int changeSize() {
        return changeCounter.intValue();
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create(long key, ADDITIONAL additionalData, CursorContext cursorContext) {
        if (recordChanges.containsKey(key)) {
            throw new IllegalStateException(key + " already exists");
        }

        RECORD record = loader.newUnused(key, additionalData, memoryTracker);
        memoryTracker.allocateHeap(RecordChange.SHALLOW_SIZE);
        var change = new RecordChange(key, record, true, additionalData);
        recordChanges.put(key, change);
        return change;
    }

    @Override
    public Collection<RecordProxy<RECORD, ADDITIONAL>> changes() {
        return recordChanges.values();
    }

    private class RecordChange implements RecordProxy<RECORD, ADDITIONAL> {
        public static final long SHALLOW_SIZE = shallowSizeOfInstance(RecordChanges.RecordChange.class);

        private final ADDITIONAL additionalData;
        private final RECORD record;
        private final boolean created;
        private final long key;

        private RECORD before;
        private boolean changed;

        private RecordChange(long key, RECORD record, boolean created, ADDITIONAL additionalData) {
            this.key = key;
            this.record = record;
            this.created = created;
            this.additionalData = additionalData;
        }

        @Override
        public String toString() {
            return "RecordChange{record=" + record + ",key=" + key + ",created=" + created + '}';
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public RECORD forChangingLinkage() {
            return prepareForChange();
        }

        @Override
        public RECORD forChangingData() {
            ensureHeavy(storeCursors);
            return prepareForChange();
        }

        private RECORD prepareForChange() {
            ensureHasBeforeRecordImage();
            if (!this.changed) {
                RecordProxy<RECORD, ADDITIONAL> previous = recordChanges.put(key, this);

                if (previous == null || !previous.isChanged()) {
                    loadMonitor.markedAsChanged(before);
                    changeCounter.increment();
                }

                this.changed = true;
            }
            return this.record;
        }

        private void ensureHeavy(StoreCursors storeCursors) {
            if (!created) {
                loader.ensureHeavy(record, storeCursors, memoryTracker);
                if (before != null) {
                    loader.ensureHeavy(before, storeCursors, memoryTracker);
                }
            }
        }

        @Override
        public RECORD forReadingLinkage() {
            return this.record;
        }

        @Override
        public RECORD forReadingData() {
            ensureHeavy(storeCursors);
            return this.record;
        }

        @Override
        public boolean isChanged() {
            return this.changed;
        }

        @Override
        public RECORD getBefore() {
            ensureHasBeforeRecordImage();
            return before;
        }

        private void ensureHasBeforeRecordImage() {
            if (before == null) {
                this.before = loader.copy(record, memoryTracker);
            }
        }

        @Override
        public boolean isCreated() {
            return created;
        }

        @Override
        public ADDITIONAL getAdditionalData() {
            return additionalData;
        }
    }
}
