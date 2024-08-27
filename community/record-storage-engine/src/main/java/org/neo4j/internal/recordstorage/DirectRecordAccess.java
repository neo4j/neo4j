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

import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.CursorType;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.storageengine.util.IdUpdateListener;

/**
 * Provides direct access to records in a store. Changes are batched up and written whenever transaction is committed.
 */
public class DirectRecordAccess<RECORD extends AbstractBaseRecord, ADDITIONAL>
        implements RecordAccess<RECORD, ADDITIONAL> {
    private final RecordStore<RECORD> store;
    private final Loader<RECORD, ADDITIONAL> loader;
    private final CursorContext cursorContext;
    private final StoreCursors storeCursors;
    private final CursorType cursorType;
    private final MemoryTracker memoryTracker;

    private final Map<Long, DirectRecordProxy> batch = new HashMap<>();
    private final MutableInt changeCounter = new MutableInt();

    public DirectRecordAccess(
            RecordStore<RECORD> store,
            Loader<RECORD, ADDITIONAL> loader,
            CursorContext cursorContext,
            CursorType cursorType,
            StoreCursors storeCursors,
            MemoryTracker memoryTracker) {
        this.store = store;
        this.loader = loader;
        this.cursorContext = cursorContext;
        this.cursorType = cursorType;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getOrLoad(long key, ADDITIONAL additionalData, RecordLoad load) {
        DirectRecordProxy loaded = batch.get(key);
        if (loaded != null) {
            return loaded;
        }
        return proxy(key, loader.load(key, additionalData, load, INSTANCE), additionalData, false, cursorContext);
    }

    private RecordProxy<RECORD, ADDITIONAL> putInBatch(long key, DirectRecordProxy proxy) {
        DirectRecordProxy previous = batch.put(key, proxy);
        assert previous == null;
        return proxy;
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> create(long key, ADDITIONAL additionalData, CursorContext cursorContext) {
        return proxy(key, loader.newUnused(key, additionalData, INSTANCE), additionalData, true, cursorContext);
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> getIfLoaded(long key) {
        return batch.get(key);
    }

    @Override
    public RecordProxy<RECORD, ADDITIONAL> setRecord(
            long key, RECORD record, ADDITIONAL additionalData, CursorContext cursorContext) {
        throw new UnsupportedOperationException("Not supported");
    }

    @Override
    public int changeSize() {
        return changeCounter.intValue();
    }

    @Override
    public Collection<DirectRecordProxy> changes() {
        return batch.values();
    }

    private DirectRecordProxy proxy(
            final long key,
            final RECORD record,
            final ADDITIONAL additionalData,
            boolean created,
            CursorContext cursorContext) {
        return new DirectRecordProxy(key, record, additionalData, created, cursorContext);
    }

    private class DirectRecordProxy implements RecordProxy<RECORD, ADDITIONAL> {
        private final long key;
        private final RECORD record;
        private final ADDITIONAL additionalData;
        private final CursorContext cursorContext;
        private boolean changed;
        private final boolean created;
        private RECORD before;

        DirectRecordProxy(
                long key, RECORD record, ADDITIONAL additionalData, boolean created, CursorContext cursorContext) {
            this.key = key;
            this.record = record;
            this.additionalData = additionalData;
            this.cursorContext = cursorContext;
            if (created) {
                prepareChange();
            }
            this.created = created;
        }

        @Override
        public long getKey() {
            return key;
        }

        @Override
        public RECORD forChangingLinkage() {
            prepareChange();
            return record;
        }

        private void prepareChange() {
            if (!changed) {
                changed = true;
                putInBatch(key, this);
                changeCounter.increment();
            }
        }

        @Override
        public RECORD forChangingData() {
            loader.ensureHeavy(record, storeCursors, memoryTracker);
            prepareChange();
            return record;
        }

        @Override
        public RECORD forReadingLinkage() {
            return record;
        }

        @Override
        public RECORD forReadingData() {
            loader.ensureHeavy(record, storeCursors, memoryTracker);
            return record;
        }

        @Override
        public ADDITIONAL getAdditionalData() {
            return additionalData;
        }

        @Override
        public RECORD getBefore() {
            ensureHasBeforeRecordImage();
            return before;
        }

        private void ensureHasBeforeRecordImage() {
            if (before == null) {
                this.before = loader.copy(record, INSTANCE);
            }
        }

        @Override
        public String toString() {
            return record.toString();
        }

        public void store(PageCursor pageCursor) {
            if (changed) {
                store.updateRecord(record, IdUpdateListener.IGNORE, pageCursor, cursorContext, storeCursors);
            }
        }

        @Override
        public boolean isChanged() {
            return changed;
        }

        @Override
        public boolean isCreated() {
            return created;
        }
    }

    public void commit() {
        if (changeCounter.intValue() == 0) {
            return;
        }

        List<DirectRecordProxy> directRecordProxies = new ArrayList<>(batch.values());
        directRecordProxies.sort((o1, o2) -> Long.compare(-o1.getKey(), o2.getKey()));
        try (var cursor = storeCursors.writeCursor(cursorType)) {
            for (DirectRecordProxy proxy : directRecordProxies) {
                proxy.store(cursor);
            }
        }
        changeCounter.setValue(0);
        batch.clear();
    }
}
