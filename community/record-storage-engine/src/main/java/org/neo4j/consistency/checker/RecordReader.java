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
package org.neo4j.consistency.checker;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.CommonAbstractStore;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.StorageReader;

/**
 * Essentially a convenience for store+record+cursor. The reason why a {@link StorageReader} isn't quite enough is that they typically
 * don't handle reading of inconsistent data.
 */
class RecordReader<RECORD extends AbstractBaseRecord> implements AutoCloseable {
    private final CommonAbstractStore<RECORD, ?> store;
    private final RECORD record;
    private final PageCursor cursor;
    private final MemoryTracker memoryTracker;

    RecordReader(
            CommonAbstractStore<RECORD, ?> store,
            boolean prefetch,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        this.store = store;
        this.record = store.newRecord();
        this.memoryTracker = memoryTracker;
        this.cursor = prefetch
                ? store.openPageCursorForReadingWithPrefetching(0, cursorContext)
                : store.openPageCursorForReading(0, cursorContext);
    }

    RECORD read(long id) {
        store.getRecordByCursor(id, record, RecordLoad.FORCE, cursor, memoryTracker);
        return record;
    }

    @Override
    public void close() {
        cursor.close();
    }

    RECORD record() {
        return record;
    }

    <STORE> STORE store() {
        return (STORE) store;
    }
}
