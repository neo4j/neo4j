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
package org.neo4j.kernel.impl.store.format;

import java.io.IOException;
import org.neo4j.internal.id.IdSequence;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.StoreHeader;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

public class ForcedSecondaryUnitRecordFormat<RECORD extends AbstractBaseRecord> implements RecordFormat<RECORD> {
    private final RecordFormat<RECORD> actual;

    public ForcedSecondaryUnitRecordFormat(RecordFormat<RECORD> actual) {
        this.actual = actual;
    }

    @Override
    public RECORD newRecord() {
        return actual.newRecord();
    }

    @Override
    public int getRecordSize(StoreHeader storeHeader) {
        return actual.getRecordSize(storeHeader);
    }

    @Override
    public int getRecordHeaderSize() {
        return actual.getRecordHeaderSize();
    }

    @Override
    public boolean isInUse(PageCursor cursor) {
        return actual.isInUse(cursor);
    }

    @Override
    public void read(
            RECORD record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker)
            throws IOException {
        actual.read(record, cursor, mode, recordSize, recordsPerPage, memoryTracker);
    }

    @Override
    public void prepare(RECORD record, int recordSize, IdSequence idSequence, CursorContext cursorContext) {
        actual.prepare(record, recordSize, idSequence, cursorContext);
        if (!record.hasSecondaryUnitId()) {
            record.setSecondaryUnitIdOnCreate(idSequence.nextId(cursorContext));
        }
    }

    @Override
    public void write(RECORD record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException {
        actual.write(record, cursor, recordSize, recordsPerPage);
    }

    @Override
    public long getNextRecordReference(RECORD record) {
        return actual.getNextRecordReference(record);
    }

    @Override
    public boolean equals(Object otherFormat) {
        return actual.equals(otherFormat);
    }

    @Override
    public int hashCode() {
        return actual.hashCode();
    }

    @Override
    public long getMaxId() {
        return actual.getMaxId();
    }

    @Override
    public int getFilePageSize(int pageSize, int recordSize) {
        return actual.getFilePageSize(pageSize, recordSize);
    }
}
