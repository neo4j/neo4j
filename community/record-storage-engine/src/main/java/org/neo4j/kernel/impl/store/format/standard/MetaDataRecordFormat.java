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
package org.neo4j.kernel.impl.store.format.standard;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.record.MetaDataRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

public class MetaDataRecordFormat extends BaseOneByteHeaderRecordFormat<MetaDataRecord> {
    public static final int RECORD_SIZE = 9;
    private static final int ID_BITS = 32;

    public MetaDataRecordFormat() {
        this(true);
    }

    public MetaDataRecordFormat(boolean pageAligned) {
        super(fixedRecordSize(RECORD_SIZE), 0, IN_USE_BIT, ID_BITS, pageAligned);
    }

    @Override
    public MetaDataRecord newRecord() {
        return new MetaDataRecord();
    }

    @Override
    public void read(
            MetaDataRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker) {
        int id = record.getIntId();
        if (id > MetaDataStore.lastOccupiedSlot() && !mode.shouldLoad(false)) {
            record.initialize(false, 0);
            return;
        }

        int offset = id * recordSize;
        cursor.setOffset(offset);
        boolean inUse = cursor.getByte() == Record.IN_USE.byteValue();
        long value = inUse ? cursor.getLong() : 0;
        record.initialize(inUse, value);
    }

    @Override
    public void write(MetaDataRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
        assert record.inUse();
        cursor.putByte(Record.IN_USE.byteValue());
        cursor.putLong(record.getValue());
    }
}
