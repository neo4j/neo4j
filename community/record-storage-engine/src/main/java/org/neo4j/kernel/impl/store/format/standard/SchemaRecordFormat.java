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

import java.io.IOException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.SchemaRecord;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.LongReference;

public class SchemaRecordFormat extends BaseOneByteHeaderRecordFormat<SchemaRecord>
        implements RecordFormat<SchemaRecord> {
    // 8 bits header. 56 possible bits for property record reference. (Even high-limit format only uses 50 bits for
    // property ids).
    public static final int RECORD_SIZE = Long.BYTES;
    private static final int HEADER_SHIFT = Long.SIZE - Byte.SIZE;
    private static final long RECORD_IN_USE_BIT = ((long) IN_USE_BIT) << HEADER_SHIFT;
    private static final int HAS_PROP_BIT = 0b0000_0010;
    private static final long RECORD_HAS_PROPERTY = ((long) HAS_PROP_BIT) << HEADER_SHIFT;
    private static final long RECORD_PROPERTY_REFERENCE_MASK = 0x00FFFFFF_FFFFFFFFL;
    private static final long NO_NEXT_PROP = Record.NO_NEXT_PROPERTY.longValue();

    public SchemaRecordFormat() {
        this(false);
    }

    public SchemaRecordFormat(boolean pageAligned) {
        super(fixedRecordSize(RECORD_SIZE), 0, IN_USE_BIT, StandardFormatSettings.SCHEMA_RECORD_ID_BITS, pageAligned);
    }

    @Override
    public SchemaRecord newRecord() {
        return new SchemaRecord(LongReference.NULL);
    }

    @Override
    public void read(
            SchemaRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker)
            throws IOException {
        long data = cursor.getLong();
        boolean inUse = (data & RECORD_IN_USE_BIT) != 0;
        boolean shouldLoad = mode.shouldLoad(inUse);
        boolean hasProperty = (data & RECORD_HAS_PROPERTY) == RECORD_HAS_PROPERTY;
        record.initialize(inUse, shouldLoad && hasProperty ? data & RECORD_PROPERTY_REFERENCE_MASK : NO_NEXT_PROP);
    }

    @Override
    public void write(SchemaRecord record, PageCursor cursor, int recordSize, int recordsPerPage) throws IOException {
        long data = 0;
        if (record.inUse()) {
            data = RECORD_IN_USE_BIT;
            long prop = record.getNextProp();
            if (prop != NO_NEXT_PROP) {
                if ((prop & RECORD_PROPERTY_REFERENCE_MASK) != prop) {
                    cursor.setCursorException(
                            "Property reference value outside of range that can be stored in a schema record: " + prop);
                    return;
                }
                data += RECORD_HAS_PROPERTY | prop;
            }
        }
        cursor.putLong(data);
    }

    @Override
    public boolean isInUse(PageCursor cursor) {
        long data = cursor.getLong();
        return (data & RECORD_IN_USE_BIT) != 0;
    }
}
