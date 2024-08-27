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
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

public class NodeRecordFormat extends BaseOneByteHeaderRecordFormat<NodeRecord> {
    // in_use(byte)+next_rel_id(int)+next_prop_id(int)+labels(5)+extra(byte)
    public static final int RECORD_SIZE = 15;

    public NodeRecordFormat() {
        this(false);
    }

    public NodeRecordFormat(boolean pageAligned) {
        super(fixedRecordSize(RECORD_SIZE), 0, IN_USE_BIT, StandardFormatSettings.NODE_MAXIMUM_ID_BITS, pageAligned);
    }

    @Override
    public NodeRecord newRecord() {
        return new NodeRecord(-1);
    }

    @Override
    public void read(
            NodeRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker) {
        byte headerByte = cursor.getByte();
        boolean inUse = isInUse(headerByte);
        record.setInUse(inUse);
        if (mode.shouldLoad(inUse)) {
            long nextRel = cursor.getInt() & 0xFFFFFFFFL;
            long nextProp = cursor.getInt() & 0xFFFFFFFFL;

            long relModifier = (headerByte & 0xEL) << 31;
            long propModifier = (headerByte & 0xF0L) << 28;

            long lsbLabels = cursor.getInt() & 0xFFFFFFFFL;
            long hsbLabels =
                    cursor.getByte() & 0xFF; // so that a negative byte won't fill the "extended" bits with ones.
            long labels = lsbLabels | (hsbLabels << 32);
            byte extra = cursor.getByte();
            boolean dense = (extra & 0x1) > 0;

            record.initialize(
                    inUse,
                    BaseRecordFormat.longFromIntAndMod(nextProp, propModifier),
                    dense,
                    BaseRecordFormat.longFromIntAndMod(nextRel, relModifier),
                    labels);
        } else {
            int nextOffset = cursor.getOffset() + recordSize - HEADER_SIZE;
            cursor.setOffset(nextOffset);
        }
    }

    @Override
    public void write(NodeRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
        if (record.inUse()) {
            long nextRel = record.getNextRel();
            long nextProp = record.getNextProp();

            short relModifier =
                    nextRel == Record.NO_NEXT_RELATIONSHIP.intValue() ? 0 : (short) ((nextRel & 0x700000000L) >> 31);
            short propModifier =
                    nextProp == Record.NO_NEXT_PROPERTY.intValue() ? 0 : (short) ((nextProp & 0xF00000000L) >> 28);

            // [    ,   x] in use bit
            // [    ,xxx ] higher bits for rel id
            // [xxxx,    ] higher bits for prop id
            short inUseUnsignedByte = (record.inUse() ? Record.IN_USE : Record.NOT_IN_USE).byteValue();
            inUseUnsignedByte = (short) (inUseUnsignedByte | relModifier | propModifier);

            cursor.putByte((byte) inUseUnsignedByte);
            cursor.putInt((int) nextRel);
            cursor.putInt((int) nextProp);

            // lsb of labels
            long labelField = record.getLabelField();
            cursor.putInt((int) labelField);
            // msb of labels
            cursor.putByte((byte) ((labelField & 0xFF00000000L) >> 32));

            byte extra = record.isDense() ? (byte) 1 : (byte) 0;
            cursor.putByte(extra);
        } else {
            markAsUnused(cursor);
        }
    }
}
