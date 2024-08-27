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

import static java.lang.String.format;

import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

public class PropertyRecordFormat extends BaseRecordFormat<PropertyRecord> {
    public static final int DEFAULT_DATA_BLOCK_SIZE = 120;
    public static final int DEFAULT_PAYLOAD_SIZE = 32;

    public static final int RECORD_SIZE =
            1 /*next and prev high bits*/ + 4 /*next*/ + 4 /*prev*/ + DEFAULT_PAYLOAD_SIZE /*property blocks*/;
    // = 41

    public PropertyRecordFormat() {
        this(false);
    }

    public PropertyRecordFormat(boolean pageAligned) {
        super(fixedRecordSize(RECORD_SIZE), 0, StandardFormatSettings.PROPERTY_MAXIMUM_ID_BITS, pageAligned);
    }

    @Override
    public PropertyRecord newRecord() {
        return new PropertyRecord(-1);
    }

    @Override
    public void read(
            PropertyRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker) {
        int offsetAtBeginning = cursor.getOffset();

        /*
         * [pppp,nnnn] previous, next high bits
         */
        byte modifiers = cursor.getByte();
        long prevMod = (modifiers & 0xF0L) << 28;
        long nextMod = (modifiers & 0x0FL) << 32;
        long prevProp = cursor.getInt() & 0xFFFFFFFFL;
        long nextProp = cursor.getInt() & 0xFFFFFFFFL;
        record.initialize(
                false,
                BaseRecordFormat.longFromIntAndMod(prevProp, prevMod),
                BaseRecordFormat.longFromIntAndMod(nextProp, nextMod));
        while (cursor.getOffset() - offsetAtBeginning < RECORD_SIZE) {
            long block = cursor.getLong();
            PropertyType type = PropertyType.getPropertyTypeOrNull(block);
            if (type == null) {
                // We assume that storage is defragged
                break;
            }

            record.setInUse(true);
            record.addLoadedBlock(block);
            int numberOfBlocksUsed = type.calculateNumberOfBlocksUsed(block);
            if (numberOfBlocksUsed == PropertyType.BLOCKS_USED_FOR_BAD_TYPE_OR_ENCODING) {
                cursor.setCursorException(
                        "Invalid type or encoding of property block: " + block + " (type = " + type + ")");
                return;
            }
            int additionalBlocks = numberOfBlocksUsed - 1;
            if (additionalBlocks * Long.BYTES > RECORD_SIZE - (cursor.getOffset() - offsetAtBeginning)) {
                cursor.setCursorException(format(
                        "PropertyRecord[%d] claims to have more property blocks than can fit in a record. "
                                + "additionalBlocks:%d, recordSize:%d, cursorOffset:%d, offsetAtBeginning:%d",
                        record.getId(), additionalBlocks, RECORD_SIZE, cursor.getOffset(), offsetAtBeginning));
                return;
            }
            while (additionalBlocks-- > 0) {
                record.addLoadedBlock(cursor.getLong());
            }
        }
    }

    @Override
    public void write(PropertyRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
        if (record.inUse()) {
            // Set up the record header
            short prevModifier = record.getPrevProp() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (short) ((record.getPrevProp() & 0xF00000000L) >> 28);
            short nextModifier = record.getNextProp() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (short) ((record.getNextProp() & 0xF00000000L) >> 32);
            byte modifiers = (byte) (prevModifier | nextModifier);
            /*
             * [pppp,nnnn] previous, next high bits
             */
            cursor.putByte(modifiers);
            cursor.putInt((int) record.getPrevProp());
            cursor.putInt((int) record.getNextProp());

            // Then go through the blocks
            int longsAppended = 0; // For marking the end of blocks
            for (PropertyBlock block : record) {
                long[] propBlockValues = block.getValueBlocks();
                for (long propBlockValue : propBlockValues) {
                    cursor.putLong(propBlockValue);
                }

                longsAppended += propBlockValues.length;
            }
            if (longsAppended < PropertyType.getPayloadSizeLongs()) {
                cursor.putLong(0);
            }
        } else {
            // skip over the record header, nothing useful there
            cursor.setOffset(cursor.getOffset() + 9);
            cursor.putLong(0);
        }
    }

    @Override
    public long getNextRecordReference(PropertyRecord record) {
        return record.getNextProp();
    }

    /**
     * For property records there's no "inUse" byte and we need to read the whole record to
     * see if there are any PropertyBlocks in use in it.
     */
    @Override
    public boolean isInUse(PageCursor cursor) {
        cursor.setOffset(cursor.getOffset() /*skip...*/ + 1 /*mod*/ + 4 /*prev*/ + 4 /*next*/);
        int blocks = PropertyType.getPayloadSizeLongs();
        for (int i = 0; i < blocks; i++) {
            long block = cursor.getLong();
            // Since there's no inUse byte we have to check the special case of first block == 0, which will mean that
            // it's deleted
            if (i == 0 && block == 0) {
                return false;
            }
            if (PropertyType.getPropertyTypeOrNull(block) != null) {
                return true;
            }
        }
        return false;
    }
}
