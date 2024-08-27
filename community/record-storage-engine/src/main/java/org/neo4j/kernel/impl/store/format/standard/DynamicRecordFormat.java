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
import org.neo4j.kernel.impl.store.format.BaseOneByteHeaderRecordFormat;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.memory.MemoryTracker;

public class DynamicRecordFormat extends BaseOneByteHeaderRecordFormat<DynamicRecord> {
    // (in_use+next high)(1 byte)+nr_of_bytes(3 bytes)+next_block(int)
    public static final int RECORD_HEADER_SIZE = 1 + 3 + 4; // = 8

    public DynamicRecordFormat() {
        this(false);
    }

    public DynamicRecordFormat(boolean pageAligned) {
        super(
                INT_STORE_HEADER_READER,
                RECORD_HEADER_SIZE,
                0x10 /*the inUse bit is the lsb in the second nibble*/,
                StandardFormatSettings.DYNAMIC_MAXIMUM_ID_BITS,
                pageAligned);
    }

    @Override
    public DynamicRecord newRecord() {
        return new DynamicRecord(-1);
    }

    @Override
    public void read(
            DynamicRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker) {
        /*
         * First 4b
         * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
         * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
         * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
         * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record in big-endian
         */
        byte firstByte = cursor.getByte();
        boolean isStartRecord = (firstByte & 0x80) == 0;
        boolean inUse = (firstByte & 0x10) != 0;
        if (mode.shouldLoad(inUse)) {
            int dataSize = recordSize - getRecordHeaderSize();
            int nrOfBytes =
                    (cursor.getByte() & 0xFF) << 16 | (cursor.getByte() & 0xFF) << 8 | (cursor.getByte() & 0xFF);
            if (nrOfBytes > dataSize) {
                // We must have performed an inconsistent read,
                // because this many bytes cannot possibly fit in a record!
                cursor.setCursorException(payloadTooBigErrorMessage(record, recordSize, nrOfBytes));
                return;
            }

            /*
             * Pointer to next block 4b (low bits of the pointer)
             */
            long nextBlock = cursor.getInt() & 0xFFFFFFFFL;
            long nextModifier = (firstByte & 0x0FL) << 32;

            long longNextBlock = BaseRecordFormat.longFromIntAndMod(nextBlock, nextModifier);
            record.initialize(inUse, isStartRecord, longNextBlock, -1);
            readData(record, cursor, nrOfBytes, memoryTracker);
            if (longNextBlock != Record.NO_NEXT_BLOCK.intValue() && nrOfBytes != dataSize) {
                // If we have a next block, but don't use the whole current block
                cursor.setCursorException(illegalBlockSizeMessage(record, dataSize));
            }
        } else {
            record.setInUse(inUse);
        }
    }

    public static String payloadTooBigErrorMessage(DynamicRecord record, int recordSize, int nrOfBytes) {
        return format(
                "DynamicRecord[%s] claims to have a payload of %s bytes, "
                        + "which is larger than the record size of %s bytes.",
                record.getId(), nrOfBytes, recordSize);
    }

    private static String illegalBlockSizeMessage(DynamicRecord record, int dataSize) {
        return format(
                "Next block set[%d] current block illegal size[%d/%d]",
                record.getNextBlock(), record.getLength(), dataSize);
    }

    public static void readData(DynamicRecord record, PageCursor cursor, int len, MemoryTracker memoryTracker) {
        byte[] data = prepareRecordDataArray(record, len, memoryTracker);
        cursor.getBytes(data);
    }

    private static byte[] prepareRecordDataArray(DynamicRecord record, int length, MemoryTracker memoryTracker) {
        byte[] data = record.getData();
        if (data != null && data.length == length) {
            return data;
        }
        memoryTracker.allocateHeap(length);
        byte[] newData = new byte[length];
        record.setData(newData);
        return newData;
    }

    @Override
    public void write(DynamicRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
        if (record.inUse()) {
            /*
             * First 4b
             * [x   ,    ][    ,    ][    ,    ][    ,    ] 0: start record, 1: linked record
             * [   x,    ][    ,    ][    ,    ][    ,    ] inUse
             * [    ,xxxx][    ,    ][    ,    ][    ,    ] high next block bits
             * [    ,    ][xxxx,xxxx][xxxx,xxxx][xxxx,xxxx] nr of bytes in the data field in this record in big endian
             */
            long nextBlock = record.getNextBlock();
            int highNextBlockBits =
                    nextBlock == Record.NO_NEXT_BLOCK.intValue() ? 0 : (int) ((nextBlock & 0xF00000000L) >> 32);

            byte firstByte = (byte) (0x10 | (record.isStartRecord() ? 0 : 0x80) | highNextBlockBits);

            int recordLength = record.getLength();
            assert recordLength < (1 << 24) - 1;
            cursor.putByte(firstByte);
            cursor.putByte((byte) ((recordLength >> 16) & 0xFF));
            cursor.putByte((byte) ((recordLength >> 8) & 0xFF));
            cursor.putByte((byte) ((recordLength) & 0xFF));
            cursor.putInt((int) nextBlock);
            cursor.putBytes(record.getData());
        } else {
            cursor.putByte(Record.NOT_IN_USE.byteValue());
        }
    }

    @Override
    public long getNextRecordReference(DynamicRecord record) {
        return record.getNextBlock();
    }
}
