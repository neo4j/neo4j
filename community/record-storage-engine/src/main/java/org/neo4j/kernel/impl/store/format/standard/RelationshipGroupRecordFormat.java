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
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.store.record.RelationshipGroupRecord;
import org.neo4j.memory.MemoryTracker;

public class RelationshipGroupRecordFormat extends BaseOneByteHeaderRecordFormat<RelationshipGroupRecord> {
    /* Record layout
     *
     * [type+inUse+highbits,next,firstOut,firstIn,firstLoop,owningNode] = 25B
     *
     * One record holds first relationship links (out,in,loop) to relationships for one type for one entity.
     */

    public static final int RECORD_SIZE = 25;

    public RelationshipGroupRecordFormat() {
        this(false);
    }

    public RelationshipGroupRecordFormat(boolean pageAligned) {
        super(
                fixedRecordSize(RECORD_SIZE),
                0,
                IN_USE_BIT,
                StandardFormatSettings.RELATIONSHIP_GROUP_MAXIMUM_ID_BITS,
                pageAligned);
    }

    @Override
    public void read(
            RelationshipGroupRecord record,
            PageCursor cursor,
            RecordLoad mode,
            int recordSize,
            int recordsPerPage,
            MemoryTracker memoryTracker) {
        // [    ,   x] in use
        // [    ,xxx ] high next id bits
        // [ xxx,    ] high firstOut bits
        // [x   ,    ] has external degrees (out)
        long headerByte = cursor.getByte();
        boolean inUse = isInUse((byte) headerByte);
        record.setInUse(inUse);
        if (mode.shouldLoad(inUse)) {
            // [    ,   x] has external degrees (in)
            // [    ,xxx ] high firstIn bits
            // [ xxx,    ] high firstLoop bits
            // [x   ,    ] has external degrees (loop)
            long highByte = cursor.getByte();

            int type = cursor.getShort() & 0xFFFF;
            long nextLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextOutLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextInLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long nextLoopLowBits = cursor.getInt() & 0xFFFFFFFFL;
            long owningNode = (cursor.getInt() & 0xFFFFFFFFL) | (((long) cursor.getByte()) << 32);

            long nextMod = (headerByte & 0xE) << 31;
            long nextOutMod = (headerByte & 0x70) << 28;
            long nextInMod = (highByte & 0xE) << 31;
            long nextLoopMod = (highByte & 0x70) << 28;

            boolean hasExternalDegreesOut = (headerByte & 0x80) != 0;
            boolean hasExternalDegreesIn = (highByte & 0x1) != 0;
            boolean hasExternalDegreesLoop = (highByte & 0x80) != 0;

            record.initialize(
                    inUse,
                    type,
                    BaseRecordFormat.longFromIntAndMod(nextOutLowBits, nextOutMod),
                    BaseRecordFormat.longFromIntAndMod(nextInLowBits, nextInMod),
                    BaseRecordFormat.longFromIntAndMod(nextLoopLowBits, nextLoopMod),
                    owningNode,
                    BaseRecordFormat.longFromIntAndMod(nextLowBits, nextMod));
            record.setHasExternalDegreesOut(hasExternalDegreesOut);
            record.setHasExternalDegreesIn(hasExternalDegreesIn);
            record.setHasExternalDegreesLoop(hasExternalDegreesLoop);
        }
    }

    @Override
    public void write(RelationshipGroupRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
        if (record.inUse()) {
            long nextMod = record.getNext() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (record.getNext() & 0x700000000L) >> 31;
            long nextOutMod = record.getFirstOut() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (record.getFirstOut() & 0x700000000L) >> 28;
            long nextInMod = record.getFirstIn() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (record.getFirstIn() & 0x700000000L) >> 31;
            long nextLoopMod = record.getFirstLoop() == Record.NO_NEXT_RELATIONSHIP.intValue()
                    ? 0
                    : (record.getFirstLoop() & 0x700000000L) >> 28;
            long hasExternalDegreesOutMod = record.hasExternalDegreesOut() ? 0x80 : 0;
            long hasExternalDegreesInMod = record.hasExternalDegreesIn() ? 0x1 : 0;
            long hasExternalDegreesLoopMod = record.hasExternalDegreesLoop() ? 0x80 : 0;

            // [    ,   x] in use
            // [    ,xxx ] high next id bits
            // [ xxx,    ] high firstOut bits
            // [x   ,    ] has external degrees (out)
            cursor.putByte((byte) (hasExternalDegreesOutMod | nextOutMod | nextMod | 1));

            // [    ,   x] has external degrees (in)
            // [    ,xxx ] high firstIn bits
            // [ xxx,    ] high firstLoop bits
            // [x   ,    ] has external degrees (loop)
            cursor.putByte((byte) (hasExternalDegreesLoopMod | nextLoopMod | nextInMod | hasExternalDegreesInMod));

            cursor.putShort((short) record.getType());
            cursor.putInt((int) record.getNext());
            cursor.putInt((int) record.getFirstOut());
            cursor.putInt((int) record.getFirstIn());
            cursor.putInt((int) record.getFirstLoop());
            cursor.putInt((int) record.getOwningNode());
            cursor.putByte((byte) (record.getOwningNode() >> 32));
        } else {
            markAsUnused(cursor);
        }
    }

    @Override
    public RelationshipGroupRecord newRecord() {
        return new RelationshipGroupRecord(-1);
    }

    @Override
    public long getNextRecordReference(RelationshipGroupRecord record) {
        return record.getNext();
    }
}
