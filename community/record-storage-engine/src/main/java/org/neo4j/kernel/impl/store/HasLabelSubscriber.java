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
package org.neo4j.kernel.impl.store;

import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.util.BitBuffer;

/**
 * Used for streaming hasLabel check
 */
class HasLabelSubscriber implements RecordSubscriber<DynamicRecord> {
    private int requiredBits;
    private int remainingBits;
    private BitBuffer bits;
    private boolean firstRecord = true;
    private boolean found;
    private final int label;
    private final DynamicArrayStore labelStore;
    private final StoreCursors storeCursors;
    private int bitsUsedInLastByte;
    private final MemoryTracker memoryTracker;

    HasLabelSubscriber(
            int label, DynamicArrayStore labelStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        this.label = label;
        this.labelStore = labelStore;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
    }

    boolean hasLabel() {
        return found;
    }

    @Override
    public boolean onRecord(DynamicRecord record) {
        if (!record.inUse()) {
            return true;
        }
        labelStore.ensureHeavy(record, storeCursors, memoryTracker);

        boolean lastRecord = Record.NO_NEXT_BLOCK.is(record.getNextBlock());
        if (firstRecord) {
            firstRecord = false;
            return processFirstRecord(record.getData(), lastRecord);
        } else {
            return processRecord(record.getData(), lastRecord);
        }
    }

    private boolean processFirstRecord(byte[] data, boolean lastRecord) {
        assert ShortArray.typeOf(data[0]) == ShortArray.LONG;
        bitsUsedInLastByte = data[1];
        requiredBits = data[2];
        bits = BitBuffer.bitsFromBytes(data, 3);
        int numberOfUsedBitsInRecord = numberOfUsedBitsInRecord((data.length - 3) * Byte.SIZE, lastRecord);
        int numberOfCompleteLabels = numberOfUsedBitsInRecord / requiredBits;
        remainingBits = numberOfUsedBitsInRecord - numberOfCompleteLabels * requiredBits;
        return findLabel(numberOfCompleteLabels, true);
    }

    private boolean processRecord(byte[] data, boolean lastRecord) {
        int numberOfBitsInRecord = remainingBits + (data.length) * Byte.SIZE;
        int numberOfUsedBitsInRecord = numberOfUsedBitsInRecord(numberOfBitsInRecord, lastRecord);
        int numberOfCompleteLabels = numberOfUsedBitsInRecord / requiredBits;
        computeBits(data, numberOfBitsInRecord, numberOfCompleteLabels);
        return findLabel(numberOfCompleteLabels, false);
    }

    private int numberOfUsedBitsInRecord(int numberOfBitsInRecord, boolean lastRecord) {
        int bitsUsedInLastByteInThisRecord = lastRecord ? bitsUsedInLastByte : Byte.SIZE;
        return numberOfBitsInRecord - (Byte.SIZE - bitsUsedInLastByteInThisRecord);
    }

    private void computeBits(byte[] data, int totalNumberOfBits, int numberOfCompleteLabels) {
        if (remainingBits > 0) {
            BitBuffer newBits = BitBuffer.bits((int) Math.ceil((totalNumberOfBits + remainingBits) / 8.0));
            newBits.put(bits.getLong(remainingBits), remainingBits);
            newBits.put(data, 0, data.length);
            bits = newBits;
        } else {
            bits = BitBuffer.bitsFromBytes(data);
        }
        remainingBits = totalNumberOfBits - numberOfCompleteLabels * requiredBits;
    }

    private boolean findLabel(int numberOfCompleteLabels, boolean skipFirst) {
        for (int i = 0; i < numberOfCompleteLabels; i++) {
            long foundLabel = bits.getLong(requiredBits);
            // first item of the first record is the node id
            if (skipFirst && i == 0) {
                continue;
            }
            if (foundLabel == label) {
                found = true;
                return false;
            }
        }
        return true;
    }
}
