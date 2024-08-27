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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import org.junit.jupiter.api.Test;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.util.BitBuffer;

public class HasLabelSubscriberTest {
    private static final int THE_NODE = 13;

    private final DynamicArrayStore labelStore = mock(DynamicArrayStore.class);

    @Test
    void shouldFindLabels() {
        int numberOfLabels = 20;
        DynamicRecord record = record(numberOfLabels);

        for (int label = 0; label < numberOfLabels; label++) {
            HasLabelSubscriber subscriber = labelSubscriberFor(label);
            subscriber.onRecord(record);
            assertTrue(subscriber.hasLabel());
        }

        for (int label = numberOfLabels; label < 2 * numberOfLabels; label++) {
            HasLabelSubscriber subscriber = labelSubscriberFor(label);
            subscriber.onRecord(record);
            assertFalse(subscriber.hasLabel());
        }
    }

    @Test
    void shouldFindLabelInContinuationRecord() {
        int numberOfLabelsPerRecord = 10;
        // record1 has 0,1,2,...9
        DynamicRecord record1 = record(numberOfLabelsPerRecord);
        // record2 has 10, 11, 12
        DynamicRecord record2 = record(numberOfLabelsPerRecord, numberOfLabelsPerRecord, false);
        HasLabelSubscriber subscriber = labelSubscriberFor(numberOfLabelsPerRecord);

        subscriber.onRecord(record1);
        assertFalse(subscriber.hasLabel());
        subscriber.onRecord(record2);
        assertTrue(subscriber.hasLabel());
    }

    @Test
    void shouldNotFindLabelsNotInUse() {
        int numberOfLabels = 20;
        DynamicRecord record = record(numberOfLabels);
        record.setInUse(false);

        for (int label = 0; label < numberOfLabels; label++) {
            HasLabelSubscriber subscriber = labelSubscriberFor(label);
            subscriber.onRecord(record);
            assertFalse(subscriber.hasLabel());
        }
    }

    @Test
    void shouldFindLabelSplitBetweenTwoRecords() {
        // Given record containing nodeId 13(0b1101), and labels 10(0b1010), 11(0b1011), 12(0b1100), and 13(0b1101)
        // where we assume each item requires 5bits, first record contains header, node, 10, 11 and one bit of 12
        // second record contains the rest of 12 and 13
        byte[] record1 = new byte[] {6, 1, 5, 0b0100_1101, 0b0010_1101};
        byte[] record2 = new byte[] {
            (byte) 0b1101_0110, 0b0000_0000,
        };
        HasLabelSubscriber subscriber = labelSubscriberFor(12);

        // when
        DynamicRecord record = new DynamicRecord(1337);
        record.setInUse(true);

        // then
        record.setData(record1);
        record.setNextBlock(789);
        subscriber.onRecord(record);
        assertFalse(subscriber.hasLabel());

        record.setData(record2);
        record.setNextBlock(Record.NO_NEXT_BLOCK.longValue());
        subscriber.onRecord(record);
        assertTrue(subscriber.hasLabel());
    }

    private HasLabelSubscriber labelSubscriberFor(int label) {
        return new HasLabelSubscriber(label, labelStore, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
    }

    private static DynamicRecord record(int numberOfLabels) {
        return record(numberOfLabels, 0, true);
    }

    private static DynamicRecord record(int numberOfLabels, int startLabel, boolean needsHeader) {
        BitBuffer bits;
        if (needsHeader) {
            bits = BitBuffer.bits(3 + (int) Math.ceil(8 * (numberOfLabels + 1) / 8.0));
            bits.put((byte) 6); // ShortArray.LONG
            bits.put((byte) 8); // bits used in last byte
            bits.put((byte) 8); // required bits
            bits.put(THE_NODE, 8);
        } else {
            bits = BitBuffer.bits((int) Math.ceil(8 * numberOfLabels / 8.0));
        }

        for (int label = startLabel; label < startLabel + numberOfLabels; label++) {
            bits.put(label, 8);
        }
        DynamicRecord dynamicRecord = new DynamicRecord(4);
        dynamicRecord.setInUse(true);
        dynamicRecord.setData(bits.asBytes());
        return dynamicRecord;
    }
}
