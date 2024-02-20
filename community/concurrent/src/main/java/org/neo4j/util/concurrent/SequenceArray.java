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
package org.neo4j.util.concurrent;

import static java.lang.Math.max;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;

import org.neo4j.internal.helpers.Numbers;
import org.neo4j.util.Preconditions;
import org.neo4j.util.concurrent.OutOfOrderSequence.Meta;
import org.neo4j.util.concurrent.OutOfOrderSequence.NumberWithMeta;

public class SequenceArray {
    private static final long UNSET = -1L;
    // This is the backing store, treated as a ring, courtesy of cursor
    private long[] array;
    private Meta[] metas;
    private int cursor;
    private int itemsAhead;
    private int capacityMask;
    private int missingCount;

    SequenceArray(int initialCapacity) {
        Preconditions.requirePowerOfTwo(initialCapacity);
        this.array = new long[initialCapacity];
        this.metas = new Meta[initialCapacity];
        this.missingCount = 0;
        this.capacityMask = initialCapacity - 1;
    }

    public void clear() {
        cursor = 0;
        itemsAhead = 0;
        missingCount = 0;
    }

    void offer(long baseNumber, long number, Meta meta) {
        int diff = (int) (number - baseNumber);
        ensureArrayCapacity(diff);
        int index = cursor + diff - 1;

        // If we offer a value a bit ahead of the last offered value then clear the values in between
        for (int i = cursor + itemsAhead; i < index; i++) {
            array[index(i)] = UNSET;
        }

        // we either close one hole, or add few new holes
        if (diff < itemsAhead) {
            missingCount--;
        } else {
            missingCount += diff - 1 - itemsAhead;
        }
        int absIndex = index(index);
        array[absIndex] = number;
        metas[absIndex] = meta;
        itemsAhead = max(itemsAhead, diff);
    }

    private int index(int logicalIndex) {
        return logicalIndex & capacityMask;
    }

    NumberWithMeta pollHighestGapFree(long given, Meta givenMeta) {
        // when nothing is ahead given is gap free
        if (itemsAhead == 0) {
            return new NumberWithMeta(given, givenMeta);
        }
        // "given" would be placed at cursor
        long number = given;
        int length = itemsAhead - 1;
        missingCount--;
        boolean seenHole = false;
        int absIndex = 0;
        for (int i = 0; i < length; i++) {
            // Advance the cursor first because this method is only assumed to be called when offering the number
            // immediately after
            // the current highest gap-free number
            advanceCursor();
            int tentativeAbsIndex = index(cursor);
            if (array[tentativeAbsIndex] == UNSET) { // we found a gap, return the number before the gap
                seenHole = true;
                break;
            }

            absIndex = tentativeAbsIndex;
            number++;
            assert array[absIndex] == number
                    : "Expected index " + cursor + " to be " + number + ", but was " + array[absIndex]
                            + ". This is for i=" + i;
        }
        if (!seenHole) {
            // we reached the end of the above loop, this means no holes left, last item is gapFree,
            // advance cursor once more so it points to "empty" space with zero items ahead
            advanceCursor();
        }
        return new NumberWithMeta(number, number == given ? givenMeta : metas[absIndex]);
    }

    private void advanceCursor() {
        assert itemsAhead > 0;
        itemsAhead--;
        cursor = advanceCursor(cursor);
    }

    private int advanceCursor(int cursor) {
        return (cursor + 1) & capacityMask;
    }

    private void ensureArrayCapacity(int capacity) {
        if (capacity > array.length) {
            int newCapacity = Numbers.ceilingPowerOfTwo(capacity);
            long[] newArray = new long[newCapacity];
            Meta[] newMetas = new Meta[newCapacity];
            for (int i = 0; i < itemsAhead; i++) {
                int index = index(cursor + i);
                newArray[i] = array[index];
                newMetas[i] = metas[index];
            }
            this.array = newArray;
            this.metas = newMetas;
            this.cursor = 0;
            this.capacityMask = newCapacity - 1;
        }
    }

    @Override
    public String toString() {
        StringBuilder builder = new StringBuilder();
        for (int i = 0; i < itemsAhead; i++) {
            long value = array[index(cursor + i)];
            if (value != UNSET) {
                builder.append(builder.isEmpty() ? "" : ",").append(value);
            }
        }
        return builder.toString();
    }

    long[] missingItems(long gapFree) {
        int count = missingCount;
        if (count == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] missingItems = new long[count];
        int resultCursor = 0;
        for (int i = 0; i < itemsAhead; i++) {
            long value = array[index(cursor + i)];
            if (value == UNSET) {
                missingItems[resultCursor++] = gapFree + i + 1;
                if (resultCursor == missingItems.length) {
                    return missingItems;
                }
            }
        }
        return missingItems;
    }

    long[] snapshot() {
        int snapshotSize = itemsAhead - missingCount;
        if (snapshotSize == 0) {
            return EMPTY_LONG_ARRAY;
        }
        long[] snapshot = new long[snapshotSize];
        int resultCursor = 0;
        for (int i = 0; i < itemsAhead; i++) {
            long value = array[index(cursor + i)];
            if (value != UNSET) {
                snapshot[resultCursor++] = value;
            }
        }
        return snapshot;
    }
}
