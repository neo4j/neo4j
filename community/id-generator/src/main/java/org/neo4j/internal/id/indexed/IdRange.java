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
package org.neo4j.internal.id.indexed;

import static java.lang.Long.toBinaryString;
import static java.lang.String.format;
import static java.util.Arrays.fill;
import static org.neo4j.internal.helpers.Numbers.log2floor;

import java.util.Arrays;
import org.apache.commons.lang3.StringUtils;
import org.neo4j.util.VisibleForTesting;

/**
 * Value in a GB+Tree for indexing id states. Accompanies that with a generation, i.e. which generation this value were written in.
 * ID states are kept in three bit-sets, each consisting of one or more {@code long}s. The three bit-sets are:
 * <ul>
 *     <li>Commit bits, i.e. 0=used, 1=unused</li>
 *     <li>Reuse bits, i.e. 0=not reusable, 1=free</li>
 *     <li>Reserved bits, i.e. 0=not reserved, 1=reserved</li>
 * </ul>
 *
 * Each {@link IdRange} is associated with an {@link IdRangeKey} which specifies the range, e.g. an ID range of 3 in a layout where ids-per-entry is 128
 * holds IDs between 384-511.
 */
class IdRange {
    static final int BITSET_COUNT = 3;

    static final int BITSET_COMMIT = 0;
    static final int BITSET_REUSE = 1;
    static final int BITSET_RESERVED = 2;
    static final int BITSET_ALL = -1;

    static final int BITSET_SIZE = Long.SIZE;

    static final int BITSET_AND_MASK = BITSET_SIZE - 1;
    static final int BITSET_SHIFT = log2floor(BITSET_SIZE);
    static final byte ADDITION_COMMIT = 1 << BITSET_COMMIT;
    static final byte ADDITION_REUSE = 1 << BITSET_REUSE;
    static final byte ADDITION_RESERVED = 1 << BITSET_RESERVED;
    static final byte ADDITION_ALL = ADDITION_COMMIT | ADDITION_REUSE | ADDITION_RESERVED;

    private long generation;
    private byte addition;
    private final long[][] bitSets;
    private final int numOfLongs;
    private final int idsPerEntry;

    IdRange(int numOfLongs, int idsPerEntry) {
        this.bitSets = new long[BITSET_COUNT][numOfLongs];
        this.numOfLongs = numOfLongs;
        this.idsPerEntry = idsPerEntry;
    }

    @VisibleForTesting
    IdRange(int numOfLongs) {
        this(numOfLongs, numOfLongs * BITSET_SIZE);
    }

    IdState getState(int n) {
        int longIndex = n >> BITSET_SHIFT;
        int bitIndex = n & BITSET_AND_MASK;
        boolean commitBit = (bitSets[BITSET_COMMIT][longIndex] & bitMask(bitIndex)) != 0;
        if (commitBit) {
            boolean reuseBit = (bitSets[BITSET_REUSE][longIndex] & bitMask(bitIndex)) != 0;
            boolean reservedBit = (bitSets[BITSET_RESERVED][longIndex] & bitMask(bitIndex)) != 0;
            return reuseBit && !reservedBit ? IdState.FREE : IdState.DELETED;
        }
        return IdState.USED;
    }

    private static long bitMask(int bitIndex) {
        return 1L << bitIndex;
    }

    void setBits(int type, int offset, int numberOfIds) {
        int bitSetIndex = offset >> BITSET_SHIFT;
        int bitIndex = offset & BITSET_AND_MASK;
        if (numberOfIds == 1) {
            updateBitSet(type, bitSetIndex, bitMask(bitIndex));
        } else {
            int endLongIndex = (offset + numberOfIds - 1) >> BITSET_SHIFT;
            for (int longIndex = bitSetIndex; longIndex <= endLongIndex; longIndex++, bitIndex = 0) {
                int numBitsInThisLong = Math.min(BITSET_SIZE - bitIndex, numberOfIds);
                long mask = numBitsInThisLong == BITSET_SIZE ? -1 : ((1L << numBitsInThisLong) - 1) << bitIndex;
                updateBitSet(type, longIndex, mask);
                numberOfIds -= numBitsInThisLong;
            }
        }
    }

    private void updateBitSet(int type, int bitSetIndex, long mask) {
        if (type == BITSET_ALL) {
            bitSets[BITSET_COMMIT][bitSetIndex] |= mask;
            bitSets[BITSET_REUSE][bitSetIndex] |= mask;
            bitSets[BITSET_RESERVED][bitSetIndex] |= mask;
        } else {
            bitSets[type][bitSetIndex] |= mask;
        }
    }

    void clear(long generation, boolean allAdditions) {
        clear(generation, allAdditions ? ADDITION_ALL : 0);
    }

    void clear(long generation, byte addition) {
        this.generation = generation;
        this.addition = addition;
        fill(bitSets[BITSET_COMMIT], 0);
        fill(bitSets[BITSET_REUSE], 0);
        fill(bitSets[BITSET_RESERVED], 0);
    }

    private static boolean isAddition(byte addition, int bitSet) {
        return (addition & (1 << bitSet)) != 0;
    }

    long getGeneration() {
        return generation;
    }

    void setGeneration(long generation) {
        this.generation = generation;
    }

    long[][] getBitSets() {
        return bitSets;
    }

    void normalize() {
        for (int i = 0; i < numOfLongs; i++) {
            // Set the reuse bits to whatever the commit bits are. This will let USED be USED and DELETED will become
            // FREE
            bitSets[BITSET_REUSE][i] = bitSets[BITSET_COMMIT][i];
            bitSets[BITSET_RESERVED][i] = 0;
        }
    }

    void mergeFrom(IdRangeKey key, IdRange other, boolean recoveryMode) {
        if (!recoveryMode) {
            verifyMerge(key, other);
        }

        for (int bitSetIndex = 0; bitSetIndex < BITSET_COUNT; bitSetIndex++) {
            mergeBitSet(bitSets[bitSetIndex], other.bitSets[bitSetIndex], isAddition(other.addition, bitSetIndex));
        }
    }

    private static void mergeBitSet(long[] into, long[] mergeFrom, boolean addition) {
        for (int i = 0; i < into.length; i++) {
            into[i] = addition ? into[i] | mergeFrom[i] : into[i] & ~mergeFrom[i];
        }
    }

    void visitFreeIds(long baseId, long generation, FreeIdVisitor visitor) {
        var differentGeneration = generation != this.generation;
        var firstFreeI = -1;
        var prevFreeI = -1;
        var baseI = 0;

        // If we're looking in a range w/ current generation it's more efficient to let the REUSE bits dictate
        // which bits we're looking at because they are a subset of the COMMIT bits,
        // and otherwise we have to look at the COMMIT bits.
        var primaryBitSet = differentGeneration ? BITSET_COMMIT : BITSET_REUSE;
        var secondaryBitSet = differentGeneration ? -1 : BITSET_COMMIT;
        for (var i = 0; i < numOfLongs; i++, baseI += BITSET_SIZE) {
            var primaryBits = bitSets[primaryBitSet][i];
            var secondaryBits = secondaryBitSet == -1 ? -1L : bitSets[secondaryBitSet][i];
            var reservedBits = bitSets[BITSET_RESERVED][i];
            while (primaryBits != 0) {
                var bit = Long.lowestOneBit(primaryBits);
                if (differentGeneration || (secondaryBits & bit) != 0 && (reservedBits & bit) == 0) {
                    var localBitIndex = Long.numberOfTrailingZeros(bit);
                    var bitIndex = baseI + localBitIndex;
                    if (firstFreeI == -1) {
                        firstFreeI = prevFreeI = bitIndex;
                    } else if (prevFreeI == bitIndex - 1) {
                        prevFreeI = bitIndex;
                    } else {
                        // Here's an ID
                        var id = baseId + firstFreeI;
                        var numberOfIds = prevFreeI - firstFreeI + 1;
                        if (!visitor.visitFreeId(id, numberOfIds)) {
                            return;
                        }
                        firstFreeI = prevFreeI = bitIndex;
                    }
                }
                primaryBits ^= bit;
            }
        }

        if (firstFreeI != -1) {
            // And visit the last free ID too
            var id = baseId + firstFreeI;
            var numberOfIds = prevFreeI - firstFreeI + 1;
            visitor.visitFreeId(id, numberOfIds);
        }
    }

    private void verifyMerge(IdRangeKey key, IdRange other) {
        if (!isAddition(other.addition, BITSET_COMMIT)) {
            return;
        }
        long[] intoBitSet = bitSets[BITSET_COMMIT];
        long[] fromBitSet = other.bitSets[BITSET_COMMIT];
        for (int i = 0; i < intoBitSet.length; i++) {
            long into = intoBitSet[i];
            long from = fromBitSet[i];
            if ((into & from) != 0) {
                long rangeFirstId = key.getIdRangeIdx() * idsPerEntry;
                long firstId = rangeFirstId + (long) i * BITSET_SIZE;
                long lastId = Long.min(firstId + BITSET_SIZE, rangeFirstId + idsPerEntry) - 1;
                throw new IllegalIdTransitionException(key.getIdRangeIdx(), firstId, lastId, into, from);
            }
            // don't verify removal since we can't quite verify transitioning to USED since 0 is the default bit value
        }
    }

    static String toPaddedBinaryString(long bits) {
        char[] padded =
                StringUtils.leftPad(toBinaryString(bits), Long.SIZE, '0').toCharArray();

        // Now add a space between each byte
        int numberOfSpaces = padded.length / Byte.SIZE - 1;
        char[] spaced = new char[padded.length + numberOfSpaces];
        Arrays.fill(spaced, ' ');
        for (int i = 0; i < numberOfSpaces + 1; i++) {
            System.arraycopy(padded, i * Byte.SIZE, spaced, i * Byte.SIZE + i, Byte.SIZE);
        }
        return String.valueOf(spaced);
    }

    @Override
    public String toString() {
        var additionCommit = isAddition(addition, BITSET_COMMIT);
        StringBuilder builder = new StringBuilder(additionCommit ? "+" : "-");
        if (additionCommit != isAddition(addition, BITSET_REUSE)
                || additionCommit != isAddition(addition, BITSET_RESERVED)) {
            builder.append(isAddition(addition, BITSET_REUSE) ? "+" : "-");
            builder.append(isAddition(addition, BITSET_RESERVED) ? "+" : "-");
        }
        builder.append(" gen:").append(generation);
        appendBitSet(builder, bitSets[BITSET_COMMIT], "deleted ");
        appendBitSet(builder, bitSets[BITSET_REUSE], "freed   ");
        appendBitSet(builder, bitSets[BITSET_RESERVED], "reserved");
        return builder.toString();
    }

    private static void appendBitSet(StringBuilder builder, long[] bitSet, String name) {
        builder.append(format("%n")).append(name).append(':');
        String delimiter = "";
        for (int i = bitSet.length - 1; i >= 0; i--) {
            builder.append(delimiter).append(toPaddedBinaryString(bitSet[i]));
            delimiter = " , ";
        }
    }

    public boolean isEmpty() {
        for (long bits : bitSets[BITSET_COMMIT]) {
            if (bits != 0) {
                return false;
            }
        }
        return true;
    }

    enum IdState {
        USED,
        DELETED,
        FREE
    }

    @FunctionalInterface
    interface FreeIdVisitor {
        boolean visitFreeId(long id, int numberOfIds);
    }
}
