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
package org.neo4j.internal.counts;

import static java.lang.String.format;

import java.util.function.Function;
import org.neo4j.index.internal.gbptree.GBPTree;

/**
 * Key in a {@link GBPTree} owned by {@link GBPTreeCountsStore}.
 */
public class CountsKey {
    static final int SIZE = Byte.BYTES
            + // type
            Long.BYTES
            + // long for main data
            Integer.BYTES; // int for additional data

    /**
     * Key data layout for this type:
     * <pre>
     * first:  8B txId
     * second: 0
     * </pre>
     */
    private static final byte TYPE_STRAY_TX_ID = 0;

    // Commonly used keys
    static final CountsKey MIN_COUNT = new CountsKey((byte) (TYPE_STRAY_TX_ID + 1), Long.MIN_VALUE, Integer.MIN_VALUE);
    static final CountsKey MAX_COUNT = new CountsKey(Byte.MAX_VALUE, Long.MAX_VALUE, Integer.MAX_VALUE);
    static final CountsKey MIN_STRAY_TX_ID = strayTxId(Long.MIN_VALUE);
    static final CountsKey MAX_STRAY_TX_ID = strayTxId(Long.MAX_VALUE);

    /**
     * Type of key, as defined by "TYPE_" constants in this class.
     */
    byte type;

    /**
     * First 8B of the key data. Depending on {@link #type} these bytes mean different things.
     * Keeping the layout fixed and always 12B (these bytes plus 4B from {@link #second} simplified some aspects of reading, writing and working with the data.
     */
    long first;

    /**
     * Additional 4B of key data. Depending on {@link #type} these bytes mean different things or are unused.
     */
    int second;

    CountsKey() {}

    public CountsKey(byte type, long keyFirst, int keySecond) {
        initialize(type, keyFirst, keySecond);
    }

    public void initialize(byte type, long keyFirst, int keySecond) {
        this.type = type;
        this.first = keyFirst;
        this.second = keySecond;
    }

    static CountsKey strayTxId(long txId) {
        return new CountsKey(TYPE_STRAY_TX_ID, txId, 0);
    }

    int extractHighFirstInt() {
        return (int) (first >>> Integer.SIZE);
    }

    int extractLowFirstInt() {
        return (int) first;
    }

    public long first() {
        return first;
    }

    public int second() {
        return second;
    }

    public int type() {
        return type;
    }

    @Override
    public int hashCode() {
        int result = type;
        result = 31 * result + (int) (first ^ (first >>> 32));
        result = 31 * result + second;
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof CountsKey other)) {
            return false;
        }
        return type == other.type && first == other.first && second == other.second;
    }

    @Override
    public String toString() {
        return toString(key -> format("CountsKey[type:%d, first:%d, second:%d]", key.type, key.first, key.second));
    }

    String toString(Function<CountsKey, String> customTypeToString) {
        if (type == TYPE_STRAY_TX_ID) {
            return format("Stray tx id:%d", first);
        }
        return customTypeToString.apply(this);
    }
}
