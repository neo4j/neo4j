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
package org.neo4j.kernel.impl.api.index.stats;

import org.neo4j.io.pagecache.PageCursor;

/**
 * The type of the key indicate what data the value contains.
 * The type byte is inlined with index id as the most significant byte in the {@link #key}. Key layout:
 * <pre>
 *     0000_0000_0000_0000__0000_0000_0000_0000
 *          |---------------------------------| index id: 7 bytes
 *     |--| key type: 1 byte
 * </pre>
 *
 * The types map to different value data:
 * - {@link #TYPE_SAMPLE}: Value contains
 *      {@link IndexStatisticsValue#INDEX_SAMPLE_UNIQUE_VALUES},
 *      {@link IndexStatisticsValue#INDEX_SAMPLE_SIZE},
 *      {@link IndexStatisticsValue#INDEX_SAMPLE_UPDATES_COUNT}
 *      {@link IndexStatisticsValue#INDEX_SAMPLE_INDEX_SIZE}
 * - {@link #TYPE_USAGE}: Value contains
 *      {@link IndexStatisticsValue#INDEX_USAGE_LAST_READ},
 *      {@link IndexStatisticsValue#INDEX_USAGE_READ_COUNT}
 *      {@link IndexStatisticsValue#INDEX_USAGE_TRACKED_SINCE}
 */
@SuppressWarnings({"NonFinalFieldReferenceInEquals", "NonFinalFieldReferencedInHashCode"})
class IndexStatisticsKey implements Comparable<IndexStatisticsKey> {
    static final int SIZE = Long.SIZE;
    static final byte TYPE_SAMPLE = 0;
    static final byte TYPE_USAGE = 1;

    private static final int NUM_TYPE_BITS = Byte.SIZE;
    private static final int NUM_INDEX_ID_BITS = SIZE - NUM_TYPE_BITS;
    private static final int SHIFT_TYPE_BITS = NUM_INDEX_ID_BITS;
    private static final long MASK_INDEX_ID = (1L << SHIFT_TYPE_BITS) - 1;
    private static final long MASK_TYPE = (1L << NUM_TYPE_BITS) - 1;

    static final long MIN_INDEX_ID = 0;
    static final long MAX_INDEX_ID = (1L << NUM_INDEX_ID_BITS) - 1;

    long key;

    IndexStatisticsKey() {}

    IndexStatisticsKey(long indexId, byte type) {
        set(indexId, type);
    }

    long getIndexId() {
        return key & MASK_INDEX_ID;
    }

    byte getType() {
        return (byte) ((key >>> SHIFT_TYPE_BITS) & MASK_TYPE);
    }

    void set(long indexId, byte type) {
        key = combine(indexId, type);
    }

    @Override
    public int hashCode() {
        return Long.hashCode(key);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        final IndexStatisticsKey that = (IndexStatisticsKey) o;
        return key == that.key;
    }

    @Override
    public String toString() {
        return String.format("[type:%d, indexId:%d]", getType(), getIndexId());
    }

    @Override
    public int compareTo(IndexStatisticsKey other) {
        return Long.compare(key, other.key);
    }

    private static long combine(long indexId, byte type) {
        return indexId | ((((long) type) & MASK_TYPE) << SHIFT_TYPE_BITS);
    }

    void initializeAsLowest() {
        set(IndexStatisticsKey.MIN_INDEX_ID, IndexStatisticsKey.TYPE_SAMPLE);
    }

    void initializeAsHighest() {
        set(IndexStatisticsKey.MAX_INDEX_ID, IndexStatisticsKey.TYPE_USAGE);
    }

    void write(PageCursor cursor) {
        cursor.putLong(key);
    }

    void read(PageCursor cursor) {
        key = cursor.getLong();
    }

    void copyFrom(IndexStatisticsKey source) {
        this.key = source.key;
    }
}
