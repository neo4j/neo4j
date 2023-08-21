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

import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import org.neo4j.index.internal.gbptree.GBPTree;
import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

/**
 * {@link Layout} for a {@link GBPTree} writing and reading the ID ranges that make up the contents of an {@link IndexedIdGenerator}.
 */
public class IdRangeLayout extends Layout.Adapter<IdRangeKey, IdRange> {
    private final int longsPerEntry;
    private final int idsPerEntry;

    public IdRangeLayout(int idsPerEntry) {
        super(true, 3_735_929_054L + idsPerEntry, 1, 2);
        this.idsPerEntry = idsPerEntry;
        requirePowerOfTwo(idsPerEntry);
        this.longsPerEntry = ((idsPerEntry - 1) / (IdRange.BITSET_SIZE)) + 1;
    }

    @Override
    public IdRangeKey newKey() {
        return new IdRangeKey(0);
    }

    @Override
    public IdRangeKey copyKey(IdRangeKey key, IdRangeKey into) {
        into.setIdRangeIdx(key.getIdRangeIdx());
        return into;
    }

    @Override
    public IdRange newValue() {
        return new IdRange(longsPerEntry, idsPerEntry);
    }

    @Override
    public int keySize(IdRangeKey key) {
        // idRangeIdx
        return Long.BYTES;
    }

    @Override
    public int valueSize(IdRange ignore) {
        // generation + state bit-sets
        return Long.BYTES + longsPerEntry * Long.BYTES * IdRange.BITSET_COUNT;
    }

    @Override
    public void writeKey(PageCursor cursor, IdRangeKey key) {
        cursor.putLong(key.getIdRangeIdx());
    }

    @Override
    public void writeValue(PageCursor cursor, IdRange value) {
        cursor.putLong(value.getGeneration());
        writeLongs(cursor, value.getBitSets());
    }

    @Override
    public void readKey(PageCursor cursor, IdRangeKey into, int keySize) {
        into.setIdRangeIdx(cursor.getLong());
    }

    @Override
    public void readValue(PageCursor cursor, IdRange into, int ignore) {
        into.setGeneration(cursor.getLong());
        readLongs(cursor, into.getBitSets());
    }

    @Override
    public void initializeAsLowest(IdRangeKey idRangeKey) {
        idRangeKey.setIdRangeIdx(Long.MIN_VALUE);
    }

    @Override
    public void initializeAsHighest(IdRangeKey idRangeKey) {
        idRangeKey.setIdRangeIdx(Long.MAX_VALUE);
    }

    private static void writeLongs(PageCursor cursor, long[][] groups) {
        for (long[] group : groups) {
            for (long bits : group) {
                cursor.putLong(bits);
            }
        }
    }

    private static void readLongs(PageCursor cursor, long[][] groups) {
        for (int i = 0; i < groups.length; i++) {
            long[] group = groups[i];
            for (int ii = 0; ii < group.length; ii++) {
                group[ii] = cursor.getLong();
            }
        }
    }

    @Override
    public int compare(IdRangeKey o1, IdRangeKey o2) {
        return Long.compare(o1.getIdRangeIdx(), o2.getIdRangeIdx());
    }

    long idRangeIndex(long id) {
        return id / idsPerEntry;
    }

    int idsPerEntry() {
        return idsPerEntry;
    }
}
