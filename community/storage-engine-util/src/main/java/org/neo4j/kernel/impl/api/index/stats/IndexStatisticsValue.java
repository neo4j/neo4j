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

class IndexStatisticsValue {
    static final int NUM_LONGS = 4;
    static final int SIZE = Long.SIZE * NUM_LONGS;

    static final int INDEX_SAMPLE_UNIQUE_VALUES = 0;
    static final int INDEX_SAMPLE_SIZE = 1;
    static final int INDEX_SAMPLE_UPDATES_COUNT = 2;
    static final int INDEX_SAMPLE_INDEX_SIZE = 3;

    static final int INDEX_USAGE_LAST_READ = 0;
    static final int INDEX_USAGE_READ_COUNT = 1;
    static final int INDEX_USAGE_TRACKED_SINCE = 2;

    final long[] data = new long[NUM_LONGS];

    IndexStatisticsValue() {}

    void set(int dataIndex, long value) {
        this.data[dataIndex] = value;
    }

    long get(int dataIndex) {
        return data[dataIndex];
    }

    IndexStatisticsValue copy() {
        var copy = new IndexStatisticsValue();
        System.arraycopy(data, 0, copy.data, 0, NUM_LONGS);
        return copy;
    }

    void write(PageCursor cursor) {
        for (int i = 0; i < IndexStatisticsValue.NUM_LONGS; i++) {
            cursor.putLong(data[i]);
        }
    }

    void read(PageCursor cursor) {
        for (int i = 0; i < IndexStatisticsValue.NUM_LONGS; i++) {
            data[i] = cursor.getLong();
        }
    }
}
