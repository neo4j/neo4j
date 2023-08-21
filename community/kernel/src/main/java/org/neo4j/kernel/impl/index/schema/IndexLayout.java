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
package org.neo4j.kernel.impl.index.schema;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.io.pagecache.PageCursor;

abstract class IndexLayout<KEY extends NativeIndexKey<KEY>> extends Layout.Adapter<KEY, NullValue> {
    // allows more control of the identifier, needed for legacy reasons for the two number layouts
    IndexLayout(boolean fixedSize, long identifier, int majorVersion, int minorVersion) {
        super(fixedSize, identifier, majorVersion, minorVersion);
    }

    IndexLayout(boolean fixedSize, String layoutName, int majorVersion, int minorVersion) {
        this(fixedSize, Layout.namedIdentifier(layoutName, NullValue.SIZE), majorVersion, minorVersion);
    }

    @Override
    public NullValue newValue() {
        return NullValue.INSTANCE;
    }

    @Override
    public int valueSize(NullValue nullValue) {
        return NullValue.SIZE;
    }

    @Override
    public void writeValue(PageCursor cursor, NullValue nullValue) {
        // nothing to write
    }

    @Override
    public void readValue(PageCursor cursor, NullValue into, int valueSize) {
        // nothing to read
    }

    @Override
    public final int compare(KEY o1, KEY o2) {
        int valueComparison = compareValue(o1, o2);
        if (valueComparison == 0) {
            // This is a special case where we need also compare entityId to support inclusive/exclusive
            if (o1.getCompareId() && o2.getCompareId()) {
                return Long.compare(o1.getEntityId(), o2.getEntityId());
            }
        }
        return valueComparison;
    }

    int compareValue(KEY o1, KEY o2) {
        return o1.compareValueTo(o2);
    }
}
