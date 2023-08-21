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

class RangeLayout extends IndexLayout<RangeKey> {
    private final int numberOfSlots;

    RangeLayout(int numberOfSlots) {
        super(false, Layout.namedIdentifier("RL", numberOfSlots), 0, 1);
        this.numberOfSlots = numberOfSlots;
    }

    @Override
    public RangeKey newKey() {
        return numberOfSlots == 1
                // An optimized version which has the GenericKeyState built-in w/o indirection
                ? new RangeKey()
                // A version which has an indirection to GenericKeyState[]
                : new CompositeRangeKey(numberOfSlots);
    }

    @Override
    public RangeKey copyKey(RangeKey key, RangeKey into) {
        into.copyFrom(key);
        return into;
    }

    @Override
    public int keySize(RangeKey key) {
        return key.size();
    }

    @Override
    public void writeKey(PageCursor cursor, RangeKey key) {
        key.put(cursor);
    }

    @Override
    public void readKey(PageCursor cursor, RangeKey into, int keySize) {
        into.get(cursor, keySize);
    }

    @Override
    public void minimalSplitter(RangeKey left, RangeKey right, RangeKey into) {
        right.minimalSplitter(left, right, into);
    }

    @Override
    public void initializeAsLowest(RangeKey key) {
        key.initValuesAsLowest();
    }

    @Override
    public void initializeAsHighest(RangeKey key) {
        key.initValuesAsHighest();
    }
}
