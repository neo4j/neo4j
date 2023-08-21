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

import static org.neo4j.values.storable.Values.stringValue;

import java.util.Iterator;
import org.neo4j.index.internal.gbptree.Seeker;

class ResultCursor implements Seeker<RangeKey, NullValue> {
    private final Iterator<String> iterator;
    private int pos = -1;
    private RangeKey key;

    ResultCursor(Iterator<String> keys) {
        iterator = keys;
    }

    @Override
    public boolean next() {
        if (iterator.hasNext()) {
            String current = iterator.next();
            pos++;
            key = new RangeKey();
            key.initialize(pos);
            key.initFromValue(0, stringValue(current), NativeIndexKey.Inclusion.NEUTRAL);
            return true;
        }
        return false;
    }

    @Override
    public void close() {
        // do nothing
    }

    @Override
    public RangeKey key() {
        return key;
    }

    @Override
    public NullValue value() {
        return NullValue.INSTANCE;
    }
}
