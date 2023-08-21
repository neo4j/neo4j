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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.neo4j.values.storable.Values.stringValue;

import org.neo4j.index.internal.gbptree.Layout;
import org.neo4j.values.storable.TextValue;

public class IndexEntryTestUtil {
    public static <KEY extends NativeIndexKey<KEY>> String generateStringResultingInIndexEntrySize(int size) {
        RangeLayout layout = new RangeLayout(1);
        return generateStringValueResultingInIndexEntrySize(layout, size).stringValue();
    }

    public static <KEY extends NativeIndexKey<KEY>> TextValue generateStringValueResultingInIndexEntrySize(
            Layout<KEY, ?> layout, int size) {
        TextValue value;
        KEY key = layout.newKey();
        key.initialize(0);
        int stringLength = size;
        do {
            value = stringValue("A".repeat(stringLength--));
            key.initFromValue(0, value, NativeIndexKey.Inclusion.NEUTRAL);
        } while (layout.keySize(key) > size);
        assertEquals(size, layout.keySize(key));
        return value;
    }
}
