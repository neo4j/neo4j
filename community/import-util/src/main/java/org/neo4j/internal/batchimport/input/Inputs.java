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
package org.neo4j.internal.batchimport.input;

import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

public class Inputs {
    private Inputs() {}

    public static int calculatePropertySize(
            InputEntity entity,
            PropertySizeCalculator valueSizeCalculator,
            CursorContext cursorContext,
            MemoryTracker memoryTracker) {
        int size = 0;
        int propertyCount = entity.propertyCount();
        if (propertyCount > 0) {
            Value[] values = new Value[propertyCount];
            for (int i = 0; i < propertyCount; i++) {
                Object propertyValue = entity.propertyValue(i);
                values[i] = propertyValue instanceof Value ? (Value) propertyValue : Values.of(propertyValue);
            }
            size += valueSizeCalculator.calculateSize(values, cursorContext, memoryTracker);
        }
        return size;
    }
}
