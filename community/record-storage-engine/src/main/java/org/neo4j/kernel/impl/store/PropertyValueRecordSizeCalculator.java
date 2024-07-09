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
package org.neo4j.kernel.impl.store;

import static java.lang.Math.toIntExact;

import org.neo4j.batchimport.api.input.PropertySizeCalculator;
import org.neo4j.internal.id.BatchingIdSequence;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.values.storable.Value;

/**
 * Calculates record size that property values will occupy if encoded into a {@link PropertyStore}.
 * Contains state and is designed for multiple uses from a single thread only.
 * Does actual encoding of property values, dry-run style.
 */
public class PropertyValueRecordSizeCalculator implements PropertySizeCalculator {
    private final BatchingIdSequence stringRecordIds = new BatchingIdSequence();
    private final DynamicRecordAllocator stringRecordCounter;
    private final BatchingIdSequence arrayRecordIds = new BatchingIdSequence();
    private final DynamicRecordAllocator arrayRecordCounter;

    private final int propertyRecordSize;
    private final int stringRecordSize;
    private final int arrayRecordSize;

    public PropertyValueRecordSizeCalculator(PropertyStore propertyStore) {
        this(
                propertyStore.getRecordSize(),
                propertyStore.getStringStore().getRecordSize(),
                propertyStore.getStringStore().getRecordDataSize(),
                propertyStore.getArrayStore().getRecordSize(),
                propertyStore.getArrayStore().getRecordDataSize());
    }

    public PropertyValueRecordSizeCalculator(
            int propertyRecordSize,
            int stringRecordSize,
            int stringRecordDataSize,
            int arrayRecordSize,
            int arrayRecordDataSize) {
        this.propertyRecordSize = propertyRecordSize;
        this.stringRecordSize = stringRecordSize;
        this.arrayRecordSize = arrayRecordSize;
        this.stringRecordCounter = new StandardDynamicRecordAllocator(stringRecordIds, stringRecordDataSize);
        this.arrayRecordCounter = new StandardDynamicRecordAllocator(arrayRecordIds, arrayRecordDataSize);
    }

    @Override
    public int calculateSize(Value[] values, CursorContext cursorContext, MemoryTracker memoryTracker) {
        stringRecordIds.reset();
        arrayRecordIds.reset();

        int propertyRecordsUsed = 0;
        int freeBlocksInCurrentRecord = 0;
        for (Value value : values) {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    0 /*doesn't matter*/,
                    value,
                    stringRecordCounter,
                    arrayRecordCounter,
                    cursorContext,
                    memoryTracker);
            if (block.getValueBlocks().length > freeBlocksInCurrentRecord) {
                propertyRecordsUsed++;
                freeBlocksInCurrentRecord = PropertyType.getPayloadSizeLongs();
            }
            freeBlocksInCurrentRecord -= block.getValueBlocks().length;
        }

        int size = propertyRecordsUsed * propertyRecordSize;
        size += toIntExact(stringRecordIds.peek()) * stringRecordSize;
        size += toIntExact(arrayRecordIds.peek()) * arrayRecordSize;
        return size;
    }
}
