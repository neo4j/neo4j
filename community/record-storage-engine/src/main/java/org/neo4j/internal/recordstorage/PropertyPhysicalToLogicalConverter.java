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
package org.neo4j.internal.recordstorage;

import java.util.Arrays;
import java.util.Comparator;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.EntityUpdates;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;

public class PropertyPhysicalToLogicalConverter {
    private static final Comparator<PropertyBlock> BLOCK_COMPARATOR =
            Comparator.comparingInt(PropertyBlock::getKeyIndexId);

    private final PropertyStore propertyStore;
    private final StoreCursors storeCursors;
    private final MemoryTracker memoryTracker;

    private PropertyBlock[] beforeBlocks = new PropertyBlock[8];
    private int beforeBlocksCursor;
    private PropertyBlock[] afterBlocks = new PropertyBlock[8];
    private int afterBlocksCursor;

    public PropertyPhysicalToLogicalConverter(
            PropertyStore propertyStore, StoreCursors storeCursors, MemoryTracker memoryTracker) {
        this.propertyStore = propertyStore;
        this.storeCursors = storeCursors;
        this.memoryTracker = memoryTracker;
    }

    /**
     * Converts physical changes to PropertyRecords for a entity into logical updates
     */
    public void convertPropertyRecord(
            EntityCommandGrouper<?>.Cursor changes, EntityUpdates.Builder properties, CommandSelector commandSelector) {
        mapBlocks(changes, commandSelector);

        int bc = 0;
        int ac = 0;
        while (bc < beforeBlocksCursor || ac < afterBlocksCursor) {
            PropertyBlock beforeBlock = null;
            PropertyBlock afterBlock = null;

            int beforeKey = Integer.MAX_VALUE;
            int afterKey = Integer.MAX_VALUE;
            int key;
            if (bc < beforeBlocksCursor) {
                beforeBlock = beforeBlocks[bc];
                beforeKey = beforeBlock.getKeyIndexId();
            }
            if (ac < afterBlocksCursor) {
                afterBlock = afterBlocks[ac];
                afterKey = afterBlock.getKeyIndexId();
            }

            if (beforeKey < afterKey) {
                afterBlock = null;
                key = beforeKey;
                bc++;
            } else if (beforeKey > afterKey) {
                beforeBlock = null;
                key = afterKey;
                ac++;
            } else {
                // They are the same
                key = afterKey;
                bc++;
                ac++;
            }

            if (beforeBlock != null && afterBlock != null) {
                // CHANGE
                if (!beforeBlock.hasSameContentsAs(afterBlock)) {
                    Value beforeVal = valueOf(beforeBlock);
                    Value afterVal = valueOf(afterBlock);
                    properties.changed(key, beforeVal, afterVal);
                }
            } else {
                // ADD/REMOVE
                if (afterBlock != null) {
                    properties.added(key, valueOf(afterBlock));
                } else {
                    properties.removed(key, valueOf(beforeBlock));
                }
            }
        }
    }

    private void mapBlocks(EntityCommandGrouper<?>.Cursor changes, CommandSelector commandSelector) {
        beforeBlocksCursor = 0;
        afterBlocksCursor = 0;
        while (true) {
            Command.PropertyCommand change = changes.nextProperty();
            if (change == null) {
                break;
            }

            for (PropertyBlock block : commandSelector.getBefore(change)) {
                if (beforeBlocksCursor == beforeBlocks.length) {
                    beforeBlocks = Arrays.copyOf(beforeBlocks, beforeBlocksCursor * 2);
                }
                beforeBlocks[beforeBlocksCursor++] = block;
            }
            for (PropertyBlock block : commandSelector.getAfter(change)) {
                if (afterBlocksCursor == afterBlocks.length) {
                    afterBlocks = Arrays.copyOf(afterBlocks, afterBlocksCursor * 2);
                }
                afterBlocks[afterBlocksCursor++] = block;
            }
        }
        Arrays.sort(beforeBlocks, 0, beforeBlocksCursor, BLOCK_COMPARATOR);
        Arrays.sort(afterBlocks, 0, afterBlocksCursor, BLOCK_COMPARATOR);
    }

    private Value valueOf(PropertyBlock block) {
        if (block == null) {
            return null;
        }
        return block.getType().value(block, propertyStore, storeCursors, memoryTracker);
    }
}
