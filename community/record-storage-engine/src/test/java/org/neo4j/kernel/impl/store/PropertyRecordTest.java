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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.Iterator;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.values.storable.Values;

class PropertyRecordTest {
    @Test
    void shouldIterateOverBlocks() {
        // GIVEN
        PropertyRecord record = new PropertyRecord(0);
        PropertyBlock[] blocks = new PropertyBlock[3];
        for (int i = 0; i < blocks.length; i++) {
            blocks[i] = new PropertyBlock();
            record.addPropertyBlock(blocks[i]);
        }

        // WHEN
        Iterator<PropertyBlock> iterator = record.propertyBlocks().iterator();

        // THEN
        for (PropertyBlock block : blocks) {
            assertTrue(iterator.hasNext());
            assertEquals(block, iterator.next());
        }
        assertFalse(iterator.hasNext());
    }

    @Test
    void addLoadedBlock() {
        PropertyRecord record = new PropertyRecord(42);

        addBlock(record, 1, 2);
        addBlock(record, 3, 4);

        List<PropertyBlock> blocks = Iterables.asList(record.propertyBlocks());
        assertEquals(2, blocks.size());
        assertEquals(1, blocks.get(0).getKeyIndexId());
        assertEquals(2, blocks.get(0).getSingleValueInt());
        assertEquals(3, blocks.get(1).getKeyIndexId());
        assertEquals(4, blocks.get(1).getSingleValueInt());
    }

    @Test
    void addLoadedBlockFailsWhenTooManyBlocksAdded() {
        PropertyRecord record = new PropertyRecord(42);

        addBlock(record, 1, 2);
        addBlock(record, 3, 4);
        addBlock(record, 5, 6);
        addBlock(record, 7, 8);

        assertThrows(AssertionError.class, () -> addBlock(record, 9, 10));
    }

    private static void addBlock(PropertyRecord record, int key, int value) {
        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue(block, key, Values.of(value), null, null, NULL_CONTEXT, INSTANCE);
        for (long valueBlock : block.getValueBlocks()) {
            record.addLoadedBlock(valueBlock);
        }
    }
}
