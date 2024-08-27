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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.List;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.kernel.impl.store.allocator.ReusableRecordsAllocator;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;
import org.opentest4j.TestAbortedException;

class PropertyStoreConsistentReadTest extends RecordStoreConsistentReadTest<PropertyRecord, PropertyStore> {
    @Override
    protected PropertyStore getStore(NeoStores neoStores) {
        return neoStores.getPropertyStore();
    }

    @Override
    protected PageCursor getCursor(StoreCursors storeCursors) {
        return storeCursors.readCursor(PROPERTY_CURSOR);
    }

    @Override
    protected PropertyRecord createNullRecord(long id) {
        PropertyRecord record = new PropertyRecord(id);
        record.setNextProp(0);
        record.setPrevProp(0);
        return record;
    }

    @Override
    protected PropertyRecord createExistingRecord(boolean light) {
        PropertyRecord record = new PropertyRecord(ID);
        record.setId(ID);
        record.setNextProp(2);
        record.setPrevProp(4);
        record.setInUse(true);
        PropertyBlock block = new PropertyBlock();
        DynamicRecordAllocator stringAllocator = new ReusableRecordsAllocator(64, new DynamicRecord(7));
        Value value = Values.of("a string too large to fit in the property block itself");
        PropertyStore.encodeValue(block, 6, value, stringAllocator, null, NULL_CONTEXT, INSTANCE);
        if (light) {
            block.getValueRecords().clear();
        }
        record.addPropertyBlock(block);
        return record;
    }

    @Override
    protected PropertyRecord getLight(long id, PropertyStore store, PageCursor pageCursor) {
        throw new TestAbortedException("Getting a light non-existing property record will throw.");
    }

    @Override
    protected PropertyRecord getHeavy(PropertyStore store, long id, PageCursor pageCursor) {
        PropertyRecord record = super.getHeavy(store, id, pageCursor);
        ensureHeavy(store, record);
        return record;
    }

    private static void ensureHeavy(PropertyStore store, PropertyRecord record) {
        for (PropertyBlock propertyBlock : record) {
            store.ensureHeavy(propertyBlock, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE);
        }
    }

    @Override
    protected void assertRecordsEqual(PropertyRecord actualRecord, PropertyRecord expectedRecord) {
        assertNotNull(actualRecord, "actualRecord");
        assertNotNull(expectedRecord, "expectedRecord");
        assertThat(actualRecord.getDeletedRecords())
                .as("getDeletedRecords")
                .isEqualTo(expectedRecord.getDeletedRecords());
        assertThat(actualRecord.getNextProp()).as("getNextProp").isEqualTo(expectedRecord.getNextProp());
        assertThat(actualRecord.getNodeId()).as("getEntityId").isEqualTo(expectedRecord.getNodeId());
        assertThat(actualRecord.getPrevProp()).as("getPrevProp").isEqualTo(expectedRecord.getPrevProp());
        assertThat(actualRecord.getRelId()).as("getRelId").isEqualTo(expectedRecord.getRelId());
        assertThat(actualRecord.getId()).as("getId").isEqualTo(expectedRecord.getId());
        assertThat(actualRecord.getId()).as("getLongId").isEqualTo(expectedRecord.getId());

        List<PropertyBlock> actualBlocks = Iterables.asList(actualRecord);
        List<PropertyBlock> expectedBlocks = Iterables.asList(expectedRecord);
        assertThat(actualBlocks.size()).as("getPropertyBlocks().size").isEqualTo(expectedBlocks.size());
        for (int i = 0; i < actualBlocks.size(); i++) {
            PropertyBlock actualBlock = actualBlocks.get(i);
            PropertyBlock expectedBlock = expectedBlocks.get(i);
            assertPropertyBlocksEqual(i, actualBlock, expectedBlock);
        }
    }

    private static void assertPropertyBlocksEqual(int index, PropertyBlock actualBlock, PropertyBlock expectedBlock) {
        assertThat(actualBlock.getKeyIndexId())
                .as("[" + index + "]getKeyIndexId")
                .isEqualTo(expectedBlock.getKeyIndexId());
        assertThat(actualBlock.getSingleValueBlock())
                .as("[" + index + "]getSingleValueBlock")
                .isEqualTo(expectedBlock.getSingleValueBlock());
        assertThat(actualBlock.getSingleValueByte())
                .as("[" + index + "]getSingleValueByte")
                .isEqualTo(expectedBlock.getSingleValueByte());
        assertThat(actualBlock.getSingleValueInt())
                .as("[" + index + "]getSingleValueInt")
                .isEqualTo(expectedBlock.getSingleValueInt());
        assertThat(actualBlock.getSingleValueLong())
                .as("[" + index + "]getSingleValueLong")
                .isEqualTo(expectedBlock.getSingleValueLong());
        assertThat(actualBlock.getSingleValueShort())
                .as("[" + index + "]getSingleValueShort")
                .isEqualTo(expectedBlock.getSingleValueShort());
        assertThat(actualBlock.getSize()).as("[" + index + "]getSize").isEqualTo(expectedBlock.getSize());
        assertThat(actualBlock.getType()).as("[" + index + "]getType").isEqualTo(expectedBlock.getType());
        assertThat(actualBlock.isLight()).as("[" + index + "]isLight").isEqualTo(expectedBlock.isLight());

        List<DynamicRecord> actualValueRecords = actualBlock.getValueRecords();
        List<DynamicRecord> expectedValueRecords = expectedBlock.getValueRecords();
        assertThat(actualValueRecords.size())
                .as("[" + index + "]getValueRecords.size")
                .isEqualTo(expectedValueRecords.size());

        for (int i = 0; i < actualValueRecords.size(); i++) {
            DynamicRecord actualValueRecord = actualValueRecords.get(i);
            DynamicRecord expectedValueRecord = expectedValueRecords.get(i);
            assertThat(actualValueRecord.getData())
                    .as("[" + index + "]getValueRecords[" + i + "]getData")
                    .isEqualTo(expectedValueRecord.getData());
            assertThat(actualValueRecord.getLength())
                    .as("[" + index + "]getValueRecords[" + i + "]getLength")
                    .isEqualTo(expectedValueRecord.getLength());
            assertThat(actualValueRecord.getNextBlock())
                    .as("[" + index + "]getValueRecords[" + i + "]getNextBlock")
                    .isEqualTo(expectedValueRecord.getNextBlock());
            assertThat(actualValueRecord.getType())
                    .as("[" + index + "]getValueRecords[" + i + "]getType")
                    .isEqualTo(expectedValueRecord.getType());
            assertThat(actualValueRecord.getId())
                    .as("[" + index + "]getValueRecords[" + i + "]getId")
                    .isEqualTo(expectedValueRecord.getId());
            assertThat(actualValueRecord.getId())
                    .as("[" + index + "]getValueRecords[" + i + "]getLongId")
                    .isEqualTo(expectedValueRecord.getId());
            assertThat(actualValueRecord.isStartRecord())
                    .as("[" + index + "]getValueRecords[" + i + "]isStartRecord")
                    .isEqualTo(expectedValueRecord.isStartRecord());
            assertThat(actualValueRecord.inUse())
                    .as("[" + index + "]getValueRecords[" + i + "]inUse")
                    .isEqualTo(expectedValueRecord.inUse());
        }
    }
}
