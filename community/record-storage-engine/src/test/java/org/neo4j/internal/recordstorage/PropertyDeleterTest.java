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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordBuilders.createPropertyChain;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_ARRAY_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.common.TokenNameLookup;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.AbstractDynamicStore;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.NodeStore;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.LogAssertions;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@ExtendWith(RandomExtension.class)
@EphemeralPageCacheExtension
class PropertyDeleterTest {
    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private RandomSupport random;

    private final PropertyTraverser traverser = new PropertyTraverser();
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private NeoStores neoStores;
    private PropertyDeleter deleter;
    private PropertyStore propertyStore;
    private DefaultIdGeneratorFactory idGeneratorFactory;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    private void startStore(boolean log) {
        Config config = Config.defaults(GraphDatabaseInternalSettings.log_inconsistent_data_deletion, log);
        var pageCacheTracer = PageCacheTracer.NULL;
        idGeneratorFactory =
                new DefaultIdGeneratorFactory(directory.getFileSystem(), immediate(), pageCacheTracer, "db");
        neoStores = new StoreFactory(
                        DatabaseLayout.ofFlat(directory.homePath()),
                        config,
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        directory.getFileSystem(),
                        NullLogProvider.getInstance(),
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        propertyStore = neoStores.getPropertyStore();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
        deleter = new PropertyDeleter(
                traverser,
                this.neoStores,
                new TokenNameLookup() {
                    @Override
                    public String labelGetName(int labelId) {
                        return "" + labelId;
                    }

                    @Override
                    public String relationshipTypeGetName(int relationshipTypeId) {
                        return "" + relationshipTypeId;
                    }

                    @Override
                    public String propertyKeyGetName(int propertyKeyId) {
                        return "" + propertyKeyId;
                    }
                },
                logProvider,
                config,
                NULL_CONTEXT,
                storeCursors);
    }

    @AfterEach
    void stopStore() {
        storeCursors.close();
        neoStores.close();
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldHandlePropertyChainDeletionOnCycle(boolean log) {
        // given
        startStore(log);
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.newRecord();
        node.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        List<PropertyBlock> properties = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            properties.add(encodedValue(i, random.nextValue()));
        }
        DirectRecordAccessSet initialChanges =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        long firstPropId = createPropertyChain(propertyStore, node, properties, initialChanges.getPropertyRecords());
        node.setNextProp(firstPropId);
        initialChanges.commit(); // should update all the changed records directly into the store

        // create a cycle in the property chain A -> B
        //                                      ^---v
        List<Value> valuesInTheFirstTwoRecords = new ArrayList<>();
        PropertyRecord firstPropRecord = getRecord(propertyStore, firstPropId, NORMAL);
        readValuesFromPropertyRecord(firstPropRecord, valuesInTheFirstTwoRecords);
        long secondPropId = firstPropRecord.getNextProp();
        PropertyRecord secondPropRecord = getRecord(propertyStore, secondPropId, NORMAL);
        readValuesFromPropertyRecord(secondPropRecord, valuesInTheFirstTwoRecords);
        secondPropRecord.setNextProp(firstPropId);
        try (var cursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            propertyStore.updateRecord(secondPropRecord, cursor, NULL_CONTEXT, storeCursors);
        }

        // when
        DirectRecordAccessSet changes =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        deleter.deletePropertyChain(node, changes.getPropertyRecords(), INSTANCE);
        changes.commit();

        // then
        assertEquals(Record.NO_NEXT_PROPERTY.longValue(), node.getNextProp());
        assertFalse(getRecord(propertyStore, firstPropId, CHECK).inUse());
        assertFalse(getRecord(propertyStore, secondPropId, CHECK).inUse());
        assertLogContains("Deleted inconsistent property chain with cycle", log);
        if (log) {
            for (Value value : valuesInTheFirstTwoRecords) {
                assertLogContains(value.toString(), true);
            }
        }
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldHandlePropertyChainDeletionOnUnusedRecord(boolean log) {
        // given
        startStore(log);
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.newRecord();
        node.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        List<PropertyBlock> properties = new ArrayList<>();
        for (int i = 0; i < 20; i++) {
            properties.add(encodedValue(i, random.nextValue()));
        }
        DirectRecordAccessSet initialChanges =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        long firstPropId = createPropertyChain(propertyStore, node, properties, initialChanges.getPropertyRecords());
        node.setNextProp(firstPropId);
        initialChanges.commit(); // should update all the changed records directly into the store

        // create a cycle in the property chain A -> B
        //                                      ^---v
        List<Value> valuesInTheFirstTwoRecords = new ArrayList<>();
        PropertyRecord firstPropRecord = getRecord(propertyStore, firstPropId, NORMAL);
        readValuesFromPropertyRecord(firstPropRecord, valuesInTheFirstTwoRecords);
        long secondPropId = firstPropRecord.getNextProp();
        PropertyRecord secondPropRecord = getRecord(propertyStore, secondPropId, NORMAL);
        readValuesFromPropertyRecord(secondPropRecord, valuesInTheFirstTwoRecords);
        long thirdPropId = secondPropRecord.getNextProp();
        PropertyRecord thirdPropRecord = getRecord(propertyStore, thirdPropId, NORMAL);
        thirdPropRecord.setInUse(false);
        try (var cursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            propertyStore.updateRecord(thirdPropRecord, cursor, NULL_CONTEXT, storeCursors);
        }

        // when
        DirectRecordAccessSet changes =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        deleter.deletePropertyChain(node, changes.getPropertyRecords(), INSTANCE);
        changes.commit();

        // then
        assertEquals(Record.NO_NEXT_PROPERTY.longValue(), node.getNextProp());
        assertFalse(getRecord(propertyStore, firstPropId, CHECK).inUse());
        assertFalse(getRecord(propertyStore, secondPropId, CHECK).inUse());
        assertLogContains("Deleted inconsistent property chain with unused record", log);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldHandlePropertyChainDeletionOnDynamicRecordCycle(boolean log) {
        // given
        startStore(log);
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.newRecord();
        node.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        List<PropertyBlock> properties =
                Collections.singletonList(encodedValue(0, random.randomValues().nextAsciiTextValue(1000, 1000)));
        DirectRecordAccessSet initialChanges =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        long firstPropId = createPropertyChain(propertyStore, node, properties, initialChanges.getPropertyRecords());
        node.setNextProp(firstPropId);
        initialChanges.commit(); // should update all the changed records directly into the store

        // create a cycle in the dynamic record chain cycle
        PropertyRecord firstPropRecord = getRecord(propertyStore, firstPropId, NORMAL);
        PropertyBlock dynamicBlock = Iterables.first(firstPropRecord.propertyBlocks());
        createCycleIn(dynamicBlock);

        // when
        DirectRecordAccessSet changes =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        deleter.deletePropertyChain(node, changes.getPropertyRecords(), INSTANCE);
        changes.commit();

        // then
        assertEquals(Record.NO_NEXT_PROPERTY.longValue(), node.getNextProp());
        assertLogContains("Deleted inconsistent property chain", log);
    }

    @ValueSource(booleans = {true, false})
    @ParameterizedTest
    void shouldHandlePropertyChainDeletionOnUnusedDynamicRecord(boolean log) {
        // given
        startStore(log);
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.newRecord();
        node.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        List<PropertyBlock> properties =
                Collections.singletonList(encodedValue(0, random.randomValues().nextAsciiTextValue(1000, 1000)));
        DirectRecordAccessSet initialChanges =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        long firstPropId = createPropertyChain(propertyStore, node, properties, initialChanges.getPropertyRecords());
        node.setNextProp(firstPropId);
        initialChanges.commit(); // should update all the changed records directly into the store

        PropertyRecord firstPropRecord = getRecord(propertyStore, firstPropId, NORMAL);
        PropertyBlock dynamicBlock = Iterables.first(firstPropRecord.propertyBlocks());
        makeSomeUnusedIn(dynamicBlock);

        // when
        DirectRecordAccessSet changes =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        deleter.deletePropertyChain(node, changes.getPropertyRecords(), INSTANCE);
        changes.commit();

        // then
        assertEquals(Record.NO_NEXT_PROPERTY.longValue(), node.getNextProp());
        assertLogContains("Deleted inconsistent property chain with unused record", log);
    }

    @Test
    void shouldLogAsManyPropertiesAsPossibleEvenThoSomeDynamicChainsAreBroken() {
        // given
        startStore(true);
        NodeStore nodeStore = neoStores.getNodeStore();
        NodeRecord node = nodeStore.newRecord();
        node.setId(nodeStore.getIdGenerator().nextId(NULL_CONTEXT));
        List<PropertyBlock> properties = new ArrayList<>();
        // We're not tampering with our "small" values and they should therefore be readable and logged during deletion
        Value value1 = randomValueWithMaxSingleDynamicRecord();
        Value value2 = randomValueWithMaxSingleDynamicRecord();
        Value bigValue1 = random.nextAlphaNumericTextValue(1_000, 1_000);
        Value bigValue2 = randomLargeLongArray();
        Value value3 = randomValueWithMaxSingleDynamicRecord();
        properties.add(encodedValue(0, value1));
        properties.add(encodedValue(1, value2));
        properties.add(encodedValue(2, bigValue1));
        properties.add(encodedValue(3, bigValue2));
        properties.add(encodedValue(4, value3));
        DirectRecordAccessSet initialChanges =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        long firstPropId = createPropertyChain(propertyStore, node, properties, initialChanges.getPropertyRecords());
        node.setNextProp(firstPropId);
        initialChanges.commit(); // should update all the changed records directly into the store

        List<PropertyBlock> blocksWithDynamicValueRecords = getBlocksContainingDynamicValueRecords(firstPropId);
        makeSomeUnusedIn(random.among(blocksWithDynamicValueRecords));
        createCycleIn(random.among(blocksWithDynamicValueRecords));

        // when
        DirectRecordAccessSet changes =
                new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT, EmptyMemoryTracker.INSTANCE);
        deleter.deletePropertyChain(node, changes.getPropertyRecords(), INSTANCE);
        changes.commit();

        // then
        assertEquals(Record.NO_NEXT_PROPERTY.longValue(), node.getNextProp());
        assertLogContains("Deleted inconsistent property chain", true);
        assertLogContains(value1.toString(), true);
        assertLogContains(value2.toString(), true);
        assertLogContains(value3.toString(), true);
    }

    private Value randomValueWithMaxSingleDynamicRecord() {
        Value value;
        PropertyBlock encoded;
        do {
            value = random.nextValue();
            encoded = encodedValue(0, value);
        } while (encoded.getValueRecords().size() > 1);
        return value;
    }

    private List<PropertyBlock> getBlocksContainingDynamicValueRecords(long firstPropId) {
        long propId = firstPropId;
        List<PropertyBlock> blocks = new ArrayList<>();
        try (PageCursor cursor = propertyStore.openPageCursorForReading(propId, NULL_CONTEXT)) {
            PropertyRecord record = propertyStore.newRecord();
            while (!Record.NO_NEXT_PROPERTY.is(propId)) {
                propertyStore.getRecordByCursor(propId, record, NORMAL, cursor, EmptyMemoryTracker.INSTANCE);
                propertyStore.ensureHeavy(record, storeCursors, EmptyMemoryTracker.INSTANCE);
                for (PropertyBlock block : record.propertyBlocks()) {
                    if (block.getValueRecords().size() > 1) {
                        blocks.add(block);
                    }
                }
                propId = record.getNextProp();
            }
        }
        return blocks;
    }

    private void makeSomeUnusedIn(PropertyBlock dynamicBlock) {
        propertyStore.ensureHeavy(dynamicBlock, storeCursors, EmptyMemoryTracker.INSTANCE);
        DynamicRecord valueRecord = dynamicBlock
                .getValueRecords()
                .get(random.nextInt(dynamicBlock.getValueRecords().size()));
        PropertyType type = valueRecord.getType();
        valueRecord.setInUse(false);
        AbstractDynamicStore dynamicStore;
        PageCursor pageCursor;
        if (type == PropertyType.STRING) {
            dynamicStore = propertyStore.getStringStore();
            pageCursor = storeCursors.writeCursor(DYNAMIC_STRING_STORE_CURSOR);
        } else {
            dynamicStore = propertyStore.getArrayStore();
            pageCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR);
        }
        try (pageCursor) {
            dynamicStore.updateRecord(valueRecord, pageCursor, NULL_CONTEXT, storeCursors);
        }
    }

    private void createCycleIn(PropertyBlock dynamicBlock) {
        propertyStore.ensureHeavy(dynamicBlock, storeCursors, EmptyMemoryTracker.INSTANCE);
        int cycleEndIndex = random.nextInt(1, dynamicBlock.getValueRecords().size());
        int cycleStartIndex = random.nextInt(cycleEndIndex);
        DynamicRecord cycleEndRecord = dynamicBlock.getValueRecords().get(cycleEndIndex);
        PropertyType type = cycleEndRecord.getType();
        cycleEndRecord.setNextBlock(
                dynamicBlock.getValueRecords().get(cycleStartIndex).getId());
        AbstractDynamicStore dynamicStore;
        PageCursor pageCursor;
        if (type == PropertyType.STRING) {
            dynamicStore = propertyStore.getStringStore();
            pageCursor = storeCursors.writeCursor(DYNAMIC_STRING_STORE_CURSOR);
        } else {
            dynamicStore = propertyStore.getArrayStore();
            pageCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR);
        }
        try (pageCursor) {
            dynamicStore.updateRecord(cycleEndRecord, pageCursor, NULL_CONTEXT, storeCursors);
        }
    }

    private Value randomLargeLongArray() {
        long[] array = new long[200];
        for (int i = 0; i < array.length; i++) {
            array[i] = random.nextLong();
        }
        return Values.of(array);
    }

    private PropertyBlock encodedValue(int key, Value value) {
        PropertyBlock block = new PropertyBlock();
        PropertyStore.encodeValue(
                block,
                key,
                value,
                allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                NULL_CONTEXT,
                INSTANCE);
        return block;
    }

    private void readValuesFromPropertyRecord(PropertyRecord record, List<Value> values) {
        for (PropertyBlock block : record.propertyBlocks()) {
            values.add(block.getType().value(block, propertyStore, storeCursors, EmptyMemoryTracker.INSTANCE));
        }
    }

    private void assertLogContains(String message, boolean shouldContain) {
        if (shouldContain) {
            LogAssertions.assertThat(logProvider).containsMessages(message);
        } else {
            LogAssertions.assertThat(logProvider).doesNotContainMessage(message);
        }
    }

    private PropertyRecord getRecord(PropertyStore propertyStore, long id, RecordLoad load) {
        try (PageCursor cursor = propertyStore.openPageCursorForReading(id, NULL_CONTEXT)) {
            return propertyStore.getRecordByCursor(
                    id, propertyStore.newRecord(), load, cursor, EmptyMemoryTracker.INSTANCE);
        }
    }
}
