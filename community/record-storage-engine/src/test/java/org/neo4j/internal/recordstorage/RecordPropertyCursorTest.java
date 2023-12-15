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

import static java.lang.String.format;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_STRING_STORE_CURSOR;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.LongReference.longReference;
import static org.neo4j.storageengine.api.PropertySelection.ALL_PROPERTIES;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.primitive.MutableIntObjectMap;
import org.eclipse.collections.impl.factory.primitive.IntObjectMaps;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.RepeatedTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.PropertySelection;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.values.storable.Value;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(RandomExtension.class)
public class RecordPropertyCursorTest {
    @Inject
    protected RandomSupport random;

    @Inject
    protected FileSystemAbstraction fs;

    @Inject
    protected PageCache pageCache;

    @Inject
    protected RecordDatabaseLayout databaseLayout;

    protected NeoStores neoStores;
    protected NodeRecord owner;
    protected DefaultIdGeneratorFactory idGeneratorFactory;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void setup() {
        var pageCacheTracer = PageCacheTracer.NULL;
        idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        neoStores = new StoreFactory(
                        databaseLayout,
                        Config.defaults(),
                        idGeneratorFactory,
                        pageCache,
                        pageCacheTracer,
                        fs,
                        getRecordFormats(),
                        NullLogProvider.getInstance(),
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL,
                        Sets.immutable.empty())
                .openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        owner = neoStores.getNodeStore().newRecord();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    protected RecordFormats getRecordFormats() {
        return defaultFormat();
    }

    @AfterEach
    void closeStore() {
        storeCursors.close();
        neoStores.close();
    }

    @RepeatedTest(10)
    void shouldReadPropertyChain() {
        // given
        Value[] values = createValues();
        long firstPropertyId = storeValuesAsPropertyChain(owner, values);

        // when
        assertPropertyChain(values, firstPropertyId, createCursor());
    }

    @Test
    void shouldReuseCursor() {
        // given
        Value[] valuesA = createValues();
        long firstPropertyIdA = storeValuesAsPropertyChain(owner, valuesA);
        Value[] valuesB = createValues();
        long firstPropertyIdB = storeValuesAsPropertyChain(owner, valuesB);

        // then
        RecordPropertyCursor cursor = createCursor();
        assertPropertyChain(valuesA, firstPropertyIdA, cursor);
        assertPropertyChain(valuesB, firstPropertyIdB, cursor);
    }

    @Test
    void closeShouldBeIdempotent() {
        // given
        RecordPropertyCursor cursor = createCursor();

        // when
        cursor.close();

        // then
        cursor.close();
    }

    @Test
    void shouldReturnNothingAfterReset() {
        // given
        long firstPropertyId = storeValuesAsPropertyChain(owner, createValues());

        // then
        try (RecordPropertyCursor cursor = createCursor()) {
            cursor.initNodeProperties(longReference(firstPropertyId), ALL_PROPERTIES, owner.getId());
            cursor.reset();
            assertThat(cursor.next()).isFalse();
        }
    }

    @Test
    void shouldAbortChainTraversalOnLikelyCycle() {
        // given
        Value[] values = createValues(20, 20); // many enough to create multiple records in the chain
        long firstProp = storeValuesAsPropertyChain(owner, values);

        // and a cycle on the second record
        PropertyStore store = neoStores.getPropertyStore();
        PropertyRecord firstRecord = getRecord(store, firstProp, NORMAL);
        long secondProp = firstRecord.getNextProp();
        PropertyRecord secondRecord = getRecord(store, secondProp, NORMAL);
        secondRecord.setNextProp(firstProp);
        try (var cursor = storeCursors.writeCursor(PROPERTY_CURSOR)) {
            store.updateRecord(secondRecord, cursor, NULL_CONTEXT, storeCursors);
        }
        owner.setId(99);

        // when
        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties(longReference(firstProp), ALL_PROPERTIES, owner.getId());
        InconsistentDataReadException e = assertThrows(InconsistentDataReadException.class, () -> {
            while (cursor.next()) {
                // just keep going, it should eventually hit the cycle detection threshold
            }
        });

        // then
        assertEquals(
                format(
                        "Aborting property reading due to detected chain cycle, starting at property record id:%d from owner NODE:%d",
                        firstProp, owner.getId()),
                e.getMessage());
    }

    @Test
    void shouldAbortChainTraversalOnLikelyDynamicValueCycle() {
        // given
        Value value = random.nextAlphaNumericTextValue(1000, 1000);
        long firstProp = storeValuesAsPropertyChain(owner, new Value[] {value});

        // and a cycle on the second record
        PropertyStore store = neoStores.getPropertyStore();
        PropertyRecord propertyRecord = getRecord(store, firstProp, NORMAL);
        store.ensureHeavy(propertyRecord, new CachedStoreCursors(neoStores, NULL_CONTEXT));
        PropertyBlock block = propertyRecord.iterator().next();
        int cycleEndRecordIndex = random.nextInt(1, block.getValueRecords().size());
        DynamicRecord cycle = block.getValueRecords().get(cycleEndRecordIndex);
        int cycleStartIndex = random.nextInt(cycleEndRecordIndex);
        cycle.setNextBlock(block.getValueRecords().get(cycleStartIndex).getId());
        try (var cursor = storeCursors.writeCursor(DYNAMIC_STRING_STORE_CURSOR)) {
            store.getStringStore().updateRecord(cycle, cursor, NULL_CONTEXT, storeCursors);
        }
        owner.setId(99);

        // when
        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties(longReference(firstProp), ALL_PROPERTIES, owner.getId());
        InconsistentDataReadException e = assertThrows(InconsistentDataReadException.class, () -> {
            while (cursor.next()) {
                // just keep going, it should eventually hit the cycle detection threshold
                cursor.propertyValue();
            }
        });

        // then
        assertThat(e).hasMessageContainingAll("Unable to read property value in record", "owner NODE:" + owner.getId());
    }

    @Test
    void shouldOnlyReturnSelectedProperties() {
        // given
        Value[] values = createValues(10, 10);
        long firstPropertyId = storeValuesAsPropertyChain(owner, values);
        int[] selectedKeys = new int[random.nextInt(1, 3)];
        MutableIntObjectMap<Value> valueMapping = IntObjectMaps.mutable.empty();
        for (int i = 0; i < selectedKeys.length; i++) {
            int prev = i == 0 ? 0 : selectedKeys[i - 1];
            int stride = random.nextInt(1, 3);
            int key = prev + stride;
            selectedKeys[i] = key;
            valueMapping.put(key, values[key]);
        }

        // when
        RecordPropertyCursor cursor = createCursor();
        cursor.initNodeProperties(longReference(firstPropertyId), PropertySelection.selection(selectedKeys));
        while (cursor.next()) {
            int key = cursor.propertyKey();
            Value expectedValue = valueMapping.remove(key);
            assertThat(expectedValue).isEqualTo(expectedValue);
        }

        // then
        assertThat(valueMapping.isEmpty()).isTrue();
    }

    protected RecordPropertyCursor createCursor() {
        return new RecordPropertyCursor(neoStores.getPropertyStore(), NULL_CONTEXT, storeCursors, INSTANCE);
    }

    protected void assertPropertyChain(Value[] values, long firstPropertyId, RecordPropertyCursor cursor) {
        Map<Integer, Value> expectedValues = asMap(values);
        // This is a specific test for RecordPropertyCursor and we know that node/relationships init methods are the
        // same
        cursor.initNodeProperties(longReference(firstPropertyId), ALL_PROPERTIES, owner.getId());
        while (cursor.next()) {
            // then
            assertEquals(expectedValues.remove(cursor.propertyKey()), cursor.propertyValue());
        }
        assertTrue(expectedValues.isEmpty());
    }

    protected Value[] createValues() {
        return createValues(1, 20);
    }

    protected Value[] createValues(int minNumProps, int maxNumProps) {
        int numberOfProperties = random.nextInt(minNumProps, maxNumProps + 1);
        Value[] values = new Value[numberOfProperties];
        for (int key = 0; key < numberOfProperties; key++) {
            values[key] = random.nextValue();
        }
        return values;
    }

    protected long storeValuesAsPropertyChain(NodeRecord owner, Value[] values) {
        DirectRecordAccessSet access = new DirectRecordAccessSet(neoStores, idGeneratorFactory, NULL_CONTEXT);
        long firstPropertyId = createPropertyChain(
                owner, blocksOf(neoStores.getPropertyStore(), values, allocatorProvider), access.getPropertyRecords());
        access.commit();
        return firstPropertyId;
    }

    public long createPropertyChain(
            PrimitiveRecord owner,
            List<PropertyBlock> properties,
            RecordAccess<PropertyRecord, PrimitiveRecord> propertyRecords) {
        return RecordBuilders.createPropertyChain(neoStores.getPropertyStore(), owner, properties, propertyRecords);
    }

    protected static Map<Integer, Value> asMap(Value[] values) {
        Map<Integer, Value> map = new HashMap<>();
        for (int key = 0; key < values.length; key++) {
            map.put(key, values[key]);
        }
        return map;
    }

    protected static List<PropertyBlock> blocksOf(
            PropertyStore propertyStore, Value[] values, DynamicAllocatorProvider allocatorProvider) {
        var list = new ArrayList<PropertyBlock>();
        for (int i = 0; i < values.length; i++) {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    i,
                    values[i],
                    allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    NULL_CONTEXT,
                    INSTANCE);
            list.add(block);
        }
        return list;
    }

    private PropertyRecord getRecord(PropertyStore propertyStore, long id, RecordLoad load) {
        try (PageCursor cursor = propertyStore.openPageCursorForReading(id, NULL_CONTEXT)) {
            return propertyStore.getRecordByCursor(id, propertyStore.newRecord(), load, cursor);
        }
    }
}
