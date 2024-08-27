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

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.dense_node_threshold;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.PROPERTY_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.api.schema.SchemaTestUtil.SIMPLE_NAME_LOOKUP;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordAccess.RecordProxy;
import org.neo4j.internal.recordstorage.id.TransactionIdSequenceProvider;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProvider;
import org.neo4j.kernel.impl.store.DynamicAllocatorProviders;
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.PropertyStore;
import org.neo4j.kernel.impl.store.PropertyType;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.StoreType;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.store.record.PrimitiveRecord;
import org.neo4j.kernel.impl.store.record.PropertyBlock;
import org.neo4j.kernel.impl.store.record.PropertyRecord;
import org.neo4j.kernel.impl.store.record.Record;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.lock.LockTracer;
import org.neo4j.lock.ResourceLocker;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.Values;

@PageCacheExtension
@Neo4jLayoutExtension
class PropertyCreatorTest {
    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private final MyPrimitiveProxy primitive = new MyPrimitiveProxy();
    private NeoStores neoStores;
    private PropertyStore propertyStore;
    private PropertyCreator creator;
    private DirectRecordAccess<PropertyRecord, PrimitiveRecord> records;
    private CursorContext cursorContext;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void startStore() {
        var pageCacheTracer = PageCacheTracer.NULL;
        NullLogProvider logProvider = NullLogProvider.getInstance();
        neoStores = new StoreFactory(
                        databaseLayout,
                        Config.defaults(),
                        new DefaultIdGeneratorFactory(
                                fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                        pageCache,
                        pageCacheTracer,
                        fileSystem,
                        logProvider,
                        new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openAllNeoStores();
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);

        propertyStore = neoStores.getPropertyStore();
        StoreCursors storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        cursorContext = contextFactory.create("propertyStore");
        records = new DirectRecordAccess<>(
                propertyStore,
                Loaders.propertyLoader(propertyStore, storeCursors),
                cursorContext,
                PROPERTY_CURSOR,
                storeCursors,
                EmptyMemoryTracker.INSTANCE);
        var context = new RecordStorageCommandCreationContext(
                neoStores,
                SIMPLE_NAME_LOOKUP,
                logProvider,
                dense_node_threshold.defaultValue(),
                Config.defaults(),
                false);
        context.initialize(
                KernelVersionProvider.THROWING_PROVIDER,
                cursorContext,
                storeCursors,
                () -> (long) -1,
                ResourceLocker.IGNORE,
                () -> LockTracer.NONE);

        creator = new PropertyCreator(
                allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                new PropertyTraverser(),
                new TransactionIdSequenceProvider(neoStores),
                cursorContext);
    }

    @AfterEach
    void closeStore() {
        neoStores.close();
    }

    @Test
    void noPageCacheAccessOnCleanIdGenerator() {
        assertZeroCursor();

        existingChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(6, 6), property(7, 7)));

        setProperty(10, 10);

        assertZeroCursor();
    }

    @Test
    void pageCacheAccessOnPropertyCreation() {
        assertZeroCursor();
        prepareDirtyGenerator(propertyStore);

        setProperty(10, 10);
        assertOneCursor();
    }

    @Test
    void shouldAddPropertyToEmptyChain() {
        // GIVEN
        existingChain();

        // WHEN
        setProperty(1, "value");

        // THEN
        assertChain(record(property(1, "value")));
    }

    @Test
    void shouldAddPropertyToChainContainingOtherFullRecords() {
        // GIVEN
        existingChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(6, 6), property(7, 7)));

        // WHEN
        setProperty(10, 10);

        // THEN
        assertChain(
                record(property(10, 10)),
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(6, 6), property(7, 7)));
    }

    @Test
    void shouldAddPropertyToChainContainingOtherNonFullRecords() {
        // GIVEN
        existingChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(6, 6)));

        // WHEN
        setProperty(10, 10);

        // THEN
        assertChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(6, 6), property(10, 10)));
    }

    @Test
    void shouldAddPropertyToChainContainingOtherNonFullRecordsInMiddle() {
        // GIVEN
        existingChain(
                record(property(0, 0), property(1, 1), property(2, 2)),
                record(property(3, 3), property(4, 4), property(5, 5), property(6, 6)));

        // WHEN
        setProperty(10, 10);

        // THEN
        assertChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(10, 10)),
                record(property(3, 3), property(4, 4), property(5, 5), property(6, 6)));
    }

    @Test
    void shouldChangeOnlyProperty() {
        // GIVEN
        existingChain(record(property(0, "one")));

        // WHEN
        setProperty(0, "two");

        // THEN
        assertChain(record(property(0, "two")));
    }

    @Test
    void shouldChangePropertyInChainWithOthersBeforeIt() {
        // GIVEN
        existingChain(record(property(0, "one"), property(1, 1)), record(property(2, "two"), property(3, 3)));

        // WHEN
        setProperty(2, "two*");

        // THEN
        assertChain(record(property(0, "one"), property(1, 1)), record(property(2, "two*"), property(3, 3)));
    }

    @Test
    void shouldChangePropertyInChainWithOthersAfterIt() {
        // GIVEN
        existingChain(record(property(0, "one"), property(1, 1)), record(property(2, "two"), property(3, 3)));

        // WHEN
        setProperty(0, "one*");

        // THEN
        assertChain(record(property(0, "one*"), property(1, 1)), record(property(2, "two"), property(3, 3)));
    }

    @Test
    void shouldChangePropertyToBiggerInFullChain() {
        // GIVEN
        existingChain(record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)));

        // WHEN
        setProperty(1, Long.MAX_VALUE);

        // THEN
        assertChain(record(property(1, Long.MAX_VALUE)), record(property(0, 0), property(2, 2), property(3, 3)));
    }

    @Test
    void shouldChangePropertyToBiggerInChainWithHoleAfter() {
        // GIVEN
        existingChain(
                record(property(0, 0), property(1, 1), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5)));

        // WHEN
        setProperty(1, Long.MAX_VALUE);

        // THEN
        assertChain(
                record(property(0, 0), property(2, 2), property(3, 3)),
                record(property(4, 4), property(5, 5), property(1, Long.MAX_VALUE)));
    }

    // change property so that it gets bigger and fits in a record earlier in the chain
    @Test
    void shouldChangePropertyToBiggerInChainWithHoleBefore() {
        // GIVEN
        existingChain(
                record(property(0, 0), property(1, 1)),
                record(property(2, 2), property(3, 3), property(4, 4), property(5, 5)));

        // WHEN
        setProperty(2, Long.MAX_VALUE);

        // THEN
        assertChain(
                record(property(0, 0), property(1, 1), property(2, Long.MAX_VALUE)),
                record(property(3, 3), property(4, 4), property(5, 5)));
    }

    @Test
    void canAddMultipleShortStringsToTheSameNode() {
        // GIVEN
        existingChain();

        // WHEN
        setProperty(0, "value");
        setProperty(1, "esrever");

        // THEN
        assertChain(record(property(0, "value", false), property(1, "esrever", false)));
    }

    @Test
    void canUpdateShortStringInplace() {
        // GIVEN
        existingChain(record(property(0, "value")));

        // WHEN
        long before = propertyRecordsInUse();
        setProperty(0, "other");
        long after = propertyRecordsInUse();

        // THEN
        assertChain(record(property(0, "other")));
        assertEquals(before, after);
    }

    @Test
    void canReplaceLongStringWithShortString() {
        // GIVEN
        long recordCount = dynamicStringRecordsInUse();
        long propCount = propertyRecordsInUse();
        existingChain(record(property(0, "this is a really long string, believe me!")));
        assertEquals(recordCount + 1, dynamicStringRecordsInUse());
        assertEquals(propCount + 1, propertyRecordsInUse());

        // WHEN
        setProperty(0, "value");

        // THEN
        assertChain(record(property(0, "value", false)));
        assertEquals(recordCount + 1, dynamicStringRecordsInUse());
        assertEquals(propCount + 1, propertyRecordsInUse());
    }

    @Test
    void canReplaceShortStringWithLongString() {
        // GIVEN
        long recordCount = dynamicStringRecordsInUse();
        long propCount = propertyRecordsInUse();
        existingChain(record(property(0, "value")));
        assertEquals(recordCount, dynamicStringRecordsInUse());
        assertEquals(propCount + 1, propertyRecordsInUse());

        // WHEN
        String longString = "this is a really long string, believe me!";
        setProperty(0, longString);

        // THEN
        assertChain(record(property(0, longString, true)));
        assertEquals(recordCount + 1, dynamicStringRecordsInUse());
        assertEquals(propCount + 1, propertyRecordsInUse());
    }

    private static void prepareDirtyGenerator(PropertyStore store) {
        var idGenerator = store.getIdGenerator();
        var marker = idGenerator.transactionalMarker(NULL_CONTEXT);
        marker.markDeleted(1L);
        idGenerator.clearCache(true, NULL_CONTEXT);
    }

    private void assertZeroCursor() {
        assertThat(cursorContext.getCursorTracer().hits()).isZero();
        assertThat(cursorContext.getCursorTracer().pins()).isZero();
        assertThat(cursorContext.getCursorTracer().unpins()).isZero();
    }

    private void assertOneCursor() {
        assertThat(cursorContext.getCursorTracer().hits()).isOne();
        assertThat(cursorContext.getCursorTracer().pins()).isOne();
        assertThat(cursorContext.getCursorTracer().unpins()).isOne();
    }

    private void existingChain(ExpectedRecord... initialRecords) {
        PropertyRecord prev = null;
        var idGenerator = propertyStore.getIdGenerator();
        for (ExpectedRecord initialRecord : initialRecords) {
            PropertyRecord record = this.records
                    .create(idGenerator.nextId(cursorContext), primitive.record, NULL_CONTEXT)
                    .forChangingData();
            record.setInUse(true);
            existingRecord(record, initialRecord);

            if (prev == null) {
                // This is the first one, update primitive to point to this
                primitive.record.setNextProp(record.getId());
            } else {
                // link property records together
                record.setPrevProp(prev.getId());
                prev.setNextProp(record.getId());
            }

            prev = record;
        }
        this.records.commit();
    }

    private void existingRecord(PropertyRecord record, ExpectedRecord initialRecord) {
        for (ExpectedProperty initialProperty : initialRecord.properties) {
            PropertyBlock block = new PropertyBlock();
            PropertyStore.encodeValue(
                    block,
                    initialProperty.key,
                    initialProperty.value,
                    allocatorProvider.allocator(StoreType.PROPERTY_STRING),
                    allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                    cursorContext,
                    INSTANCE);
            record.addPropertyBlock(block);
        }
        assertTrue(record.size() <= PropertyType.getPayloadSize());
    }

    private void setProperty(int key, Object value) {
        creator.primitiveSetProperty(primitive, key, Values.of(value), records, INSTANCE);
    }

    private void assertChain(ExpectedRecord... expectedRecords) {
        long nextProp = primitive.forReadingLinkage().getNextProp();
        int expectedRecordCursor = 0;
        while (!Record.NO_NEXT_PROPERTY.is(nextProp)) {
            PropertyRecord record =
                    records.getOrLoad(nextProp, primitive.forReadingLinkage()).forReadingData();
            assertRecord(record, expectedRecords[expectedRecordCursor++]);
            nextProp = record.getNextProp();
        }
    }

    private void assertRecord(PropertyRecord record, ExpectedRecord expectedRecord) {
        assertEquals(expectedRecord.properties.length, record.numberOfProperties());
        for (ExpectedProperty expectedProperty : expectedRecord.properties) {
            PropertyBlock block = record.getPropertyBlock(expectedProperty.key);
            assertNotNull(block);
            assertEquals(
                    expectedProperty.value,
                    block.getType().value(block, propertyStore, StoreCursors.NULL, EmptyMemoryTracker.INSTANCE));
            if (expectedProperty.assertHasDynamicRecords != null) {
                if (expectedProperty.assertHasDynamicRecords) {
                    assertThat(block.getValueRecords().size()).isGreaterThan(0);
                } else {
                    assertEquals(0, block.getValueRecords().size());
                }
            }
        }
    }

    private static class ExpectedProperty {
        private final int key;
        private final Value value;
        private final Boolean assertHasDynamicRecords;

        ExpectedProperty(int key, Object value) {
            this(key, value, null /*don't care*/);
        }

        ExpectedProperty(int key, Object value, Boolean assertHasDynamicRecords) {
            this.key = key;
            this.value = Values.of(value);
            this.assertHasDynamicRecords = assertHasDynamicRecords;
        }
    }

    private record ExpectedRecord(ExpectedProperty... properties) {}

    private static ExpectedProperty property(int key, Object value) {
        return new ExpectedProperty(key, value);
    }

    private static ExpectedProperty property(int key, Object value, boolean hasDynamicRecords) {
        return new ExpectedProperty(key, value, hasDynamicRecords);
    }

    private static ExpectedRecord record(ExpectedProperty... properties) {
        return new ExpectedRecord(properties);
    }

    private static class MyPrimitiveProxy implements RecordProxy<NodeRecord, Void> {
        private final NodeRecord record = new NodeRecord(5);
        private boolean changed;

        MyPrimitiveProxy() {
            record.setInUse(true);
        }

        @Override
        public long getKey() {
            return record.getId();
        }

        @Override
        public NodeRecord forChangingLinkage() {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forChangingData() {
            changed = true;
            return record;
        }

        @Override
        public NodeRecord forReadingLinkage() {
            return record;
        }

        @Override
        public NodeRecord forReadingData() {
            return record;
        }

        @Override
        public Void getAdditionalData() {
            return null;
        }

        @Override
        public NodeRecord getBefore() {
            return record;
        }

        @Override
        public boolean isChanged() {
            return changed;
        }

        @Override
        public boolean isCreated() {
            return false;
        }
    }

    private long propertyRecordsInUse() {
        return propertyStore.getIdGenerator().getHighId();
    }

    private long dynamicStringRecordsInUse() {
        return propertyStore.getStringStore().getIdGenerator().getHighId();
    }
}
