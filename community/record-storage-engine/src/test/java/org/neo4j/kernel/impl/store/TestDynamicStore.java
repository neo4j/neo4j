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

import static org.apache.commons.lang3.ArrayUtils.EMPTY_STRING_ARRAY;
import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.DYNAMIC_ARRAY_STORE_CURSOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
class TestDynamicStore {
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    @Inject
    private DatabaseLayout databaseLayout;

    private StoreFactory storeFactory;
    private NeoStores neoStores;
    private CachedStoreCursors storeCursors;
    private DynamicAllocatorProvider allocatorProvider;

    @BeforeEach
    void setUp() {
        var pageCacheTracer = PageCacheTracer.NULL;
        storeFactory = new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(fs, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fs,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
    }

    @AfterEach
    void tearDown() {
        if (storeCursors != null) {
            storeCursors.close();
        }
        if (neoStores != null) {
            neoStores.close();
        }
    }

    private DynamicArrayStore createDynamicArrayStore() throws IOException {
        neoStores = storeFactory.openAllNeoStores();
        neoStores.start(NULL_CONTEXT);
        allocatorProvider = DynamicAllocatorProviders.nonTransactionalAllocator(neoStores);
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
        return neoStores.getPropertyStore().getArrayStore();
    }

    @Test
    void testClose() throws IOException {
        DynamicArrayStore store = createDynamicArrayStore();
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecordsFromBytes(
                records, new byte[10], allocatorProvider.allocator(StoreType.PROPERTY_ARRAY), NULL_CONTEXT, INSTANCE);
        long blockId = Iterables.first(records).getId();
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            for (DynamicRecord record : records) {
                store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }
        neoStores.close();
        neoStores = null;

        assertThrows(
                RuntimeException.class,
                () -> store.getArrayFor(
                        store.getRecords(blockId, NORMAL, false, null, EmptyMemoryTracker.INSTANCE),
                        storeCursors,
                        EmptyMemoryTracker.INSTANCE));
        assertThrows(
                RuntimeException.class, () -> store.getRecords(0, NORMAL, false, null, EmptyMemoryTracker.INSTANCE));
    }

    @Test
    void testStoreGetCharsFromString() throws IOException {
        final String STR = "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ";
        DynamicArrayStore store = createDynamicArrayStore();
        char[] chars = new char[STR.length()];
        STR.getChars(0, STR.length(), chars, 0);
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecords(
                records, chars, allocatorProvider.allocator(StoreType.PROPERTY_ARRAY), NULL_CONTEXT, INSTANCE);
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            for (DynamicRecord record : records) {
                store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }
        // assertEquals( STR, new String( store.getChars( blockId ) ) );
    }

    @Test
    void testRandomTest() throws IOException {
        Random random = new Random(System.currentTimeMillis());
        DynamicArrayStore store = createDynamicArrayStore();
        List<Long> idsTaken = new ArrayList<>();
        Map<Long, byte[]> byteData = new HashMap<>();
        float deleteIndex = 0.2f;
        float closeIndex = 0.1f;
        int currentCount = 0;
        int maxCount = 128;
        Set<Long> set = new HashSet<>();
        while (currentCount < maxCount) {
            float rIndex = random.nextFloat();
            if (rIndex < deleteIndex && currentCount > 0) {
                long blockId = idsTaken.remove(random.nextInt(currentCount));
                store.getRecords(
                        blockId,
                        NORMAL,
                        false,
                        storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                        EmptyMemoryTracker.INSTANCE);
                byte[] bytes = (byte[]) store.getArrayFor(
                                store.getRecords(
                                        blockId,
                                        NORMAL,
                                        false,
                                        storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                                        EmptyMemoryTracker.INSTANCE),
                                storeCursors,
                                EmptyMemoryTracker.INSTANCE)
                        .asObject();
                validateData(bytes, byteData.remove(blockId));
                Collection<DynamicRecord> records = store.getRecords(
                        blockId,
                        NORMAL,
                        false,
                        storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                        EmptyMemoryTracker.INSTANCE);
                try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
                    for (DynamicRecord record : records) {
                        record.setInUse(false);
                        store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
                        set.remove(record.getId());
                    }
                }
                currentCount--;
            } else {
                byte[] bytes = createRandomBytes(random);
                Collection<DynamicRecord> records = new ArrayList<>();
                store.allocateRecords(
                        records, bytes, allocatorProvider.allocator(StoreType.PROPERTY_ARRAY), NULL_CONTEXT, INSTANCE);
                try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
                    for (DynamicRecord record : records) {
                        assertFalse(set.contains(record.getId()));
                        store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
                        set.add(record.getId());
                    }
                }
                long blockId = Iterables.first(records).getId();
                idsTaken.add(blockId);
                byteData.put(blockId, bytes);
                currentCount++;
            }
            if (rIndex > (1.0f - closeIndex) || rIndex < closeIndex) {
                neoStores.flush(DatabaseFlushEvent.NULL, NULL_CONTEXT);
                neoStores.close();
                store = createDynamicArrayStore();
            }
        }
    }

    private static byte[] createBytes(int length) {
        return new byte[length];
    }

    private static byte[] createRandomBytes(Random r) {
        return new byte[r.nextInt(1024)];
    }

    private static void validateData(byte[] data1, byte[] data2) {
        assertEquals(data1.length, data2.length);
        for (int i = 0; i < data1.length; i++) {
            assertEquals(data1[i], data2[i]);
        }
    }

    private long create(DynamicArrayStore store, Object arrayToStore, StoreCursors storeCursors) {
        Collection<DynamicRecord> records = new ArrayList<>();
        store.allocateRecords(
                records, arrayToStore, allocatorProvider.allocator(StoreType.PROPERTY_ARRAY), NULL_CONTEXT, INSTANCE);
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            for (DynamicRecord record : records) {
                store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }
        return Iterables.first(records).getId();
    }

    @Test
    void testAddDeleteSequenceEmptyNumberArray() throws IOException {
        DynamicArrayStore store = createDynamicArrayStore();
        byte[] emptyToWrite = createBytes(0);
        long blockId = create(store, emptyToWrite, storeCursors);
        store.getRecords(
                blockId,
                NORMAL,
                false,
                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        byte[] bytes = (byte[]) store.getArrayFor(
                        store.getRecords(
                                blockId,
                                NORMAL,
                                false,
                                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                                EmptyMemoryTracker.INSTANCE),
                        storeCursors,
                        EmptyMemoryTracker.INSTANCE)
                .asObject();
        assertEquals(0, bytes.length);

        Collection<DynamicRecord> records = store.getRecords(
                blockId,
                NORMAL,
                false,
                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            for (DynamicRecord record : records) {
                record.setInUse(false);
                store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }
    }

    @Test
    void testAddDeleteSequenceEmptyStringArray() throws IOException {
        DynamicArrayStore store = createDynamicArrayStore();
        long blockId = create(store, EMPTY_STRING_ARRAY, storeCursors);
        store.getRecords(
                blockId,
                NORMAL,
                false,
                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        String[] readBack = (String[]) store.getArrayFor(
                        store.getRecords(
                                blockId,
                                NORMAL,
                                false,
                                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                                EmptyMemoryTracker.INSTANCE),
                        storeCursors,
                        EmptyMemoryTracker.INSTANCE)
                .asObject();
        assertEquals(0, readBack.length);

        Collection<DynamicRecord> records = store.getRecords(
                blockId,
                NORMAL,
                false,
                storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                EmptyMemoryTracker.INSTANCE);
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            for (DynamicRecord record : records) {
                record.setInUse(false);
                store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors);
            }
        }
    }

    @Test
    void mustThrowOnRecordChainCycle() throws IOException {
        DynamicArrayStore store = createDynamicArrayStore();
        List<DynamicRecord> records = new ArrayList<>();
        store.allocateRecords(
                records,
                createBytes(500),
                allocatorProvider.allocator(StoreType.PROPERTY_ARRAY),
                NULL_CONTEXT,
                INSTANCE);
        long firstId = records.get(0).getId();
        // Avoid creating this inconsistency at the last record, since that would trip up a data-size check instead.
        DynamicRecord secondLastRecord = records.get(records.size() - 2);
        long secondLastId = secondLastRecord.getId();
        secondLastRecord.setNextBlock(secondLastId);
        try (var storeCursor = storeCursors.writeCursor(DYNAMIC_ARRAY_STORE_CURSOR)) {
            records.forEach(record -> store.updateRecord(record, storeCursor, NULL_CONTEXT, storeCursors));
        }

        var e = assertThrows(
                RecordChainCycleDetectedException.class,
                () -> store.getRecords(
                        firstId,
                        NORMAL,
                        true,
                        storeCursors.readCursor(DYNAMIC_ARRAY_STORE_CURSOR),
                        EmptyMemoryTracker.INSTANCE));
        String message = e.getMessage();
        assertThat(message).contains("" + firstId);
        assertThat(message).contains("" + secondLastRecord.getId());
    }
}
