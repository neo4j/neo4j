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
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Iterator;
import org.eclipse.collections.api.set.ImmutableSet;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCacheOpenOptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.RecordFormats;
import org.neo4j.kernel.impl.store.record.DynamicRecord;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@ExtendWith(RandomExtension.class)
class AbstractDynamicStoreTest {
    protected static final int BLOCK_SIZE = 60;

    @Inject
    protected EphemeralFileSystemAbstraction fs;

    @Inject
    protected PageCache pageCache;

    @Inject
    RandomSupport random;

    protected final Path storeFile = Path.of("store");
    private final Path idFile = Path.of("idStore");
    private final RecordFormats formats = defaultFormat();

    @BeforeEach
    void before() throws IOException {
        try (StoreChannel channel = fs.write(storeFile)) {
            var buffer = ByteBuffers.allocate(pageCache.pageSize(), getByteOrder(), INSTANCE);
            buffer.putInt(BLOCK_SIZE);
            while (buffer.hasRemaining()) {
                buffer.put((byte) 0);
            }
            buffer.flip();
            channel.writeAll(buffer);
        }
    }

    @Test
    void tracePageCacheAccessOnRecordsAllocation() {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("tracePageCacheAccessOnRecordsAllocation");
                var store = newTestableDynamicStore()) {
            assertZeroCursor(cursorContext);
            prepareDirtyGenerator(store);

            store.getIdGenerator().maintenance(cursorContext);
            store.allocateRecordsFromBytes(
                    new ArrayList<>(),
                    new byte[] {0, 1, 2, 3, 4},
                    new StandardDynamicRecordAllocator(store.getIdGenerator(), store.getRecordDataSize()),
                    cursorContext,
                    INSTANCE);

            assertOneCursor(cursorContext);
        }
    }

    @Test
    void noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTree() {
        var contextFactory = new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);
        try (var cursorContext = contextFactory.create("noPageCacheAccessWhenIdAllocationDoesNotAccessUnderlyingTree");
                var store = newTestableDynamicStore()) {
            assertZeroCursor(cursorContext);

            store.allocateRecordsFromBytes(
                    new ArrayList<>(),
                    new byte[] {0, 1, 2, 3, 4},
                    new StandardDynamicRecordAllocator(store.getIdGenerator(), store.getRecordDataSize()),
                    cursorContext,
                    INSTANCE);

            assertZeroCursor(cursorContext);
        }
    }

    @Test
    void dynamicRecordCursorReadsInUseRecords() {
        try (AbstractDynamicStore store = newTestableDynamicStore()) {
            DynamicRecord first = createDynamicRecord(1, store, 0);
            DynamicRecord second = createDynamicRecord(2, store, 0);
            DynamicRecord third = createDynamicRecord(3, store, 10);
            store.setHighId(3);

            first.setNextBlock(second.getId());
            try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
                store.updateRecord(first, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
                second.setNextBlock(third.getId());
                store.updateRecord(second, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            }

            try (var storeCursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                Iterator<DynamicRecord> records = store.getRecords(
                                1, NORMAL, false, storeCursor, EmptyMemoryTracker.INSTANCE)
                        .iterator();
                assertTrue(records.hasNext());
                assertEquals(first, records.next());
                assertTrue(records.hasNext());
                assertEquals(second, records.next());
                assertTrue(records.hasNext());
                assertEquals(third, records.next());
                assertFalse(records.hasNext());
            }
        }
    }

    @Test
    void dynamicRecordCursorReadsNotInUseRecords() {
        try (AbstractDynamicStore store = newTestableDynamicStore()) {
            DynamicRecord first = createDynamicRecord(1, store, 0);
            DynamicRecord second = createDynamicRecord(2, store, 0);
            DynamicRecord third = createDynamicRecord(3, store, 10);
            store.setHighId(3);

            try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
                first.setNextBlock(second.getId());
                store.updateRecord(first, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
                second.setNextBlock(third.getId());
                store.updateRecord(second, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
                second.setInUse(false);
                store.updateRecord(second, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            }

            try (var storeCursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                Iterator<DynamicRecord> records = store.getRecords(
                                1, FORCE, false, storeCursor, EmptyMemoryTracker.INSTANCE)
                        .iterator();
                assertTrue(records.hasNext());
                assertEquals(first, records.next());
                assertTrue(records.hasNext());
                DynamicRecord secondReadRecord = records.next();
                assertEquals(second, secondReadRecord);
                assertFalse(secondReadRecord.inUse());
                // because mode == FORCE we can still move through the chain
                assertTrue(records.hasNext());
                assertEquals(third, records.next());
                assertFalse(records.hasNext());
            }
        }
    }

    private static void prepareDirtyGenerator(AbstractDynamicStore store) {
        var idGenerator = store.getIdGenerator();
        try (var marker = idGenerator.transactionalMarker(NULL_CONTEXT)) {
            marker.markDeleted(1L);
        }
        idGenerator.clearCache(true, NULL_CONTEXT);
    }

    private static void assertOneCursor(CursorContext cursorContext) {
        assertThat(cursorContext.getCursorTracer().hits()).isOne();
        assertThat(cursorContext.getCursorTracer().pins()).isOne();
        assertThat(cursorContext.getCursorTracer().unpins()).isOne();
    }

    private static void assertZeroCursor(CursorContext cursorContext) {
        assertThat(cursorContext.getCursorTracer().hits()).isZero();
        assertThat(cursorContext.getCursorTracer().pins()).isZero();
        assertThat(cursorContext.getCursorTracer().unpins()).isZero();
    }

    private DynamicRecord createDynamicRecord(long id, AbstractDynamicStore store, int dataSize) {
        DynamicRecord first = new DynamicRecord(id);
        first.setInUse(true);
        first.setData(
                random.nextBytes(dataSize == 0 ? BLOCK_SIZE - formats.dynamic().getRecordHeaderSize() : 10));
        try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            store.updateRecord(first, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
        }
        return first;
    }

    private AbstractDynamicStore newTestableDynamicStore() {
        DefaultIdGeneratorFactory idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), PageCacheTracer.NULL, DEFAULT_DATABASE_NAME);
        AbstractDynamicStore store =
                new AbstractDynamicStore(
                        fs,
                        storeFile,
                        idFile,
                        Config.defaults(),
                        RecordIdType.ARRAY_BLOCK,
                        idGeneratorFactory,
                        pageCache,
                        PageCacheTracer.NULL,
                        NullLogProvider.getInstance(),
                        "test",
                        BLOCK_SIZE,
                        formats.dynamic(),
                        false,
                        DEFAULT_DATABASE_NAME,
                        getOpenOptions()) {
                    @Override
                    public String getTypeDescriptor() {
                        return "TestDynamicStore";
                    }
                };
        store.initialise(new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER));
        return store;
    }

    protected ImmutableSet<OpenOption> getOpenOptions() {
        return immutable.of(PageCacheOpenOptions.BIG_ENDIAN);
    }

    protected ByteOrder getByteOrder() {
        return getOpenOptions().contains(PageCacheOpenOptions.BIG_ENDIAN)
                ? ByteOrder.BIG_ENDIAN
                : ByteOrder.LITTLE_ENDIAN;
    }
}
