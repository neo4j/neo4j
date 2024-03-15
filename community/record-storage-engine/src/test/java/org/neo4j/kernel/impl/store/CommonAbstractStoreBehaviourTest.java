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

import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.PageCache.PAGE_SIZE;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.CHECK;
import static org.neo4j.kernel.impl.store.record.RecordLoad.FORCE;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.exceptions.UnderlyingStorageException;
import org.neo4j.function.ThrowingAction;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordIdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.format.BaseRecordFormat;
import org.neo4j.kernel.impl.store.record.AbstractBaseRecord;
import org.neo4j.kernel.impl.store.record.RecordLoad;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.string.Mask;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

/**
 * Test for {@link CommonAbstractStore}, but without using mocks.
 * @see CommonAbstractStoreTest for testing with mocks.
 */
@EphemeralPageCacheExtension
class CommonAbstractStoreBehaviourTest {
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);
    /**
     * Note that tests MUST use the non-modifying methods, to make alternate copies
     * of this settings class.
     */
    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private PageCache pageCache;

    private final Queue<Long> nextPageId = new ConcurrentLinkedQueue<>();
    private final Queue<Integer> nextPageOffset = new ConcurrentLinkedQueue<>();
    private int cursorErrorOnRecord;
    private int intsPerRecord = 1;

    private MyStore store;
    private PageCursor readCursor;
    private final Config config = Config.defaults();

    @AfterEach
    void tearDown() throws IOException {
        if (readCursor != null) {
            readCursor.close();
        }
        if (store != null) {
            store.close();
            fs.deleteFile(store.getStorageFile());
            store = null;
        }
        nextPageOffset.clear();
        nextPageId.clear();
    }

    private static void assertThrowsUnderlyingStorageException(ThrowingAction<Exception> action) {
        assertThrows(UnderlyingStorageException.class, action::apply);
    }

    private static void assertThrowsInvalidRecordException(ThrowingAction<Exception> action) {
        assertThrows(InvalidRecordException.class, action::apply);
    }

    private void verifyExceptionOnOutOfBoundsAccess(ThrowingAction<Exception> access) {
        prepareStoreForOutOfBoundsAccess();
        assertThrowsUnderlyingStorageException(access);
    }

    private void prepareStoreForOutOfBoundsAccess() {
        createStore();
        nextPageOffset.add(PAGE_SIZE - 2);
    }

    private void verifyExceptionOnCursorError(ThrowingAction<Exception> access) {
        prepareStoreForCursorError();
        assertThrowsInvalidRecordException(access);
    }

    private void prepareStoreForCursorError() {
        createStore();
        cursorErrorOnRecord = 5;
    }

    private void createStore() {
        store = new MyStore(config, pageCache, 8);
        store.initialise(CONTEXT_FACTORY);
        readCursor = store.openPageCursorForReading(0, NULL_CONTEXT);
    }

    @Test
    void writingOfHeaderRecordDuringInitialiseNewStoreFileMustThrowOnPageOverflow() {
        // 16-byte header will overflow an 8-byte page size
        MyStore store = new MyStore(config, pageCache, PAGE_SIZE + 1);
        assertThrowsUnderlyingStorageException(() -> store.initialise(CONTEXT_FACTORY));
    }

    @Test
    void extractHeaderRecordDuringLoadStorageMustThrowOnPageOverflow() {
        MyStore first = new MyStore(config, pageCache, 8);
        first.initialise(CONTEXT_FACTORY);
        first.close();

        MyStore second = new MyStore(config, pageCache, PAGE_SIZE + 1);
        assertThrowsUnderlyingStorageException(() -> second.initialise(CONTEXT_FACTORY));
    }

    @Test
    void getRawRecordDataMustNotThrowOnPageOverflow() throws Exception {
        prepareStoreForOutOfBoundsAccess();
        try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
            store.getRawRecordData(5, cursor);
        }
    }

    @Test
    void isInUseMustThrowOnPageOverflow() {
        verifyExceptionOnOutOfBoundsAccess(() -> store.isInUse(5, readCursor));
    }

    @Test
    void isInUseMustThrowOnCursorError() {
        verifyExceptionOnCursorError(() -> store.isInUse(5, readCursor));
    }

    @Test
    void getRecordMustThrowOnPageOverflow() {
        verifyExceptionOnOutOfBoundsAccess(() -> {
            try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                store.getRecordByCursor(5, new IntRecord(5), NORMAL, cursor);
            }
        });
    }

    @Test
    void getRecordMustThrowOnPageOverflowWithCheckLoadMode() {
        verifyExceptionOnOutOfBoundsAccess(() -> {
            try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                store.getRecordByCursor(5, new IntRecord(5), CHECK, cursor);
            }
        });
    }

    @Test
    void getRecordMustNotThrowOnPageOverflowWithForceLoadMode() {
        prepareStoreForOutOfBoundsAccess();
        try (var cursor = store.openPageCursorForReading(5, NULL_CONTEXT)) {
            store.getRecordByCursor(5, new IntRecord(5), FORCE, cursor);
        }
    }

    @Test
    void updateRecordMustThrowOnPageOverflow() {
        verifyExceptionOnOutOfBoundsAccess(() -> {
            try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
                store.updateRecord(new IntRecord(5), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            }
        });
    }

    @Test
    void getRecordMustThrowOnCursorError() {
        verifyExceptionOnCursorError(() -> {
            try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                store.getRecordByCursor(5, new IntRecord(5), NORMAL, cursor);
            }
        });
    }

    @Test
    void getRecordMustThrowOnCursorErrorWithCheckLoadMode() {
        verifyExceptionOnCursorError(() -> {
            try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
                store.getRecordByCursor(5, new IntRecord(5), CHECK, cursor);
            }
        });
    }

    @Test
    void getRecordMustNotThrowOnCursorErrorWithForceLoadMode() {
        prepareStoreForCursorError();
        try (var cursor = store.openPageCursorForReading(0, NULL_CONTEXT)) {
            store.getRecordByCursor(5, new IntRecord(5), FORCE, cursor);
        }
    }

    @Test
    void scanForHighIdMustThrowOnPageOverflow() {
        createStore();
        store.setStoreNotOk(new RuntimeException());
        IntRecord record = new IntRecord(200);
        record.value = 0xCAFEBABE;
        try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            store.updateRecord(record, storeCursor, NULL_CONTEXT, StoreCursors.NULL);
        }
        intsPerRecord = 8192;
        assertThrowsUnderlyingStorageException(() -> store.start(NULL_CONTEXT));
    }

    @Test
    void mustFinishInitialisationOfIncompleteStoreHeader() throws IOException {
        createStore();
        int headerSizeInRecords = store.getNumberOfReservedLowIds();
        int headerSizeInBytes = headerSizeInRecords * store.getRecordSize();
        try (PageCursor cursor = store.pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT)) {
            assertTrue(cursor.next());
            for (int i = 0; i < headerSizeInBytes; i++) {
                cursor.putByte((byte) 0);
            }
        }
        int pageSize = store.pagedFile.pageSize();
        store.close();
        store.pageCache
                .map(store.getStorageFile(), pageSize, DEFAULT_DATABASE_NAME, immutable.of(TRUNCATE_EXISTING))
                .close();
        createStore();
    }

    @Test
    void shouldProvideFreeIdsToMissingIdGenerator() throws IOException {
        // given
        createStore();
        store.start(NULL_CONTEXT);
        MutableLongSet holes = LongSets.mutable.empty();
        var idGenerator = store.getIdGenerator();
        holes.add(idGenerator.nextId(NULL_CONTEXT));
        holes.add(idGenerator.nextId(NULL_CONTEXT));
        try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            store.updateRecord(
                    new IntRecord(idGenerator.nextId(NULL_CONTEXT), 1), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            holes.add(idGenerator.nextId(NULL_CONTEXT));
            store.updateRecord(
                    new IntRecord(idGenerator.nextId(NULL_CONTEXT), 1), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
        }

        // when
        store.close();
        fs.deleteFile(Path.of(MyStore.ID_FILENAME));
        createStore();
        store.start(NULL_CONTEXT);
        var restartedGenerator = store.getIdGenerator();

        // then
        int numberOfHoles = holes.size();
        for (int i = 0; i < numberOfHoles; i++) {
            assertTrue(holes.remove(restartedGenerator.nextId(NULL_CONTEXT)));
        }
        assertTrue(holes.isEmpty());
    }

    @Test
    void shouldOverwriteExistingIdGeneratorOnMissingStore() throws IOException {
        // given
        createStore();
        store.start(NULL_CONTEXT);
        var idGenerator = store.getIdGenerator();
        MutableLongSet holes = LongSets.mutable.empty();
        try (var storeCursor = store.openPageCursorForWriting(0, NULL_CONTEXT)) {
            store.updateRecord(
                    new IntRecord(idGenerator.nextId(NULL_CONTEXT), 1), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            holes.add(idGenerator.nextId(NULL_CONTEXT));
            holes.add(idGenerator.nextId(NULL_CONTEXT));
            store.updateRecord(
                    new IntRecord(idGenerator.nextId(NULL_CONTEXT), 1), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
            holes.add(idGenerator.nextId(NULL_CONTEXT));
            store.updateRecord(
                    new IntRecord(idGenerator.nextId(NULL_CONTEXT), 1), storeCursor, NULL_CONTEXT, StoreCursors.NULL);
        }

        // when
        store.close();
        fs.deleteFile(Path.of(MyStore.STORE_FILENAME));
        createStore();
        store.start(NULL_CONTEXT);
        var restartedGenerator = store.getIdGenerator();

        // then
        int numberOfReservedLowIds = store.getNumberOfReservedLowIds();
        for (int i = 0; i < 10; i++) {
            assertEquals(numberOfReservedLowIds + i, restartedGenerator.nextId(NULL_CONTEXT));
        }
    }

    private static class IntRecord extends AbstractBaseRecord {
        public int value;

        IntRecord(long id) {
            this(id, 0);
        }

        IntRecord(long id, int value) {
            super(id);
            setInUse(true);
            this.value = value;
        }

        @Override
        public String toString(Mask mask) {
            return "IntRecord[" + getId() + "](" + value + ")";
        }
    }

    private static class LongLongHeader implements StoreHeader {}

    private class MyFormat extends BaseRecordFormat<IntRecord> implements StoreHeaderFormat<LongLongHeader> {
        MyFormat(int recordHeaderSize) {
            super(x -> 4, recordHeaderSize, 32, false);
        }

        @Override
        public IntRecord newRecord() {
            return new IntRecord(0);
        }

        @Override
        public boolean isInUse(PageCursor cursor) {
            int offset = cursor.getOffset();
            long pageId = cursor.getCurrentPageId();
            long recordId = (offset + pageId * cursor.getPagedFile().payloadSize()) / 4;
            boolean inUse = false;
            for (int i = 0; i < intsPerRecord; i++) {
                inUse |= cursor.getInt() != 0;
            }
            maybeSetCursorError(cursor, recordId);
            return inUse;
        }

        @Override
        public void read(IntRecord record, PageCursor cursor, RecordLoad mode, int recordSize, int recordsPerPage) {
            for (int i = 0; i < intsPerRecord; i++) {
                record.value = cursor.getInt();
            }
            record.setInUse(true);
            maybeSetCursorError(cursor, record.getId());
        }

        private void maybeSetCursorError(PageCursor cursor, long id) {
            if (cursorErrorOnRecord == id) {
                cursor.setCursorException("boom");
            }
        }

        @Override
        public void write(IntRecord record, PageCursor cursor, int recordSize, int recordsPerPage) {
            for (int i = 0; i < intsPerRecord; i++) {
                cursor.putInt(record.value);
            }
        }

        @Override
        public int numberOfReservedRecords() {
            return 4; // 2 longs occupy 4 int records
        }

        @Override
        public LongLongHeader generateHeader() {
            return new LongLongHeader();
        }

        @Override
        public void writeHeader(PageCursor cursor) {
            for (int i = 0; i < getRecordHeaderSize(); i++) {
                cursor.putByte((byte) ThreadLocalRandom.current().nextInt());
            }
        }

        @Override
        public LongLongHeader readHeader(PageCursor cursor) {
            LongLongHeader header = new LongLongHeader();
            for (int i = 0; i < getRecordHeaderSize(); i++) {
                // pretend to read fields into the header
                cursor.getByte();
            }
            return header;
        }
    }

    private class MyStore extends CommonAbstractStore<IntRecord, LongLongHeader> {
        static final String STORE_FILENAME = "store";
        static final String ID_FILENAME = "idFile";

        MyStore(Config config, PageCache pageCache, int recordHeaderSize) {
            this(config, pageCache, new MyFormat(recordHeaderSize));
        }

        MyStore(Config config, PageCache pageCache, MyFormat format) {
            super(
                    fs,
                    Path.of(STORE_FILENAME),
                    Path.of(ID_FILENAME),
                    config,
                    RecordIdType.NODE,
                    new DefaultIdGeneratorFactory(fs, immediate(), PageCacheTracer.NULL, DEFAULT_DATABASE_NAME),
                    pageCache,
                    PageCacheTracer.NULL,
                    NullLogProvider.getInstance(),
                    "T",
                    format,
                    format,
                    false,
                    DEFAULT_DATABASE_NAME,
                    immutable.empty());
        }

        @Override
        protected long pageIdForRecord(long id) {
            Long override = nextPageId.poll();
            return override != null ? override : super.pageIdForRecord(id);
        }

        @Override
        protected int offsetForId(long id) {
            Integer override = nextPageOffset.poll();
            return override != null ? override : super.offsetForId(id);
        }
    }
}
