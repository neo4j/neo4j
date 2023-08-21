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
package org.neo4j.io.pagecache;

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.test.extension.ExecutionSharedContext.SHARED_RESOURCE;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Path;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Function;
import java.util.function.Supplier;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.io.pagecache.buffer.IOBufferFactory;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@ResourceLock(SHARED_RESOURCE)
public abstract class PageCacheTestSupport<T extends PageCache> {
    protected static ExecutorService executor;
    protected long SHORT_TIMEOUT_MILLIS = 30_000;
    protected long SEMI_LONG_TIMEOUT_MILLIS = 120_000;
    protected long LONG_TIMEOUT_MILLIS = 360_000;

    @BeforeAll
    public static void startExecutor() {
        executor = Executors.newCachedThreadPool();
    }

    @AfterAll
    public static void stopExecutor() {
        executor.shutdown();
    }

    protected int recordSize = 9;
    protected int maxPages = 40;

    protected int pageCachePageSize;
    protected int pageCachePayloadSize;
    protected int reservedBytes;
    protected boolean multiVersioned;
    protected int recordsPerFilePage;
    protected int recordCount;
    protected int filePageSize;
    protected int filePayloadSize;
    protected ByteBuffer bufA;
    protected FileSystemAbstraction fs;
    protected JobScheduler jobScheduler;
    protected T pageCache;

    private Fixture<T> fixture;

    protected abstract Fixture<T> createFixture();

    protected boolean isMultiVersioned() {
        return false;
    }

    @BeforeEach
    public void setUp() throws IOException {
        fixture = createFixture();
        multiVersioned = isMultiVersioned();
        reservedBytes = isMultiVersioned() ? fixture.getReservedBytes() : 0;
        //noinspection ResultOfMethodCallIgnored
        Thread.interrupted(); // Clear stray interrupts
        fs = createFileSystemAbstraction();
        jobScheduler = new ThreadPoolJobScheduler();
        ensureExists(file("a"));
    }

    @AfterEach
    public void tearDown() throws Exception {
        //noinspection ResultOfMethodCallIgnored
        Thread.interrupted(); // Clear stray interrupts

        if (pageCache != null) {
            tearDownPageCache(pageCache);
        }
        jobScheduler.close();
        fs.close();
    }

    protected final T createPageCache(PageSwapperFactory swapperFactory, int maxPages, PageCacheTracer tracer) {
        T pageCache =
                fixture.createPageCache(swapperFactory, maxPages, tracer, jobScheduler, fixture.getBufferFactory());
        pageCachePageSize = pageCache.pageSize();
        pageCachePayloadSize = pageCache.pageSize() - reservedBytes;
        recordsPerFilePage = pageCachePayloadSize / recordSize;
        recordCount = 5 * maxPages * recordsPerFilePage;
        filePayloadSize = recordsPerFilePage * recordSize;
        filePageSize = filePayloadSize + reservedBytes;
        bufA = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        return pageCache;
    }

    protected T createPageCache(FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer) {
        PageSwapperFactory swapperFactory = createDefaultPageSwapperFactory(fs, tracer);
        return createPageCache(swapperFactory, maxPages, tracer);
    }

    protected SingleFilePageSwapperFactory createDefaultPageSwapperFactory(
            FileSystemAbstraction fs, PageCacheTracer tracer) {
        return new SingleFilePageSwapperFactory(fs, tracer, EmptyMemoryTracker.INSTANCE);
    }

    protected final T getPageCache(FileSystemAbstraction fs, int maxPages, PageCacheTracer tracer) {
        if (pageCache != null) {
            tearDownPageCache(pageCache);
        }
        pageCache = createPageCache(fs, maxPages, tracer);
        return pageCache;
    }

    protected void configureStandardPageCache() {
        getPageCache(fs, maxPages, PageCacheTracer.NULL);
    }

    protected final void tearDownPageCache(T pageCache) {
        fixture.tearDownPageCache(pageCache);
    }

    protected final FileSystemAbstraction createFileSystemAbstraction() {
        return fixture.getFileSystemAbstraction();
    }

    protected final Path file(String pathname) throws IOException {
        return fixture.file(pathname);
    }

    protected final Path path() throws IOException {
        return fixture.file("a").getParent();
    }

    protected void ensureExists(Path file) throws IOException {
        fs.mkdirs(file.getParent());
        fs.write(file).close();
    }

    protected Path existingFile(String name) throws IOException {
        Path file = file(name);
        ensureExists(file);
        return file;
    }

    /**
     * Verifies the records on the current page of the cursor.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected(PageCursor cursor) throws IOException {
        verifyRecordsMatchExpected(cursor, recordsPerFilePage, 0);
    }

    /**
     * Verifies the records on the current page of the cursor.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected(PageCursor cursor, int recordToCheck, int startingOffset)
            throws IOException {
        ByteBuffer expectedPageContents = ByteBuffers.allocate(filePageSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        ByteBuffer actualPageContents = ByteBuffers.allocate(filePageSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        int recordInPart;
        byte[] recordBytesPart = new byte[recordSize - Integer.BYTES];
        long pageId = cursor.getCurrentPageId();
        for (int i = 0; i < recordToCheck; i++) {
            long recordId = (pageId * recordToCheck) + i;
            expectedPageContents.position(startingOffset + recordSize * i);
            ByteBuffer slice = expectedPageContents.slice().order(ByteOrder.LITTLE_ENDIAN);
            slice.limit(recordSize);
            generateRecordForId(recordId, slice);
            do {
                cursor.setOffset(startingOffset + recordSize * i);
                recordInPart = cursor.getInt();
                cursor.getBytes(recordBytesPart);
            } while (cursor.shouldRetry());
            actualPageContents.position(startingOffset + recordSize * i);
            actualPageContents.putInt(recordInPart);
            actualPageContents.put(recordBytesPart);
        }
        assertRecords(pageId, actualPageContents, expectedPageContents);
    }

    /**
     * Verifies the records in the current buffer assuming the given page id.
     * <p>
     * This does the do-while-retry loop internally.
     */
    protected void verifyRecordsMatchExpected(long pageId, int offset, ByteBuffer actualPageContents) {
        ByteBuffer expectedPageContents = ByteBuffers.allocate(filePayloadSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        for (int i = 0; i < recordsPerFilePage; i++) {
            long recordId = (pageId * recordsPerFilePage) + i;
            expectedPageContents.position(recordSize * i);
            ByteBuffer slice = expectedPageContents.slice().order(ByteOrder.LITTLE_ENDIAN);
            slice.limit(recordSize);
            generateRecordForId(recordId, slice);
        }
        int len = actualPageContents.limit() - actualPageContents.position();
        byte[] actual = new byte[len];
        byte[] expected = new byte[len];
        actualPageContents.get(actual);
        expectedPageContents.position(offset);
        expectedPageContents.get(expected);
        assertRecords(pageId, actual, expected);
    }

    protected static void assertRecords(long pageId, ByteBuffer actualPageContents, ByteBuffer expectedPageContents) {
        byte[] actualBytes = actualPageContents.array();
        byte[] expectedBytes = expectedPageContents.array();
        assertRecords(pageId, actualBytes, expectedBytes);
    }

    protected static void assertRecords(long pageId, byte[] actualBytes, byte[] expectedBytes) {
        int estimatedPageId = estimateId(actualBytes);
        assertThat(actualBytes)
                .as("Page id: " + pageId + " " + "(based on record data, it should have been " + estimatedPageId
                        + ", a difference of " + Math.abs(pageId - estimatedPageId) + ")")
                .containsExactly(expectedBytes);
    }

    protected static int estimateId(byte[] record) {
        return ByteBuffer.wrap(record).getInt() - 1;
    }

    /**
     * Fill the page bound by the cursor with records that can be verified with
     * {@link #verifyRecordsMatchExpected(PageCursor)} or {@link #verifyRecordsInFile(Path, int)}.
     */
    protected void writeRecords(PageCursor cursor) {
        cursor.setOffset(0);
        for (int i = 0; i < recordsPerFilePage; i++) {
            long recordId = (cursor.getCurrentPageId() * recordsPerFilePage) + i;
            generateRecordForId(recordId, bufA);
            cursor.putBytes(bufA.array());
        }
    }

    protected void generateFileWithRecords(
            Path file, int recordCount, int recordSize, int recordsPerFilePage, int reservedBytes, int pageSize)
            throws IOException {
        try (StoreChannel channel = fs.write(file)) {
            generateFileWithRecords(channel, recordCount, recordSize, recordsPerFilePage, reservedBytes, pageSize);
        }
    }

    protected static void generateFileWithRecords(
            StoreChannel channel,
            int recordCount,
            int recordSize,
            int recordsPerFilePage,
            int reservedBytes,
            int pageSize)
            throws IOException {
        ByteBuffer buf = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        for (int i = 0; i < recordCount; i++) {
            if (i % recordsPerFilePage == 0) {
                long position = channel.position();
                if (position % pageSize != 0) {
                    writeBuffer(channel, ByteBuffer.allocate((int) (pageSize - (position % pageSize))));
                }
                writeBuffer(channel, ByteBuffer.allocate(reservedBytes));
            }
            generateRecordForId(i, buf);
            writeBuffer(channel, buf);
        }
    }

    private static void writeBuffer(WritableByteChannel channel, ByteBuffer buf) throws IOException {
        int rem = buf.remaining();
        do {
            rem -= channel.write(buf);
        } while (rem > 0);
    }

    protected static void generateRecordForId(long id, ByteBuffer buf) {
        buf.position(0);
        int x = (int) (id + 1);
        buf.putInt(x);
        while (buf.position() < buf.limit()) {
            x++;
            buf.put((byte) (x & 0xFF));
        }
        buf.position(0);
    }

    protected void verifyRecordsInFile(Path file, int recordCount) throws IOException {
        try (StoreChannel channel = fs.read(file)) {
            verifyRecordsInFile(channel, recordCount);
        }
    }

    protected void verifyRecordsInFile(StoreChannel channel, int recordCount) throws IOException {
        ByteBuffer buf = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        ByteBuffer observation = ByteBuffers.allocate(recordSize, ByteOrder.LITTLE_ENDIAN, INSTANCE);
        for (int i = 0; i < recordCount; i++) {
            if (i % recordsPerFilePage == 0) {
                channel.position(channel.position() + reservedBytes);
            }
            generateRecordForId(i, buf);
            observation.position(0);
            channel.read(observation);
            assertRecords(i, observation, buf);
        }
    }

    protected static Runnable closePageFile(final PagedFile file) {
        return file::close;
    }

    public abstract static class Fixture<T extends PageCache> {
        private Supplier<FileSystemAbstraction> fileSystemAbstractionSupplier = EphemeralFileSystemAbstraction::new;
        private Function<String, Path> fileConstructor = Path::of;
        private IOBufferFactory bufferFactory;
        private int reservedBytes = PageCache.RESERVED_BYTES;

        public abstract T createPageCache(
                PageSwapperFactory swapperFactory,
                int maxPages,
                PageCacheTracer tracer,
                JobScheduler jobScheduler,
                IOBufferFactory bufferFactory);

        public abstract void tearDownPageCache(T pageCache);

        public final FileSystemAbstraction getFileSystemAbstraction() {
            return fileSystemAbstractionSupplier.get();
        }

        public IOBufferFactory getBufferFactory() {
            return bufferFactory;
        }

        public int getReservedBytes() {
            return reservedBytes;
        }

        public final Fixture<T> withFileSystemAbstraction(
                Supplier<FileSystemAbstraction> fileSystemAbstractionSupplier) {
            this.fileSystemAbstractionSupplier = fileSystemAbstractionSupplier;
            return this;
        }

        public final Path file(String pathname) {
            return fileConstructor.apply(pathname).toAbsolutePath().normalize();
        }

        public final Fixture<T> withBufferFactory(IOBufferFactory bufferFactory) {
            this.bufferFactory = bufferFactory;
            return this;
        }

        public final Fixture<T> withReservedBytes(int reservedBytes) {
            this.reservedBytes = reservedBytes;
            return this;
        }

        public final Fixture<T> withFileConstructor(Function<String, Path> fileConstructor) {
            this.fileConstructor = fileConstructor;
            return this;
        }
    }
}
