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
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.io.pagecache.PagedFile.PF_READ_AHEAD;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.format.RecordFormatSelector.defaultFormat;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.SplittableRandom;
import java.util.function.Consumer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.kernel.impl.store.NoStoreHeader;
import org.neo4j.kernel.impl.store.format.RecordFormat;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
class PageCachePrefetchingTest {
    @Inject
    TestDirectory dir;

    @Inject
    FileSystemAbstraction fs;

    @Inject
    PageCache pageCache;

    private Path file;
    private CursorContext cursorContext;
    private Consumer<PageCursor> scanner;

    @BeforeEach
    void setUp() {
        file = dir.createFile("file");
        var pageCacheTracer = new DefaultPageCacheTracer();
        var cursorContextFactory = new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER);
        cursorContext = cursorContextFactory.create("test");
    }

    @Test
    void scanningWithPreFetcherMustGiveScannerFewerPageFaultsWhenScannerIsFast() throws Exception {
        scanner = cursor -> cursor.putBytes(PageCache.PAGE_SIZE, (byte) 0xA7); // This is pretty fast.

        runScan(file, cursorContext, "Warmup", PF_READ_AHEAD);
        long faultsWithPreFetch = runScan(file, cursorContext, "Scanner With Prefetch", PF_READ_AHEAD);
        long faultsWithoutPreFetch = runScan(file, cursorContext, "Scanner Without Prefetch", 0);

        assertThat(faultsWithPreFetch).as("faults").isLessThan(faultsWithoutPreFetch);
    }

    @Test
    void scanningWithPreFetchMustGiveScannerFewerPageFaultsWhenScannerIsSlow() throws Exception {
        RecordFormat<RelationshipRecord> format = defaultFormat().relationship();
        RelationshipRecord record = format.newRecord();
        int recordSize = format.getRecordSize(NoStoreHeader.NO_STORE_HEADER);
        int recordsPerPage = PageCache.PAGE_SIZE / recordSize;
        SplittableRandom rng = new SplittableRandom(13);

        // This scanner is a bit on the slow side:
        scanner = cursor -> {
            for (int j = 0; j < recordsPerPage; j++) {
                try {
                    record.initialize(
                            rng.nextBoolean(),
                            rng.nextInt(),
                            rng.nextInt(),
                            rng.nextInt(),
                            rng.nextInt() & 0xFFFF,
                            rng.nextInt(),
                            rng.nextInt(),
                            rng.nextInt(),
                            rng.nextInt(),
                            rng.nextBoolean(),
                            rng.nextBoolean());
                    format.write(record, cursor, recordSize, recordsPerPage);
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        };

        runScan(file, cursorContext, "Warmup", PF_READ_AHEAD);
        long faultsWithPreFetch = runScan(file, cursorContext, "Scanner With Prefetch", PF_READ_AHEAD);
        long faultsWithoutPreFetch = runScan(file, cursorContext, "Scanner Without Prefetch", 0);

        assertThat(faultsWithPreFetch).as("faults").isLessThan(faultsWithoutPreFetch);
    }

    private long runScan(Path file, CursorContext cursorContext, String threadName, int additionalPfFlags)
            throws InterruptedException {
        long faultsWith;
        RunnerThread thread = new RunnerThread(threadName);
        thread.additionalPfFlags = additionalPfFlags;
        thread.file = file;
        thread.cursorContext = cursorContext;
        thread.start();
        thread.join();
        faultsWith = thread.faults;
        return faultsWith;
    }

    private class RunnerThread extends Thread {
        private int additionalPfFlags;
        private Path file;
        private CursorContext cursorContext;
        private long faults;

        RunnerThread(String name) {
            super(name);
        }

        @Override
        public void run() {
            try {
                cursorContext.getCursorTracer().reportEvents();
                writeToFile(file, cursorContext, additionalPfFlags);
                faults = cursorContext.getCursorTracer().faults();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
    }

    private void writeToFile(Path file, CursorContext cursorContext, int additionalPfFlags) throws IOException {
        try (PagedFile pagedFile = pageCache.map(
                file,
                PageCache.PAGE_SIZE,
                DEFAULT_DATABASE_NAME,
                immutable.of(StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE))) {
            for (int i = 0; i < 5; i++) {
                writeToFile(pagedFile, cursorContext, additionalPfFlags);
            }
        }
    }

    private void writeToFile(PagedFile pagedFile, CursorContext cursorContext, int additionalPfFlags)
            throws IOException {
        try (PageCursor cursor = pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK | additionalPfFlags, cursorContext)) {
            for (int i = 0; i < 6_000; i++) {
                cursor.next();
                scanner.accept(cursor);
            }
        }
    }
}
