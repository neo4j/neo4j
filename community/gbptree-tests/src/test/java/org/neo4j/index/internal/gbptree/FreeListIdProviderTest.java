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
package org.neo4j.index.internal.gbptree;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.neo4j.index.internal.gbptree.FreelistIdProvider.NO_MONITOR;
import static org.neo4j.index.internal.gbptree.Generation.generation;
import static org.neo4j.index.internal.gbptree.Generation.stableGeneration;
import static org.neo4j.index.internal.gbptree.Generation.unstableGeneration;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.test.Race.throwing;

import java.io.IOException;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.index.internal.gbptree.FreelistIdProvider.Monitor;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;
import org.neo4j.io.pagecache.impl.muninn.StoreFile;
import org.neo4j.test.Race;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomSupportExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
@RandomSupportExtension
class FreeListIdProviderTest {
    private static final int PAYLOAD_SIZE = 128;
    private static final long GENERATION_ONE = GenerationSafePointer.MIN_GENERATION;
    private static final long GENERATION_TWO = GENERATION_ONE + 1;
    private static final long GENERATION_THREE = GENERATION_TWO + 1;
    private static final long BASE_ID = 5;

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory directory;

    @Inject
    private RandomSupport random;

    private final FreelistPageMonitor monitor = new FreelistPageMonitor();
    private FreelistIdProvider freelist;
    private PagedFile pagedFile;

    @BeforeEach
    void setUpPagedFile() throws IOException {
        pagedFile = pageCache.map(
                new StoreFile(directory.file("freelist")),
                PAYLOAD_SIZE,
                "db",
                Sets.immutable.of(StandardOpenOption.CREATE));
        freelist = new FreelistIdProvider(pagedFile, monitor);
        freelist.initialize(BASE_ID + 1, BASE_ID + 1, BASE_ID + 1, 0, 0);
    }

    @AfterEach
    void closePagedFile() {
        pagedFile.close();
    }

    @Test
    void shouldReleaseAndAcquireId() throws Exception {
        // GIVEN
        long releasedId = 11;
        fillPageWithRandomBytes(pagedFile, releasedId);

        // WHEN
        freelist.releaseId(GENERATION_ONE, GENERATION_TWO, releasedId, writeCursor(pagedFile));
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));
        long acquiredId = freelist.acquireNewId(GENERATION_TWO, writeCursor(pagedFile), NULL_CONTEXT);

        // THEN
        assertEquals(releasedId, acquiredId);
        try (var cursor = readCursor(pagedFile).create()) {
            cursor.next(acquiredId);
            assertEmpty(cursor);
        }
    }

    @Test
    void shouldReleaseAndAcquireIdsFromMultiplePages() throws Exception {
        // GIVEN
        int entries = freelist.entriesPerPage() + freelist.entriesPerPage() / 2;
        long baseId = 101;
        for (int i = 0; i < entries; i++) {
            freelist.releaseId(GENERATION_ONE, GENERATION_TWO, baseId + i, writeCursor(pagedFile));
        }
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));

        // WHEN/THEN
        for (int i = 0; i < entries; i++) {
            long acquiredId = freelist.acquireNewId(GENERATION_TWO, writeCursor(pagedFile), NULL_CONTEXT);
            assertEquals(baseId + i, acquiredId);
        }
    }

    @Test
    void shouldPutFreedFreeListPagesIntoFreeListAsWell() throws Exception {
        // GIVEN
        long prevId;
        long acquiredId = BASE_ID + 1;
        long freelistPageId = BASE_ID + 1;
        MutableLongSet released = new LongHashSet();
        do {
            prevId = acquiredId;
            acquiredId = freelist.acquireNewId(GENERATION_ONE, writeCursor(pagedFile), NULL_CONTEXT);
            freelist.releaseId(GENERATION_ONE, GENERATION_TWO, acquiredId, writeCursor(pagedFile));
            released.add(acquiredId);
        } while (acquiredId - prevId == 1);
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));

        // WHEN
        while (!released.isEmpty()) {
            long reAcquiredId = freelist.acquireNewId(GENERATION_TWO, writeCursor(pagedFile), NULL_CONTEXT);
            assertTrue(released.remove(reAcquiredId));
        }
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));

        // THEN
        assertEquals(freelistPageId, freelist.acquireNewId(GENERATION_THREE, writeCursor(pagedFile), NULL_CONTEXT));
    }

    @Test
    void shouldStayBoundUnderStress() throws Exception {
        // GIVEN
        MutableLongSet acquired = new LongHashSet();
        List<Long> acquiredList = new ArrayList<>(); // for quickly finding random to remove
        long stableGeneration = GenerationSafePointer.MIN_GENERATION;
        long unstableGeneration = stableGeneration + 1;
        int iterations = 1000;

        // WHEN
        for (int i = 0; i < iterations; i++) {
            for (int j = 0; j < 10; j++) {
                if (random.nextBoolean()) {
                    // acquire
                    int count = random.intBetween(5, 10);
                    for (int k = 0; k < count; k++) {
                        long acquiredId = freelist.acquireNewId(stableGeneration, writeCursor(pagedFile), NULL_CONTEXT);
                        assertTrue(acquired.add(acquiredId));
                        acquiredList.add(acquiredId);
                    }
                } else {
                    // release
                    int count = random.intBetween(5, 20);
                    for (int k = 0; k < count && !acquired.isEmpty(); k++) {
                        long id = acquiredList.remove(random.nextInt(acquiredList.size()));
                        assertTrue(acquired.remove(id));
                        freelist.releaseId(stableGeneration, unstableGeneration, id, writeCursor(pagedFile));
                    }
                }
            }

            for (long id : acquiredList) {
                freelist.releaseId(stableGeneration, unstableGeneration, id, writeCursor(pagedFile));
            }
            acquiredList.clear();
            acquired.clear();

            // checkpoint, sort of
            stableGeneration = unstableGeneration;
            unstableGeneration++;
        }

        // THEN
        assertTrue(freelist.lastId() < 200, String.valueOf(freelist.lastId()));
    }

    @Test
    void shouldStayBoundUnderMultiThreadedStress() {
        // given
        AtomicInteger checkpoints = new AtomicInteger();
        AtomicInteger numIdsAcquired = new AtomicInteger();
        AtomicInteger numIdsAcquiredThisCheckpoint = new AtomicInteger();
        AtomicInteger highestNumIdsAcquiredInCheckpoint = new AtomicInteger();
        Race race = new Race().withEndCondition(() -> checkpoints.get() >= 100 && numIdsAcquired.get() >= 10_000);
        ReadWriteLock checkpointLock = new ReentrantReadWriteLock();
        AtomicLong generation = new AtomicLong(generation(GENERATION_ONE, GENERATION_TWO));
        race.addContestants(4, throwing(() -> {
            checkpointLock.readLock().lock();
            try {
                long gen = generation.get();
                long stableGeneration = stableGeneration(gen);
                long unstableGeneration = unstableGeneration(gen);
                int count = ThreadLocalRandom.current().nextInt(1, 10);
                long[] ids = new long[count];
                for (int i = 0; i < count; i++) {
                    ids[i] = freelist.acquireNewId(stableGeneration, writeCursor(pagedFile), NULL_CONTEXT);
                }
                for (long id : ids) {
                    freelist.releaseId(stableGeneration, unstableGeneration, id, writeCursor(pagedFile));
                }
                numIdsAcquiredThisCheckpoint.addAndGet(count);
            } finally {
                checkpointLock.readLock().unlock();
            }
        }));
        race.addContestant(throwing(() -> {
            Thread.sleep(ThreadLocalRandom.current().nextInt(10));
            checkpointLock.writeLock().lock();
            try {
                long gen = generation.get();
                long unstableGeneration = unstableGeneration(gen);
                freelist.flush(stableGeneration(gen), unstableGeneration, writeCursor(pagedFile));
                generation.set(generation(unstableGeneration, unstableGeneration + 1));
                checkpoints.incrementAndGet();
                int idsThisCheckpoint = numIdsAcquiredThisCheckpoint.getAndSet(0);
                numIdsAcquired.addAndGet(idsThisCheckpoint);
                highestNumIdsAcquiredInCheckpoint.set(
                        Integer.max(highestNumIdsAcquiredInCheckpoint.get(), idsThisCheckpoint));
            } finally {
                checkpointLock.writeLock().unlock();
            }
        }));

        // when
        race.goUnchecked();

        // then the last id of the freelist after all that should be >= 80% of the highest number of ids acquired in any
        // single checkpoint.
        // This accounts for actual freelist pages for storing the freelist entries (page size is small resulting in
        // roughly one freelist page per 12 ids)
        assertThat((double) highestNumIdsAcquiredInCheckpoint.get() / freelist.lastId())
                .isGreaterThanOrEqualTo(0.8);
    }

    @Test
    void shouldAcquireAndReleaseUnderMultiThreadedStress() {
        // given
        AtomicInteger checkpoints = new AtomicInteger();
        AtomicInteger numIdsAcquired = new AtomicInteger();
        Race race = new Race().withEndCondition(() -> checkpoints.get() >= 100 && numIdsAcquired.get() >= 10_000);
        ReadWriteLock checkpointLock = new ReentrantReadWriteLock();
        AtomicLong generation = new AtomicLong(generation(GENERATION_ONE, GENERATION_TWO));
        List<long[]> acquisitions = new ArrayList<>();
        BitSet acquiredIds = new BitSet();
        race.addContestants(4, throwing(() -> {
            checkpointLock.readLock().lock();
            try {
                long gen = generation.get();
                long stableGeneration = stableGeneration(gen);
                long unstableGeneration = unstableGeneration(gen);
                ThreadLocalRandom rng = ThreadLocalRandom.current();
                boolean acquire = rng.nextBoolean();
                long[] idsToRelease = null;
                if (!acquire) {
                    synchronized (acquisitions) {
                        if (acquisitions.isEmpty()) {
                            acquire = true;
                        } else {
                            idsToRelease = acquisitions.remove(rng.nextInt(acquisitions.size()));
                        }
                    }
                }

                if (acquire) {
                    // Acquire
                    int count = rng.nextInt(1, 10);
                    long[] ids = new long[count];
                    for (int i = 0; i < count; i++) {
                        ids[i] = freelist.acquireNewId(stableGeneration, writeCursor(pagedFile), NULL_CONTEXT);
                    }
                    synchronized (acquisitions) {
                        acquisitions.add(ids);
                        for (long id : ids) {
                            if (acquiredIds.get((int) id)) {
                                fail(id + " already acquired");
                            }
                            acquiredIds.set((int) id);
                        }
                    }
                    numIdsAcquired.addAndGet(count);
                } else {
                    // Release
                    synchronized (acquisitions) {
                        for (long id : idsToRelease) {
                            if (!acquiredIds.get((int) id)) {
                                fail(id + " not acquired");
                            }
                            acquiredIds.clear((int) id);
                        }
                    }
                    for (long id : idsToRelease) {
                        freelist.releaseId(stableGeneration, unstableGeneration, id, writeCursor(pagedFile));
                    }
                }
            } finally {
                checkpointLock.readLock().unlock();
            }
        }));
        race.addContestant(throwing(() -> {
            Thread.sleep(ThreadLocalRandom.current().nextInt(10));
            checkpointLock.writeLock().lock();
            try {
                long gen = generation.get();
                long unstableGeneration = unstableGeneration(gen);
                freelist.flush(stableGeneration(gen), unstableGeneration, writeCursor(pagedFile));
                generation.set(generation(unstableGeneration, unstableGeneration + 1));
                checkpoints.incrementAndGet();
            } finally {
                checkpointLock.writeLock().unlock();
            }
        }));

        // when
        race.goUnchecked();
    }

    @Test
    void shouldVisitUnacquiredIds() throws Exception {
        // GIVEN a couple of released ids
        MutableLongSet expected = new LongHashSet();
        for (int i = 0; i < 100; i++) {
            expected.add(freelist.acquireNewId(GENERATION_ONE, writeCursor(pagedFile), NULL_CONTEXT));
        }
        expected.forEach(id -> {
            try {
                freelist.releaseId(GENERATION_ONE, GENERATION_TWO, id, writeCursor(pagedFile));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));
        // and only a few acquired
        for (int i = 0; i < 10; i++) {
            long acquiredId = freelist.acquireNewId(GENERATION_TWO, writeCursor(pagedFile), NULL_CONTEXT);
            assertTrue(expected.remove(acquiredId));
        }

        // WHEN/THEN
        freelist.visitFreelist(
                new IdProvider.IdProviderVisitor.Adaptor() {
                    @Override
                    public void freelistEntry(long pageId, long generation, int pos) {
                        assertTrue(expected.remove(pageId));
                    }
                },
                readCursor(pagedFile));
        assertTrue(expected.isEmpty());
    }

    @Test
    void shouldVisitFreelistPageIds() throws Exception {
        // GIVEN a couple of released ids
        MutableLongSet expected = new LongHashSet();
        // Add the already allocated free-list page id
        expected.add(BASE_ID + 1);
        monitor.set(new Monitor() {
            @Override
            public void acquiredFreelistPageId(long freelistPageId) {
                expected.add(freelistPageId);
            }
        });
        for (int i = 0; i < 100; i++) {
            long id = freelist.acquireNewId(GENERATION_ONE, writeCursor(pagedFile), NULL_CONTEXT);
            freelist.releaseId(GENERATION_ONE, GENERATION_TWO, id, writeCursor(pagedFile));
        }
        freelist.flush(GENERATION_ONE, GENERATION_TWO, writeCursor(pagedFile));
        assertTrue(!expected.isEmpty());

        // WHEN/THEN
        freelist.visitFreelist(
                new IdProvider.IdProviderVisitor.Adaptor() {
                    @Override
                    public void beginFreelistPage(long pageId) {
                        assertTrue(expected.remove(pageId));
                    }
                },
                readCursor(pagedFile));
        assertTrue(expected.isEmpty());
    }

    @Test
    void shouldVisitUnreleasedFreelistPageIds() throws Exception {
        // GIVEN a couple of released ids
        MutableLongSet expected = new LongHashSet();
        for (int i = 0; i < 10; i++) {
            long id = freelist.acquireNewId(GENERATION_ONE, writeCursor(pagedFile), NULL_CONTEXT);
            freelist.releaseId(GENERATION_ONE, GENERATION_TWO, id, writeCursor(pagedFile));
            expected.add(id);
        }

        // WHEN/THEN
        freelist.visitFreelist(
                new IdProvider.IdProviderVisitor.Adaptor() {
                    @Override
                    public void freelistEntryFromReleaseCache(long pageId) {
                        assertTrue(expected.remove(pageId));
                    }
                },
                readCursor(pagedFile));
        assertTrue(expected.isEmpty());
    }

    static void fillPageWithRandomBytes(PagedFile pagedFile, long releasedId) throws IOException {
        try (var cursor = writeCursor(pagedFile).create()) {
            cursor.next(releasedId);
            byte[] crapData = new byte[PAYLOAD_SIZE];
            ThreadLocalRandom.current().nextBytes(crapData);
            cursor.putBytes(crapData);
        }
    }

    static void assertEmpty(PageCursor cursor) {
        byte[] data = new byte[PAYLOAD_SIZE];
        cursor.getBytes(data);
        for (byte b : data) {
            assertEquals(0, b);
        }
    }

    static CursorCreator readCursor(PagedFile pagedFile) {
        return () -> pagedFile.io(0, PagedFile.PF_SHARED_READ_LOCK, NULL_CONTEXT);
    }

    static CursorCreator writeCursor(PagedFile pagedFile) {
        return () -> pagedFile.io(0, PagedFile.PF_SHARED_WRITE_LOCK, NULL_CONTEXT);
    }

    static class FreelistPageMonitor implements Monitor {
        private Monitor actual = NO_MONITOR;

        void set(Monitor actual) {
            this.actual = actual;
        }

        @Override
        public void acquiredFreelistPageId(long freelistPageId) {
            actual.acquiredFreelistPageId(freelistPageId);
        }

        @Override
        public void releasedFreelistPageId(long freelistPageId) {
            actual.releasedFreelistPageId(freelistPageId);
        }
    }
}
