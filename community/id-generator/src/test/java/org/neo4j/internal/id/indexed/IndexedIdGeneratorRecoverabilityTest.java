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
package org.neo4j.internal.id.indexed;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.collection.PrimitiveLongResourceCollections.count;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.id.FreeIds.NO_FREE_IDS;
import static org.neo4j.internal.id.IdSlotDistribution.SINGLE_IDS;
import static org.neo4j.internal.id.indexed.IndexedIdGenerator.NO_MONITOR;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.test.utils.PageCacheConfig.config;

import java.io.IOException;
import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Sets;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.internal.id.TestIdType;
import org.neo4j.io.fs.EphemeralFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DatabaseFlushEvent;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.PageCacheSupport;
import org.neo4j.test.utils.TestDirectory;

@ExtendWith(RandomExtension.class)
@EphemeralPageCacheExtension
class IndexedIdGeneratorRecoverabilityTest {
    private static final TestIdType ID_TYPE = TestIdType.TEST;

    private static final String ID_FILE_NAME = "some.id";

    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private EphemeralFileSystemAbstraction fs;

    @Inject
    private RandomSupport random;

    @Test
    void persistHighIdBetweenCleanRestarts() throws IOException {
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            freelist.nextId(NULL_CONTEXT);
            assertEquals(1, freelist.getHighId());
            freelist.nextId(NULL_CONTEXT);
            assertEquals(2, freelist.getHighId());
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            assertEquals(2, freelist.getHighId());
        }
    }

    @Test
    void doNotPersistHighIdBetweenCleanRestartsWithoutCheckpoint() throws IOException {
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            freelist.nextId(NULL_CONTEXT);
            assertEquals(1, freelist.getHighId());
            freelist.nextId(NULL_CONTEXT);
            assertEquals(2, freelist.getHighId());
        }
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            assertEquals(0, freelist.getHighId());
        }
    }

    @Test
    void shouldPersistNumUnusedIdsOnCheckpoint() throws IOException {
        // given
        var expectedNumUnusedIds = new MutableInt();
        try (var idGenerator = instantiateFreelist()) {
            idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);
            var ids = LongLists.mutable.empty();
            for (int i = 0; i < 10; i++) {
                ids.add(idGenerator.nextId(NULL_CONTEXT));
            }
            markUsed(idGenerator, ids.toArray());
            ids.forEach(id -> {
                if (random.nextBoolean()) {
                    markDeleted(idGenerator, id);
                    expectedNumUnusedIds.increment();
                }
            });
            assertThat(count(idGenerator.notUsedIdsIterator())).isEqualTo(expectedNumUnusedIds.intValue());
            assertThat(idGenerator.getUnusedIdCount()).isEqualTo(expectedNumUnusedIds.intValue());
            idGenerator.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        // when
        try (var idGenerator = instantiateFreelist()) {
            idGenerator.start(NO_FREE_IDS, NULL_CONTEXT);

            // then
            assertThat(count(idGenerator.notUsedIdsIterator())).isEqualTo(expectedNumUnusedIds.intValue());
            assertThat(idGenerator.getUnusedIdCount()).isEqualTo(expectedNumUnusedIds.intValue());
        }
    }

    @Test
    void simpleCrashTest() throws Exception {
        final EphemeralFileSystemAbstraction snapshot;
        final long id1;
        final long id2;
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            id1 = freelist.nextId(NULL_CONTEXT);
            id2 = freelist.nextId(NULL_CONTEXT);
            markUsed(freelist, id1, id2);
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
            markDeleted(freelist, id1, id2);
            pageCache.flushAndForce(DatabaseFlushEvent.NULL);
            snapshot = fs.snapshot();
        }

        try (PageCache newPageCache = getPageCache(snapshot);
                IdGenerator freelist = instantiateFreelist(newPageCache)) {
            markDeleted(freelist, id1, id2);

            // Recovery is completed ^^^
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            markFree(freelist, id1, id2);

            freelist.maintenance(NULL_CONTEXT);
            final ImmutableLongSet reused =
                    LongSets.immutable.of(freelist.nextId(NULL_CONTEXT), freelist.nextId(NULL_CONTEXT));
            assertThat(reused).isEqualTo(LongSets.immutable.of(id1, id2));
            assertThat(freelist.getUnusedIdCount()).isEqualTo(count(freelist.notUsedIdsIterator()));
            assertThat(freelist.getUnusedIdCount()).isEqualTo(2);
        } finally {
            snapshot.close();
        }
    }

    @Test
    void resetUsabilityOnRestart() throws IOException {
        // Create the freelist
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        final long id1;
        final long id2;
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            id1 = freelist.nextId(NULL_CONTEXT);
            id2 = freelist.nextId(NULL_CONTEXT);
            markUsed(freelist, id1, id2);
            markDeleted(freelist, id1, id2);
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            final ImmutableLongSet reused =
                    LongSets.immutable.of(freelist.nextId(NULL_CONTEXT), freelist.nextId(NULL_CONTEXT));
            assertEquals(LongSets.immutable.of(id1, id2), reused, "IDs are not reused");
        }
    }

    @Test
    void resetUsabilityOnRestartWithSomeWrites() throws IOException {
        // Create the freelist
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        final long id1;
        final long id2;
        final long id3;
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            id1 = freelist.nextId(NULL_CONTEXT);
            id2 = freelist.nextId(NULL_CONTEXT);
            id3 = freelist.nextId(NULL_CONTEXT);
            markUsed(freelist, id1, id2, id3);
            markDeleted(freelist, id1, id2); // <-- Don't delete id3
            // Intentionally don't mark the ids as reusable
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        }

        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);

            // Here we expected that id1 and id2 will be reusable, even if they weren't marked as such in the previous
            // session Making changes to the tree entry where they live will update the generation and all of a
            // sudden the reusable bits in that entry will matter when we want to allocate. This is why we now want
            // to make a change to that tree entry and after that do an allocation to see if we still get them.
            markDeleted(freelist, id3);

            final ImmutableLongSet reused =
                    LongSets.immutable.of(freelist.nextId(NULL_CONTEXT), freelist.nextId(NULL_CONTEXT));
            assertEquals(LongSets.immutable.of(id1, id2), reused, "IDs are not reused");
        }
    }

    @Test
    void avoidNormalizationDuringRecovery() throws IOException {
        long id;
        long neighbourId;
        try (IdGenerator freelist = instantiateFreelist()) {
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            id = freelist.nextId(NULL_CONTEXT);
            neighbourId = freelist.nextId(NULL_CONTEXT);
            markUsed(freelist, id, neighbourId);
            markDeleted(freelist, id, neighbourId);
            // Crash (no checkpoint)
        }

        try (IdGenerator freelist = instantiateFreelist()) {
            // Recovery
            markUsed(freelist, id, neighbourId);
            markDeleted(freelist, id, neighbourId);
            freelist.start(
                    visitor -> {
                        visitor.accept(id);
                        visitor.accept(neighbourId);
                        return neighbourId;
                    },
                    NULL_CONTEXT);
            freelist.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);

            // Normal operations
            markFree(freelist, id);
            freelist.maintenance(NULL_CONTEXT);
            long idAfterRecovery = freelist.nextId(NULL_CONTEXT);
            assertEquals(id, idAfterRecovery);
            markUsed(freelist, id);
            // Crash (no checkpoint)
        }

        try (IdGenerator freelist = instantiateFreelist()) {
            // Recovery
            // If normalization happens on recovery then this transition, which really should be DELETED (last
            // check-pointed state) -> USED
            // instead becomes normalized from DELETED -> FREE and the real transition becomes FREE -> RESERVED
            markUsed(freelist, id);

            // Normal operations
            freelist.start(NO_FREE_IDS, NULL_CONTEXT);
            markDeleted(freelist, id); // <-- this must be OK

            // And as an extra measure of verification
            markFree(freelist, id);
            MutableLongSet expected = LongSets.mutable.with(id, neighbourId);
            freelist.maintenance(NULL_CONTEXT);
            assertTrue(expected.remove(freelist.nextId(NULL_CONTEXT)));
            assertTrue(expected.remove(freelist.nextId(NULL_CONTEXT)));
            assertTrue(expected.isEmpty());
            assertThat(freelist.getUnusedIdCount()).isEqualTo(2);
        }
    }

    private IndexedIdGenerator instantiateFreelist() {
        return instantiateFreelist(pageCache);
    }

    private IndexedIdGenerator instantiateFreelist(PageCache pageCache) {
        return new IndexedIdGenerator(
                pageCache,
                fs,
                testDirectory.file(ID_FILE_NAME),
                immediate(),
                ID_TYPE,
                true,
                () -> 0,
                Long.MAX_VALUE,
                false,
                Config.defaults(),
                DEFAULT_DATABASE_NAME,
                new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER),
                NO_MONITOR,
                Sets.immutable.empty(),
                SINGLE_IDS,
                PageCacheTracer.NULL,
                true,
                true);
    }

    private static PageCache getPageCache(FileSystemAbstraction fs) {
        return new PageCacheSupport().getPageCache(fs, config());
    }

    private static void markUsed(IdGenerator freelist, long... ids) {
        try (var commitMarker = freelist.transactionalMarker(NULL_CONTEXT)) {
            for (long id : ids) {
                commitMarker.markUsed(id);
            }
        }
    }

    private static void markDeleted(IdGenerator freelist, long... ids) {
        try (var commitMarker = freelist.transactionalMarker(NULL_CONTEXT)) {
            for (long id : ids) {
                commitMarker.markDeleted(id);
            }
        }
    }

    private static void markFree(IdGenerator freelist, long... ids) {
        try (var reuseMarker = freelist.contextualMarker(NULL_CONTEXT)) {
            for (long id : ids) {
                reuseMarker.markFree(id);
            }
        }
    }
}
