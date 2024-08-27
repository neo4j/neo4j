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
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.helpers.MathUtil.ceil;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.RELATIONSHIP_CURSOR;
import static org.neo4j.io.IOUtils.closeAllUnchecked;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.AbstractBaseRecord.NO_ID;

import java.util.HashSet;
import java.util.Set;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.list.primitive.LongInterval;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
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
import org.neo4j.kernel.impl.store.NeoStores;
import org.neo4j.kernel.impl.store.RelationshipStore;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.RelationshipRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.EphemeralNeo4jLayoutExtension;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;

@EphemeralPageCacheExtension
@EphemeralNeo4jLayoutExtension
@ExtendWith(RandomExtension.class)
class RecordRelationshipScanCursorTest {
    private static final long RELATIONSHIP_ID = 1L;

    @Inject
    private RandomSupport random;

    @Inject
    private FileSystemAbstraction fileSystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private RecordDatabaseLayout databaseLayout;

    private NeoStores neoStores;
    private StoreCursors storeCursors;

    @AfterEach
    void tearDown() {
        closeAllUnchecked(storeCursors, neoStores);
    }

    @BeforeEach
    void setUp() {
        StoreFactory storeFactory = getStoreFactory();
        neoStores = storeFactory.openAllNeoStores();
        storeCursors = new CachedStoreCursors(neoStores, NULL_CONTEXT);
    }

    @Test
    void retrieveUsedRelationship() {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        try (var writeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            createRelationshipRecord(RELATIONSHIP_ID, 1, relationshipStore, writeCursor, true);
        }

        try (RecordRelationshipScanCursor cursor = createRelationshipCursor()) {
            cursor.single(RELATIONSHIP_ID);
            assertThat(cursor.next()).isTrue();
            assertThat(cursor.entityReference()).isEqualTo(RELATIONSHIP_ID);
            assertThat(cursor.next()).isFalse();
            assertThat(cursor.entityReference()).isEqualTo(NO_ID);
        }
    }

    @Test
    void retrieveUnusedRelationship() {
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        relationshipStore.setHighId(10);
        try (var writeCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            createRelationshipRecord(RELATIONSHIP_ID, 1, relationshipStore, writeCursor, false);
        }

        try (RecordRelationshipScanCursor cursor = createRelationshipCursor()) {
            cursor.single(RELATIONSHIP_ID);
            assertThat(cursor.next()).isFalse();
            assertThat(cursor.entityReference()).isEqualTo(NO_ID);
        }
    }

    @Test
    void shouldScanAllInUseRelationships() {
        // given
        RelationshipStore relationshipStore = neoStores.getRelationshipStore();
        int count = 100;
        relationshipStore.setHighId(count + 1);
        Set<Long> expected = new HashSet<>();
        try (var cursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            for (long id = 0; id < count; id++) {
                boolean inUse = random.nextBoolean();
                createRelationshipRecord(id, 1, relationshipStore, cursor, inUse);
                if (inUse) {
                    expected.add(id);
                }
            }
        }

        // when
        assertSeesRelationships(expected);
    }

    @Test
    void shouldExhaustRelationshipsWithBatches() {
        final var ids = LongInterval.oneTo(random.nextInt(23, 42)).toSet();
        try (var pageCursor = storeCursors.writeCursor(RELATIONSHIP_CURSOR)) {
            final var store = neoStores.getRelationshipStore();
            store.setHighId(ids.size() + 1);
            ids.forEach(id -> createRelationshipRecord(id, 1, store, pageCursor, true));
        }

        try (var rels = createRelationshipCursor()) {
            final var scan = new RecordRelationshipScan();
            final var found = LongSets.mutable.withInitialCapacity(ids.size());

            // scan a quarter of the relationships
            assertThat(rels.scanBatch(scan, ceil(ids.size(), 4))).isTrue();
            while (rels.next()) {
                assertThat(found.add(rels.entityReference())).isTrue();
            }
            assertThat(ids.containsAll(found)).isTrue();

            // scan the rest of the relationships
            assertThat(rels.scanBatch(scan, Long.MAX_VALUE)).isTrue();
            while (rels.next()) {
                assertThat(found.add(rels.entityReference())).isTrue();
            }
            assertThat(found).isEqualTo(ids);

            // attempt to scan anything more a few times
            for (int i = 0, n = random.nextInt(2, 10); i < n; i++) {
                assertThat(rels.scanBatch(scan, Long.MAX_VALUE)).isFalse();
            }
            assertThat(rels.entityReference()).isEqualTo(NO_ID);
        }
    }

    private void assertSeesRelationships(Set<Long> expected) {
        try (RecordRelationshipScanCursor cursor = createRelationshipCursor()) {
            cursor.scan();
            while (cursor.next()) {
                // then
                assertThat(expected.remove(cursor.entityReference()))
                        .as(cursor.toString())
                        .isTrue();
            }
            assertThat(cursor.entityReference()).isEqualTo(NO_ID);
        }
        assertThat(expected).isEmpty();
    }

    private static void createRelationshipRecord(
            long id, int type, RelationshipStore relationshipStore, PageCursor pageCursor, boolean used) {
        relationshipStore.updateRecord(
                new RelationshipRecord(id).initialize(used, -1, 1, 2, type, -1, -1, -1, -1, true, true),
                pageCursor,
                NULL_CONTEXT,
                StoreCursors.NULL);
    }

    private StoreFactory getStoreFactory() {
        var pageCacheTracer = PageCacheTracer.NULL;
        return new StoreFactory(
                databaseLayout,
                Config.defaults(),
                new DefaultIdGeneratorFactory(
                        fileSystem, immediate(), pageCacheTracer, databaseLayout.getDatabaseName()),
                pageCache,
                pageCacheTracer,
                fileSystem,
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);
    }

    private RecordRelationshipScanCursor createRelationshipCursor() {
        return new RecordRelationshipScanCursor(
                neoStores.getRelationshipStore(), NULL_CONTEXT, storeCursors, EmptyMemoryTracker.INSTANCE);
    }
}
