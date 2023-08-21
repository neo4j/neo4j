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
package org.neo4j.internal.counts;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.eclipse.collections.api.factory.Sets.immutable;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.counts.GBPTreeGenericCountsStore.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.degreeKey;
import static org.neo4j.internal.counts.GBPTreeRelationshipGroupDegreesStore.keyToString;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.RelationshipDirection.INCOMING;
import static org.neo4j.storageengine.api.RelationshipDirection.LOOP;
import static org.neo4j.storageengine.api.RelationshipDirection.OUTGOING;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.eclipse.collections.api.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.storageengine.api.RelationshipDirection;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
class GBPTreeRelationshipGroupDegreesStoreTest {
    private static final long GROUP_ID_1 = 1;
    private static final long GROUP_ID_2 = 2;
    private static final CursorContextFactory CONTEXT_FACTORY =
            new CursorContextFactory(PageCacheTracer.NULL, EMPTY_CONTEXT_SUPPLIER);

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    private GBPTreeRelationshipGroupDegreesStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception {
        openCountsStore(EMPTY_REBUILD);
    }

    @AfterEach
    void closeCountsStore() {
        countsStore.close();
    }

    @Test
    void failToApplySameTransactionTwice() {
        long txId = BASE_TX_ID + 1;

        try (DegreeUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.increment(GROUP_ID_1, OUTGOING, 10);
        }
        assertThatThrownBy(() -> {
                    try (DegreeUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
                        updater.increment(GROUP_ID_1, OUTGOING, 10);
                    }
                })
                .isInstanceOf(IllegalStateException.class)
                .hasMessageContaining("but highest gap-free is");
    }

    @Test
    void applySeveralChunksOfSameTransaction() {
        long txId = BASE_TX_ID + 1;

        assertDoesNotThrow(() -> {
            for (int i = 0; i < 100; i++) {
                try (DegreeUpdater updater = countsStore.updater(txId, false, NULL_CONTEXT)) {
                    updater.increment(GROUP_ID_1, OUTGOING, 10);
                }
            }

            try (DegreeUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
                updater.increment(GROUP_ID_1, OUTGOING, 10);
            }
        });
    }

    @Test
    void shouldUpdateAndReadSomeCounts() throws IOException {
        // given
        long txId = BASE_TX_ID;
        try (DegreeUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.increment(GROUP_ID_1, OUTGOING, 10);
            updater.increment(GROUP_ID_1, INCOMING, 3);
            updater.increment(GROUP_ID_2, OUTGOING, 7);
            updater.increment(GROUP_ID_2, LOOP, 14);
        }
        try (DegreeUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.increment(GROUP_ID_1, OUTGOING, 5); // now at 15
            updater.increment(GROUP_ID_1, INCOMING, 2); // now at 5
        }

        countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);

        // when/then
        assertEquals(15, countsStore.degree(GROUP_ID_1, OUTGOING, NULL_CONTEXT));
        assertEquals(5, countsStore.degree(GROUP_ID_1, INCOMING, NULL_CONTEXT));
        assertEquals(7, countsStore.degree(GROUP_ID_2, OUTGOING, NULL_CONTEXT));
        assertEquals(14, countsStore.degree(GROUP_ID_2, LOOP, NULL_CONTEXT));

        // and when
        try (DegreeUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.increment(GROUP_ID_1, OUTGOING, -7);
            updater.increment(GROUP_ID_1, INCOMING, -5);
            updater.increment(GROUP_ID_2, OUTGOING, -2);
        }

        // then
        assertEquals(8, countsStore.degree(GROUP_ID_1, OUTGOING, NULL_CONTEXT));
        assertEquals(0, countsStore.degree(GROUP_ID_1, INCOMING, NULL_CONTEXT));
        assertEquals(5, countsStore.degree(GROUP_ID_2, OUTGOING, NULL_CONTEXT));
        assertEquals(14, countsStore.degree(GROUP_ID_2, LOOP, NULL_CONTEXT));
    }

    @Test
    void shouldUseCountsBuilderOnCreation() throws Exception {
        // given
        long rebuiltAtTransactionId = 5;
        long groupId1 = 3;
        long groupId2 = 6;
        closeCountsStore();
        deleteCountsStore();

        // when
        TestableCountsBuilder builder = new TestableCountsBuilder(rebuiltAtTransactionId) {
            @Override
            public void rebuild(DegreeUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                super.rebuild(updater, cursorContext, memoryTracker);
                updater.increment(groupId1, OUTGOING, 10);
                updater.increment(groupId2, INCOMING, 14);
            }
        };
        openCountsStore(builder);
        assertTrue(builder.lastCommittedTxIdCalled);
        assertTrue(builder.rebuildCalled);
        assertEquals(10, countsStore.degree(groupId1, OUTGOING, NULL_CONTEXT));
        assertEquals(14, countsStore.degree(groupId2, INCOMING, NULL_CONTEXT));
        assertEquals(0, countsStore.degree(groupId1, INCOMING, NULL_CONTEXT));

        // and when
        checkpointAndRestartCountsStore();
        // Re-applying a txId below or equal to the "rebuild transaction id" should not apply it
        increment(rebuiltAtTransactionId - 1, groupId1, OUTGOING, 100);
        assertEquals(10, countsStore.degree(groupId1, OUTGOING, NULL_CONTEXT));
        increment(rebuiltAtTransactionId, groupId1, OUTGOING, 100);
        assertEquals(10, countsStore.degree(groupId1, OUTGOING, NULL_CONTEXT));

        // then
        increment(rebuiltAtTransactionId + 1, groupId1, OUTGOING, 100);
        assertEquals(110, countsStore.degree(groupId1, OUTGOING, NULL_CONTEXT));
    }

    @Test
    void shouldDumpCountsStore() throws IOException {
        // given
        long txId = BASE_TX_ID + 1;
        try (DegreeUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.increment(GROUP_ID_1, OUTGOING, 10);
            updater.increment(GROUP_ID_1, INCOMING, 3);
            updater.increment(GROUP_ID_2, LOOP, 7);
        }
        countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        closeCountsStore();

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        GBPTreeRelationshipGroupDegreesStore.dump(
                pageCache,
                fs,
                countsStoreFile(),
                new PrintStream(out),
                CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                immutable.empty());

        // then
        String dump = out.toString();
        assertThat(dump).contains(keyToString(degreeKey(GROUP_ID_1, OUTGOING)) + " = 10");
        assertThat(dump).contains(keyToString(degreeKey(GROUP_ID_1, INCOMING)) + " = 3");
        assertThat(dump).contains(keyToString(degreeKey(GROUP_ID_2, LOOP)) + " = 7");
        assertThat(dump).contains("Highest gap-free txId: " + txId);
    }

    private void increment(long txId, long groupId, RelationshipDirection direction, int delta) {
        try (DegreeUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.increment(groupId, direction, delta);
        }
    }

    private void checkpointAndRestartCountsStore() throws Exception {
        countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        closeCountsStore();
        openCountsStore();
    }

    private void deleteCountsStore() throws IOException {
        directory.getFileSystem().deleteFile(countsStoreFile());
    }

    private Path countsStoreFile() {
        return directory.file("counts.db");
    }

    private void openCountsStore(DegreesRebuilder builder) throws IOException {
        instantiateCountsStore(builder, false, NO_MONITOR);
        countsStore.start(NULL_CONTEXT, INSTANCE);
    }

    private void instantiateCountsStore(
            DegreesRebuilder builder, boolean readOnly, GBPTreeGenericCountsStore.Monitor monitor) throws IOException {
        countsStore = new GBPTreeRelationshipGroupDegreesStore(
                pageCache,
                countsStoreFile(),
                fs,
                immediate(),
                builder,
                readOnly,
                monitor,
                DEFAULT_DATABASE_NAME,
                10,
                NullLogProvider.getInstance(),
                CONTEXT_FACTORY,
                PageCacheTracer.NULL,
                Sets.immutable.empty());
    }

    private static class TestableCountsBuilder implements DegreesRebuilder {
        private final long rebuiltAtTransactionId;
        boolean lastCommittedTxIdCalled;
        boolean rebuildCalled;

        TestableCountsBuilder(long rebuiltAtTransactionId) {
            this.rebuiltAtTransactionId = rebuiltAtTransactionId;
        }

        @Override
        public void rebuild(DegreeUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            rebuildCalled = true;
        }

        @Override
        public long lastCommittedTxId() {
            lastCommittedTxIdCalled = true;
            return rebuiltAtTransactionId;
        }
    }

    private static final DegreesRebuilder EMPTY_REBUILD =
            new GBPTreeRelationshipGroupDegreesStore.EmptyDegreesRebuilder(BASE_TX_ID);
}
