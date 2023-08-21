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
import static org.neo4j.internal.counts.GBPTreeCountsStore.NO_MONITOR;
import static org.neo4j.internal.counts.GBPTreeCountsStore.keyToString;
import static org.neo4j.internal.counts.GBPTreeCountsStore.nodeKey;
import static org.neo4j.internal.counts.GBPTreeCountsStore.relationshipKey;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Path;
import org.eclipse.collections.impl.factory.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.counts.CountsUpdater;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
class GBPTreeCountsStoreTest {
    private static final int LABEL_ID_1 = 1;
    private static final int LABEL_ID_2 = 2;
    private static final int RELATIONSHIP_TYPE_ID_1 = 1;
    private static final int RELATIONSHIP_TYPE_ID_2 = 2;

    @Inject
    private TestDirectory directory;

    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    private GBPTreeCountsStore countsStore;

    @BeforeEach
    void openCountsStore() throws Exception {
        openCountsStore(CountsBuilder.EMPTY);
    }

    @AfterEach
    void closeCountsStore() {
        countsStore.close();
    }

    @Test
    void failToApplySameTransactionTwice() {
        long txId = BASE_TX_ID + 1;

        try (var updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(LABEL_ID_1, 10);
        }
        assertThatThrownBy(() -> {
                    try (var updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
                        updater.incrementNodeCount(LABEL_ID_1, 10);
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
                try (var updater = countsStore.updater(txId, false, NULL_CONTEXT)) {
                    updater.incrementNodeCount(LABEL_ID_1, 10);
                }
            }

            try (var updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
                updater.incrementNodeCount(LABEL_ID_1, 10);
            }
        });
    }

    @Test
    void shouldUpdateAndReadSomeCounts() throws IOException {
        // given
        long txId = BASE_TX_ID;
        try (CountsUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(LABEL_ID_1, 10);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7);
        }
        try (CountsUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(LABEL_ID_1, 5); // now at 15
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 2); // now at 5
        }

        countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);

        // when/then
        assertEquals(15, countsStore.nodeCount(LABEL_ID_1, NULL_CONTEXT));
        assertEquals(5, countsStore.relationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, NULL_CONTEXT));
        assertEquals(7, countsStore.relationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, NULL_CONTEXT));

        // and when
        try (CountsUpdater updater = countsStore.updater(++txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(LABEL_ID_1, -7);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, -5);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, -2);
        }

        // then
        assertEquals(8, countsStore.nodeCount(LABEL_ID_1, NULL_CONTEXT));
        assertEquals(0, countsStore.relationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, NULL_CONTEXT));
        assertEquals(5, countsStore.relationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, NULL_CONTEXT));
    }

    @Test
    void shouldUseCountsBuilderOnCreation() throws Exception {
        // given
        long rebuiltAtTransactionId = 5;
        int labelId = 3;
        int labelId2 = 6;
        int relationshipTypeId = 7;
        closeCountsStore();
        deleteCountsStore();

        // when
        TestableCountsBuilder builder = new TestableCountsBuilder(rebuiltAtTransactionId) {
            @Override
            public void initialize(CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
                super.initialize(updater, cursorContext, memoryTracker);
                updater.incrementNodeCount(labelId, 10);
                updater.incrementRelationshipCount(labelId, relationshipTypeId, labelId2, 14);
            }
        };
        openCountsStore(builder);
        assertTrue(builder.lastCommittedTxIdCalled);
        assertTrue(builder.initializeCalled);
        assertEquals(10, countsStore.nodeCount(labelId, NULL_CONTEXT));
        assertEquals(0, countsStore.nodeCount(labelId2, NULL_CONTEXT));
        assertEquals(14, countsStore.relationshipCount(labelId, relationshipTypeId, labelId2, NULL_CONTEXT));

        // and when
        checkpointAndRestartCountsStore();
        // Re-applying a txId below or equal to the "rebuild transaction id" should not apply it
        incrementNodeCount(rebuiltAtTransactionId - 1, labelId, 100);
        assertEquals(10, countsStore.nodeCount(labelId, NULL_CONTEXT));
        incrementNodeCount(rebuiltAtTransactionId, labelId, 100);
        assertEquals(10, countsStore.nodeCount(labelId, NULL_CONTEXT));

        // then
        incrementNodeCount(rebuiltAtTransactionId + 1, labelId, 100);
        assertEquals(110, countsStore.nodeCount(labelId, NULL_CONTEXT));
    }

    @Test
    void shouldDumpCountsStore() throws IOException {
        // given
        long txId = BASE_TX_ID + 1;
        try (CountsUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(LABEL_ID_1, 10);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2, 3);
            updater.incrementRelationshipCount(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2, 7);
        }
        countsStore.checkpoint(FileFlushEvent.NULL, NULL_CONTEXT);
        closeCountsStore();

        // when
        ByteArrayOutputStream out = new ByteArrayOutputStream(1024);
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        GBPTreeCountsStore.dump(
                pageCache,
                fs,
                countsStoreFile(),
                new PrintStream(out),
                new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                cacheTracer,
                immutable.empty());

        // then
        String dump = out.toString();
        assertThat(dump).contains(keyToString(nodeKey(LABEL_ID_1)) + " = 10");
        assertThat(dump)
                .contains(keyToString(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_1, LABEL_ID_2)) + " = 3");
        assertThat(dump)
                .contains(keyToString(relationshipKey(LABEL_ID_1, RELATIONSHIP_TYPE_ID_2, LABEL_ID_2)) + " = 7");
        assertThat(dump).contains("Highest gap-free txId: " + txId);
    }

    private void incrementNodeCount(long txId, int labelId, int delta) {
        try (CountsUpdater updater = countsStore.updater(txId, true, NULL_CONTEXT)) {
            updater.incrementNodeCount(labelId, delta);
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

    private void openCountsStore(CountsBuilder builder) throws IOException {
        instantiateCountsStore(builder, false, NO_MONITOR);
        countsStore.start(NULL_CONTEXT, INSTANCE);
    }

    private void instantiateCountsStore(
            CountsBuilder builder, boolean readOnly, GBPTreeGenericCountsStore.Monitor monitor) throws IOException {
        PageCacheTracer cacheTracer = PageCacheTracer.NULL;
        countsStore = new GBPTreeCountsStore(
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
                new CursorContextFactory(cacheTracer, EMPTY_CONTEXT_SUPPLIER),
                cacheTracer,
                Sets.immutable.empty());
    }

    private static class TestableCountsBuilder implements CountsBuilder {
        private final long rebuiltAtTransactionId;
        boolean lastCommittedTxIdCalled;
        boolean initializeCalled;

        TestableCountsBuilder(long rebuiltAtTransactionId) {
            this.rebuiltAtTransactionId = rebuiltAtTransactionId;
        }

        @Override
        public void initialize(CountsUpdater updater, CursorContext cursorContext, MemoryTracker memoryTracker) {
            initializeCalled = true;
        }

        @Override
        public long lastCommittedTxId() {
            lastCommittedTxIdCalled = true;
            return rebuiltAtTransactionId;
        }
    }
}
