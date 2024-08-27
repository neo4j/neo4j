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
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.internal.recordstorage.RecordCursorTypes.NODE_CURSOR;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.kernel.impl.store.record.RecordLoad.NORMAL;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.DisabledOnOs;
import org.junit.jupiter.api.condition.OS;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.id.IdGenerator;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.cursor.CachedStoreCursors;
import org.neo4j.kernel.impl.store.record.NodeRecord;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@PageCacheExtension
@Neo4jLayoutExtension
class TestGrowingFileMemoryMapping {
    @Inject
    private PageCache pageCache;

    @Inject
    private TestDirectory testDirectory;

    @Inject
    private DatabaseLayout databaseLayout;

    @Test
    @DisabledOnOs(OS.WINDOWS)
    void shouldGrowAFileWhileContinuingToMemoryMapNewRegions() {
        // given
        final int NUMBER_OF_RECORDS = 1000000;

        Config config = Config.defaults();
        var pageCacheTracer = PageCacheTracer.NULL;
        DefaultIdGeneratorFactory idGeneratorFactory = new DefaultIdGeneratorFactory(
                testDirectory.getFileSystem(), immediate(), pageCacheTracer, databaseLayout.getDatabaseName());
        StoreFactory storeFactory = new StoreFactory(
                databaseLayout,
                config,
                idGeneratorFactory,
                pageCache,
                pageCacheTracer,
                testDirectory.getFileSystem(),
                NullLogProvider.getInstance(),
                new CursorContextFactory(pageCacheTracer, EMPTY_CONTEXT_SUPPLIER),
                false,
                LogTailLogVersionsMetadata.EMPTY_LOG_TAIL);

        try (NeoStores neoStores = storeFactory.openAllNeoStores();
                var storeCursors = new CachedStoreCursors(neoStores, CursorContext.NULL_CONTEXT)) {
            NodeStore nodeStore = neoStores.getNodeStore();
            IdGenerator idGenerator = nodeStore.getIdGenerator();
            // when
            int iterations = 2 * NUMBER_OF_RECORDS;
            long startingId = idGenerator.nextId(CursorContext.NULL_CONTEXT);
            long nodeId = startingId;
            try (PageCursor pageCursor = storeCursors.writeCursor(NODE_CURSOR)) {
                for (int i = 0; i < iterations; i++) {
                    NodeRecord record = new NodeRecord(nodeId).initialize(false, 0, false, i, 0);
                    record.setInUse(true);
                    nodeStore.updateRecord(record, pageCursor, CursorContext.NULL_CONTEXT, StoreCursors.NULL);
                    nodeId = idGenerator.nextId(CursorContext.NULL_CONTEXT);
                }
            }

            // then
            NodeRecord record = new NodeRecord(0).initialize(false, 0, false, 0, 0);
            var pageCursor = storeCursors.readCursor(NODE_CURSOR);
            for (int i = 0; i < iterations; i++) {
                record.setId(startingId + i);
                nodeStore.getRecordByCursor(i, record, NORMAL, pageCursor, EmptyMemoryTracker.INSTANCE);
                assertTrue(record.inUse(), "record[" + i + "] should be in use");
                assertThat(record.getNextRel())
                        .as("record[" + i + "] should have nextRelId of " + i)
                        .isEqualTo(i);
            }
        }
    }
}
