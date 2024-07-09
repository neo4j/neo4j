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
package org.neo4j.kernel.impl.storemigration;

import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.neo4j.index.internal.gbptree.RecoveryCleanupWorkCollector.immediate;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.io.pagecache.context.CursorContextFactory.NULL_CONTEXT_FACTORY;
import static org.neo4j.io.pagecache.tracing.PageCacheTracer.NULL;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.input.InputEntityVisitor;
import org.neo4j.configuration.Config;
import org.neo4j.internal.id.DefaultIdGeneratorFactory;
import org.neo4j.internal.recordstorage.RecordStorageReader;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.kernel.impl.store.StoreFactory;
import org.neo4j.kernel.impl.transaction.log.LogTailLogVersionsMetadata;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.storageengine.api.StorageNodeCursor;
import org.neo4j.storageengine.api.StoragePropertyCursor;
import org.neo4j.storageengine.api.cursor.StoreCursors;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.pagecache.EphemeralPageCacheExtension;
import org.neo4j.test.utils.TestDirectory;

@EphemeralPageCacheExtension
class StoreScanChunkTest {
    @Inject
    private PageCache pageCache;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private TestDirectory directory;

    @Test
    void differentChunksHaveDifferentCursors() {
        var layout = RecordDatabaseLayout.ofFlat(directory.homePath());
        var idGeneratorFactory =
                new DefaultIdGeneratorFactory(fs, immediate(), false, NULL, layout.getDatabaseName(), true, true);
        try (var neoStores = new StoreFactory(
                        layout,
                        Config.defaults(),
                        idGeneratorFactory,
                        pageCache,
                        NULL,
                        fs,
                        NullLogProvider.getInstance(),
                        NULL_CONTEXT_FACTORY,
                        false,
                        LogTailLogVersionsMetadata.EMPTY_LOG_TAIL)
                .openAllNeoStores()) {
            RecordStorageReader storageReader = new RecordStorageReader(neoStores);
            TestStoreScanChunk scanChunk1 = new TestStoreScanChunk(storageReader, false);
            TestStoreScanChunk scanChunk2 = new TestStoreScanChunk(storageReader, false);
            assertNotSame(scanChunk1.getCursor(), scanChunk2.getCursor());
            assertNotSame(scanChunk1.getStorePropertyCursor(), scanChunk2.getStorePropertyCursor());
        }
    }

    private static class TestStoreScanChunk extends StoreScanChunk<StorageNodeCursor> {
        TestStoreScanChunk(RecordStorageReader storageReader, boolean requiresPropertyMigration) {
            super(
                    storageReader.allocateNodeCursor(NULL_CONTEXT, StoreCursors.NULL),
                    storageReader,
                    requiresPropertyMigration,
                    NULL_CONTEXT,
                    StoreCursors.NULL,
                    INSTANCE);
        }

        @Override
        protected void read(StorageNodeCursor cursor, long id) {
            cursor.single(id);
        }

        @Override
        void visitRecord(StorageNodeCursor record, InputEntityVisitor visitor) {
            // empty
        }

        StorageNodeCursor getCursor() {
            return cursor;
        }

        StoragePropertyCursor getStorePropertyCursor() {
            return storePropertyCursor;
        }
    }
}
