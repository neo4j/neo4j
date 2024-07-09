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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assumptions.assumeThat;
import static org.neo4j.io.pagecache.context.FixedVersionContextSupplier.EMPTY_CONTEXT_SUPPLIER;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.batchimport.api.IndexImporterFactory;
import org.neo4j.configuration.Config;
import org.neo4j.internal.batchimport.BatchImporterFactory;
import org.neo4j.internal.helpers.progress.ProgressListener;
import org.neo4j.internal.recordstorage.RecordStorageEngineFactory;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.layout.recordstorage.RecordDatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.DefaultPageCacheTracer;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.store.MetaDataStore;
import org.neo4j.kernel.impl.store.format.standard.Standard;
import org.neo4j.kernel.impl.transaction.log.EmptyLogTailMetadata;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.test.extension.pagecache.PageCacheExtension;
import org.neo4j.test.scheduler.ThreadPoolJobScheduler;

@PageCacheExtension
@Neo4jLayoutExtension
class RecordStoreMigratorTest {
    @Inject
    private FileSystemAbstraction filesystem;

    @Inject
    private PageCache pageCache;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Inject
    private DatabaseLayout databaseLayout;

    private JobScheduler jobScheduler;
    private final BatchImporterFactory batchImporterFactory = BatchImporterFactory.withHighestPriority();
    private final CursorContextFactory contextFactory =
            new CursorContextFactory(new DefaultPageCacheTracer(), EMPTY_CONTEXT_SUPPLIER);

    @BeforeEach
    void setUp() {
        assumeThat(databaseLayout).isInstanceOf(RecordDatabaseLayout.class);
        jobScheduler = new ThreadPoolJobScheduler();
    }

    @AfterEach
    void tearDown() throws Exception {
        jobScheduler.close();
    }

    @Test
    void shouldNotMigrateFilesForVersionsWithSameCapability() throws Exception {
        // Prepare migrator and file
        RecordStorageMigrator migrator = newStoreMigrator();
        filesystem.write(databaseLayout.pathForExistsMarker()).close();

        // Monitor what happens
        MyProcessListener progressReporter = new MyProcessListener();
        // Migrate with two storeversions that have the same FORMAT capabilities
        DatabaseLayout migrationLayout = RecordDatabaseLayout.of(neo4jLayout, "migrationDir");
        filesystem.mkdirs(migrationLayout.databaseDirectory());

        var format = Standard.LATEST_RECORD_FORMATS;
        filesystem.write(migrationLayout.pathForExistsMarker()).close();
        var fieldAccess = MetaDataStore.getFieldAccess(
                pageCache,
                migrationLayout.metadataStore(),
                migrationLayout.getDatabaseName(),
                CursorContext.NULL_CONTEXT);
        fieldAccess.writeStoreId(StoreId.generateNew(
                RecordStorageEngineFactory.NAME,
                format.getFormatFamily().name(),
                format.majorVersion(),
                format.minorVersion()));

        var storeVersionIdentifier = new StoreVersionIdentifier(
                RecordStorageEngineFactory.NAME,
                format.getFormatFamily().name(),
                format.majorVersion(),
                format.minorVersion());

        var storageEngineFactory = StorageEngineFactory.defaultStorageEngine();
        migrator.migrate(
                databaseLayout,
                migrationLayout,
                progressReporter,
                storageEngineFactory.versionInformation(storeVersionIdentifier).orElseThrow(),
                storageEngineFactory.versionInformation(storeVersionIdentifier).orElseThrow(),
                IndexImporterFactory.EMPTY,
                new EmptyLogTailMetadata(Config.defaults()));

        // Should not have started any migration
        assertThat(progressReporter.added).isFalse();
    }

    private RecordStorageMigrator newStoreMigrator() {
        return new RecordStorageMigrator(
                filesystem,
                pageCache,
                PageCacheTracer.NULL,
                Config.defaults(),
                NullLogService.getInstance(),
                jobScheduler,
                contextFactory,
                batchImporterFactory,
                INSTANCE,
                false);
    }

    private static class MyProcessListener implements ProgressListener {
        public boolean added;

        MyProcessListener() {
            added = false;
        }

        @Override
        public void add(long progress) {
            added = true;
        }

        @Override
        public void mark(char mark) {}

        @Override
        public void close() {}

        @Override
        public void failed(Throwable e) {}

        @Override
        public ProgressListener threadLocalReporter(int threshold) {
            return null;
        }
    }
}
