/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.disk;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.neo4j.configuration.GraphDatabaseSettings.DEFAULT_DATABASE_NAME;
import static org.neo4j.configuration.GraphDatabaseSettings.preallocate_store_files;
import static org.neo4j.io.mem.MemoryAllocator.createAllocator;
import static org.neo4j.memory.MemoryGroup.PAGE_CACHE;

import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import org.neo4j.graphdb.WriteOperationsNotAllowedException;
import org.neo4j.graphdb.event.DatabaseEventContext;
import org.neo4j.graphdb.event.DatabaseEventListenerAdapter;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.nativeimpl.ErrorTranslator;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.OutOfDiskSpaceException;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.impl.factory.DbmsInfo;
import org.neo4j.kernel.impl.store.format.standard.NodeRecordFormat;
import org.neo4j.kernel.internal.GraphDatabaseAPI;
import org.neo4j.kernel.monitoring.tracing.Tracers;
import org.neo4j.logging.internal.LogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.test.TestDatabaseManagementServiceBuilder;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.Neo4jLayoutExtension;
import org.neo4j.time.SystemNanoClock;

@Neo4jLayoutExtension
class OutOfDiskByStoreGrowTest {
    // assuming at least 15 bytes per node will get us over the limit regardless of storage engine and sub-format
    private static final int PAGE_COUNT_LIMIT = 10;
    private static final long FILE_SIZE_LIMIT = PageCache.PAGE_SIZE * PAGE_COUNT_LIMIT;
    private static final int RECORDS_PER_PAGE = PageCache.PAGE_SIZE / NodeRecordFormat.RECORD_SIZE;
    private static final int RECORD_LIMIT = RECORDS_PER_PAGE * PAGE_COUNT_LIMIT;

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    @Test
    void outOfDiskDuringNodeStoreGrowCausesTheDatabaseToPanic() {
        AtomicReference<DatabaseEventContext> outOfDiskSpaceEvent = new AtomicReference<>();
        var listener = new DatabaseEventListenerAdapter() {
            @Override
            public void databaseOutOfDiskSpace(DatabaseEventContext event) {
                outOfDiskSpaceEvent.set(event);
            }
        };
        var dbms = new TestDatabaseManagementServiceBuilderWithCustomPageCacheNativeAccess(
                        neo4jLayout.homeDirectory(), fs)
                .setConfig(GraphDatabaseInternalSettings.out_of_disk_space_protection, true)
                .addDatabaseListener(listener)
                .build();
        try {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            assertThat(isReadOnly(db)).isFalse();
            assertThatThrownBy(() -> {
                        int i = 0;
                        // To make sure we cross the boundary
                        var countLimit = RECORD_LIMIT + 2 * RECORDS_PER_PAGE;
                        while (i < countLimit) {
                            try (var tx = db.beginTx()) {
                                for (int localCount = 0; localCount < RECORDS_PER_PAGE; localCount++, i++) {
                                    tx.createNode();
                                }
                                tx.commit();
                            }
                        }
                    })
                    .hasRootCauseInstanceOf(OutOfDiskSpaceException.class)
                    .rootCause()
                    .hasMessageContaining("System is out of disk space for store file ");

            // Then no panic
            var health = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(DatabaseHealth.class);
            assertThat(health.hasNoPanic()).isTrue();

            // Then out of disk space event
            var actualOutOfDiskSpaceEvent = outOfDiskSpaceEvent.get();
            assertThat(actualOutOfDiskSpaceEvent).isNotNull();
            assertThat(actualOutOfDiskSpaceEvent.getDatabaseName()).isEqualTo(db.databaseName());

            // Then read only
            assertThat(isReadOnly(db)).isTrue();

            // Then following write transactions fail
            // FIXME ODP: Error message on write transactions should also contain reason for read only mode
            assertThatThrownBy(() -> {
                        try (Transaction tx = db.beginTx()) {
                            tx.createNode();
                        }
                    })
                    .isInstanceOf(WriteOperationsNotAllowedException.class)
                    .hasMessageContaining(
                            "No write operations are allowed on this database. The database is in read-only mode on this Neo4j instance.");
        } finally {
            dbms.shutdown();
        }
    }

    private static boolean isReadOnly(GraphDatabaseService db) {
        try (Transaction tx = db.beginTx()) {
            var result = tx.execute("CALL dbms.listConfig(\"server.databases.read_only\") YIELD value");
            var readOnlyDatabases = (String) result.next().get("value");
            return !readOnlyDatabases.isEmpty() && readOnlyDatabases.contains(db.databaseName());
        }
    }

    private static class TestDatabaseManagementServiceBuilderWithCustomPageCacheNativeAccess
            extends TestDatabaseManagementServiceBuilder {
        private final FileSystemAbstraction fs;

        TestDatabaseManagementServiceBuilderWithCustomPageCacheNativeAccess(
                Path homeDirectory, FileSystemAbstraction fs) {
            super(homeDirectory);
            this.fs = fs;
        }

        @Override
        protected DatabaseManagementService newDatabaseManagementService(
                Config config, ExternalDependencies dependencies) {
            return new DatabaseManagementServiceFactory(DbmsInfo.COMMUNITY, CommunityEditionModule::new) {

                @Override
                protected GlobalModule createGlobalModule(
                        Config config, boolean daemonMode, ExternalDependencies dependencies) {
                    return new GlobalModule(config, dbmsInfo, daemonMode, dependencies) {
                        @Override
                        protected PageCache createPageCache(
                                FileSystemAbstraction fileSystem1,
                                Config config1,
                                LogService logging,
                                Tracers tracers,
                                JobScheduler jobScheduler,
                                SystemNanoClock clock1,
                                MemoryPools memoryPools) {
                            long pageCacheMaxMemory = ByteUnit.mebiBytes(20);
                            var memoryPool = memoryPools.pool(PAGE_CACHE, pageCacheMaxMemory, false, null);
                            var memoryTracker = memoryPool.getPoolMemoryTracker();
                            var swapperFactory =
                                    new SingleFilePageSwapperFactory(fs, tracers.getPageCacheTracer(), memoryTracker) {
                                        @Override
                                        protected NativeAccess nativeAccess() {
                                            return new FileLimitingNativeAccess();
                                        }
                                    };

                            MemoryAllocator memoryAllocator = createAllocator(pageCacheMaxMemory, memoryTracker);
                            var bufferFactory = new ConfigurableIOBufferFactory(config1, memoryTracker);
                            MuninnPageCache.Configuration configuration = MuninnPageCache.config(memoryAllocator)
                                    .memoryTracker(memoryTracker)
                                    .bufferFactory(bufferFactory)
                                    .reservedPageBytes(PageCache.RESERVED_BYTES)
                                    .preallocateStoreFiles(config1.get(preallocate_store_files))
                                    .clock(clock1)
                                    .pageCacheTracer(tracers.getPageCacheTracer());
                            return new MuninnPageCache(swapperFactory, jobScheduler, configuration);
                        }

                        @Override
                        protected FileSystemAbstraction createFileSystemAbstraction() {
                            return fs;
                        }
                    };
                }
            }.build(config, daemonMode, dependencies);
        }
    }

    private static class FileLimitingNativeAccess implements NativeAccess {

        private final NativeCallResult OUT_OF_DISK = new NativeCallResult(123, "Out of disc");

        @Override
        public boolean isAvailable() {
            return true;
        }

        @Override
        public NativeCallResult tryEvictFromCache(int fd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NativeCallResult tryAdviseSequentialAccess(int fd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NativeCallResult tryAdviseToKeepInCache(int fd) {
            throw new UnsupportedOperationException();
        }

        @Override
        public NativeCallResult tryPreallocateSpace(int fd, long bytes) {
            if (bytes > FILE_SIZE_LIMIT) {
                return OUT_OF_DISK;
            }
            return NativeCallResult.SUCCESS;
        }

        @Override
        public ErrorTranslator errorTranslator() {
            return callResult -> callResult == OUT_OF_DISK;
        }

        @Override
        public String describe() {
            return "Test native access";
        }
    }
}
