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

import java.io.IOException;
import java.nio.file.Path;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.pagecache.ConfigurableIOBufferFactory;
import org.neo4j.dbms.api.DatabaseManagementService;
import org.neo4j.graphdb.TransactionFailureException;
import org.neo4j.graphdb.facade.DatabaseManagementServiceFactory;
import org.neo4j.graphdb.facade.ExternalDependencies;
import org.neo4j.graphdb.factory.module.GlobalModule;
import org.neo4j.graphdb.factory.module.edition.CommunityEditionModule;
import org.neo4j.internal.kernel.api.exceptions.TransactionApplyKernelException;
import org.neo4j.internal.nativeimpl.ErrorTranslator;
import org.neo4j.internal.nativeimpl.NativeAccess;
import org.neo4j.internal.nativeimpl.NativeCallResult;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.Neo4jLayout;
import org.neo4j.io.mem.MemoryAllocator;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapperFactory;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.kernel.impl.factory.DbmsInfo;
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

    // The first 32MBs of a store are not pre-allocated.
    // The limit works only for files managed by page cache, so not TX logs, Lucene files and so on.
    private static final long FILE_LIMIT = ByteUnit.mebiBytes(33);

    @Inject
    private FileSystemAbstraction fs;

    @Inject
    private Neo4jLayout neo4jLayout;

    // This test is just documenting the current behaviour
    // and is disabled, because it is painfully slow.
    // Marking the reserved node IDs as free seems to be very slow.
    @Disabled
    @Test
    void outOfDiskDuringNodeStoreGrowCausesTheDatabaseToPanic() {
        var dbms = new TestDatabaseManagementServiceBuilderWithCustomPageCacheNativeAccess(
                        neo4jLayout.homeDirectory(), fs)
                .build();
        try {
            var db = dbms.database(DEFAULT_DATABASE_NAME);
            try (var tx = db.beginTx()) {
                // assuming at least 15 bytes per node will get us over the limit regardless of storage engine and
                // sub-format
                for (int i = 0; i < FILE_LIMIT / 15 + 1; i++) {
                    tx.createNode();
                }

                assertThatThrownBy(tx::commit)
                        .hasRootCauseInstanceOf(IOException.class)
                        .getRootCause()
                        .hasMessageContaining("System is out of disk space for store file ");
            }
            var health = ((GraphDatabaseAPI) db).getDependencyResolver().resolveDependency(DatabaseHealth.class);
            assertThat(health.isHealthy()).isFalse();
            assertThat(health.cause()).isInstanceOf(TransactionApplyKernelException.class);

            assertThatThrownBy(db::beginTx)
                    .isInstanceOf(TransactionFailureException.class)
                    .hasRootCauseInstanceOf(IOException.class)
                    .getRootCause()
                    .hasMessageContaining("System is out of disk space for store file ");
        } finally {
            dbms.shutdown();
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
                protected GlobalModule createGlobalModule(Config config, ExternalDependencies dependencies) {
                    GlobalModule globalModule = new GlobalModule(config, dbmsInfo, dependencies) {
                        @Override
                        protected PageCache createPageCache(
                                FileSystemAbstraction fileSystem,
                                Config config,
                                LogService logging,
                                Tracers tracers,
                                JobScheduler jobScheduler,
                                SystemNanoClock clock,
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
                            var bufferFactory = new ConfigurableIOBufferFactory(config, memoryTracker);
                            MuninnPageCache.Configuration configuration = MuninnPageCache.config(memoryAllocator)
                                    .memoryTracker(memoryTracker)
                                    .bufferFactory(bufferFactory)
                                    .reservedPageBytes(PageCache.RESERVED_BYTES)
                                    .preallocateStoreFiles(config.get(preallocate_store_files))
                                    .clock(clock)
                                    .pageCacheTracer(tracers.getPageCacheTracer());
                            return new MuninnPageCache(swapperFactory, jobScheduler, configuration);
                        }

                        @Override
                        protected FileSystemAbstraction createFileSystemAbstraction() {
                            return fs;
                        }
                    };
                    return globalModule;
                }
            }.build(config, dependencies);
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
            if (bytes > FILE_LIMIT) {
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
