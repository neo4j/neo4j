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
package org.neo4j.commandline.dbms;

import java.util.Objects;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.time.Clocks;

public class StoreVersionLoader implements AutoCloseable {
    private final FileSystemAbstraction fs;
    private final Config config;
    private final CursorContextFactory contextFactory;
    private final JobScheduler jobScheduler;
    private final PageCache pageCache;

    public StoreVersionLoader(FileSystemAbstraction fs, Config config, CursorContextFactory contextFactory) {
        this.fs = fs;
        this.config = Config.newBuilder()
                .fromConfig(config)
                .set(GraphDatabaseSettings.pagecache_memory, ByteUnit.mebiBytes(8))
                .build();
        this.contextFactory = contextFactory;
        this.jobScheduler = JobSchedulerFactory.createInitialisedScheduler();
        this.pageCache = new ConfiguringPageCacheFactory(
                        fs,
                        config,
                        PageCacheTracer.NULL,
                        NullLog.getInstance(),
                        jobScheduler,
                        Clocks.nanoClock(),
                        new MemoryPools())
                .getOrCreatePageCache();
    }

    @Override
    public void close() {
        IOUtils.closeAllSilently(pageCache, jobScheduler);
    }

    /**
     * Reads store version from the store files
     * @param layout The la
     * @return the {@link Result} of the store version if the format can not be read.
     */
    public Result loadStoreVersion(DatabaseLayout layout) {
        StorageEngineFactory sef = StorageEngineFactory.selectStorageEngine(fs, layout, pageCache)
                .orElseGet(StorageEngineFactory::defaultStorageEngine);
        StoreVersionCheck versionCheck =
                sef.versionCheck(fs, layout, config, pageCache, NullLogService.getInstance(), contextFactory);

        String storeVersion = versionCheck
                .storeVersion(CursorContext.NULL_CONTEXT)
                .orElseThrow(() -> new IllegalStateException(
                        "Can not read store version of database " + layout.getDatabaseName()));
        return new Result(storeVersion, sef.versionInformation(storeVersion).latestStoreVersion(config));
    }

    public static class Result {
        public final String currentFormatName;
        public final String latestFormatName;
        public final boolean isLatest;

        private Result(String currentFormatName, String latestFormatName) {
            this.currentFormatName = currentFormatName;
            this.latestFormatName = latestFormatName;
            this.isLatest = Objects.equals(currentFormatName, latestFormatName);
        }
    }
}
