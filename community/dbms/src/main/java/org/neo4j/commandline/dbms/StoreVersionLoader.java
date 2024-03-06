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
package org.neo4j.commandline.dbms;

import java.io.IOException;
import java.io.UncheckedIOException;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseSettings;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.pagecache.ConfiguringPageCacheFactory;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.recovery.LogTailExtractor;
import org.neo4j.logging.NullLog;
import org.neo4j.logging.internal.NullLogService;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryPools;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.storageengine.api.StorageEngineFactory;
import org.neo4j.storageengine.api.StoreVersionCheck;
import org.neo4j.storageengine.api.StoreVersionIdentifier;
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
     * Reads store version from the store files and also performs downgrade check.
     * @return the {@link Result} of the store version if the format can be read.
     */
    public Result loadStoreVersionAndCheckDowngrade(DatabaseLayout layout) {
        StorageEngineFactory sef = StorageEngineFactory.selectStorageEngine(fs, layout, config);
        StoreVersionCheck versionCheck =
                sef.versionCheck(fs, layout, config, pageCache, NullLogService.getInstance(), contextFactory);
        try (CursorContext cursorContext = contextFactory.create("Store version loader")) {
            StoreVersionCheck.UpgradeCheckResult checkResult =
                    versionCheck.getAndCheckUpgradeTargetVersion(cursorContext);
            if (checkResult.outcome() == StoreVersionCheck.UpgradeOutcome.STORE_VERSION_RETRIEVAL_FAILURE) {
                throw new IllegalStateException(
                        "Can not read store version of database " + layout.getDatabaseName(), checkResult.cause());
            }

            checkDowngrade(sef, layout);

            return new Result(
                    checkResult.outcome() == StoreVersionCheck.UpgradeOutcome.UNSUPPORTED_TARGET_VERSION,
                    checkResult.versionToUpgradeFrom(),
                    checkResult.versionToUpgradeTo(),
                    versionCheck.getIntroductionVersionFromVersion(checkResult.versionToUpgradeFrom()));
        }
    }

    private void checkDowngrade(StorageEngineFactory engineFactory, DatabaseLayout layout) {
        // Let's check if we can read TX logs metadata tail.
        // We are not interested in the metadata, just check if it blows up or not.
        try {
            new LogTailExtractor(fs, config, engineFactory, DatabaseTracers.EMPTY)
                    // We don't really care about the situation when there are no TX logs,
                    // so the latest kernel version as a fallback is fine. We just don't want this check to blow up when
                    // there are no TX logs.
                    .getTailMetadata(layout, EmptyMemoryTracker.INSTANCE, () -> KernelVersion.getLatestVersion(config));
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (RuntimeException e) {
            if (Exceptions.contains(e, ex -> ex instanceof UnsupportedLogVersionException)) {
                // Why is it safe to assume that unknown TX log format version is a downgrade attempt?
                // We were able to read and understand the current store format (We did not get
                // STORE_VERSION_RETRIEVAL_FAILURE), which means that the store is not an old version
                // beyond the currently supported range, so if we cannot read TX tail metadata it must be a newer
                // version.

                // The messages in UnsupportedLogVersionException  are quite informative, so let's fish it out as
                // a cause.
                Throwable cause = e;
                while (!(cause instanceof UnsupportedLogVersionException)) {
                    cause = cause.getCause();
                }

                throw new IllegalStateException(
                        "The loaded database '" + layout.getDatabaseName()
                                + "' is of a newer version than the current binaries. " + "Downgrade is not supported.",
                        cause);
            }

            throw e;
        }
    }

    public static class Result {
        public final StoreVersionIdentifier currentFormat;
        public final StoreVersionIdentifier latestFormat;
        public final boolean migrationNeeded;
        public final String currentFormatIntroductionVersion;

        private Result(
                boolean migrationNeeded,
                StoreVersionIdentifier currentFormat,
                StoreVersionIdentifier latestFormat,
                String currentFormatIntroductionVersion) {
            this.currentFormat = currentFormat;
            this.latestFormat = latestFormat;
            this.migrationNeeded = migrationNeeded;
            this.currentFormatIntroductionVersion = currentFormatIntroductionVersion;
        }
    }
}
