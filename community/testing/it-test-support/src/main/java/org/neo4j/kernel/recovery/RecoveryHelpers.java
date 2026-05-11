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
package org.neo4j.kernel.recovery;

import static org.neo4j.kernel.impl.pagecache.ConfigurableStandalonePageCacheFactory.createPageCache;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.Optional;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.layout.DatabaseLayout;
import org.neo4j.io.pagecache.IOController;
import org.neo4j.io.pagecache.PageCache;
import org.neo4j.io.pagecache.tracing.PageCacheTracer;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.database.DatabaseTracers;
import org.neo4j.kernel.impl.scheduler.JobSchedulerFactory;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogFilesBuilder;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.time.Clocks;

public final class RecoveryHelpers {
    private RecoveryHelpers() { // non-constructable
    }

    public static void removeLastCheckpointRecordFromLogFile(
            DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config) throws IOException {
        removeLastCheckpointRecordFromLogFile(dbLayout, fs, config, false);
    }

    public static void removeLastCheckpointRecordFromLogFile(
            DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config, boolean throwOnNoCheckpoint)
            throws IOException {
        LogFiles logFiles = buildLogFiles(dbLayout, fs, config);
        var checkpointFile = logFiles.getCheckpointFile();
        Optional<CheckpointInfo> latestCheckpoint = checkpointFile.findLatestCheckpoint();
        latestCheckpoint.ifPresentOrElse(
                checkpointInfo -> {
                    LogPosition entryPosition = checkpointInfo.checkpointEntryPosition();
                    try (StoreChannel storeChannel =
                            fs.write(checkpointFile.getLogFileForVersion(entryPosition.getLogVersion()))) {
                        storeChannel.truncate(entryPosition.getByteOffset());
                    } catch (IOException e) {
                        throw new UncheckedIOException(e);
                    }
                },
                () -> {
                    if (throwOnNoCheckpoint) {
                        throw new RuntimeException("No checkpoint found");
                    }
                });
    }

    public static void removeLastCheckpointRecordFromLogFile(DatabaseLayout dbLayout, FileSystemAbstraction fs)
            throws IOException {
        removeLastCheckpointRecordFromLogFile(dbLayout, fs, null);
    }

    /**
     * Throws if no checkpoint was found.
     */
    public static void throwingRemoveLastCheckpointRecordFromLogFile(DatabaseLayout dbLayout, FileSystemAbstraction fs)
            throws IOException {
        removeLastCheckpointRecordFromLogFile(dbLayout, fs, null, true);
    }

    public static boolean logsContainCheckpoint(DatabaseLayout dbLayout, FileSystemAbstraction fs) throws IOException {
        return logsContainCheckpoint(dbLayout, fs, null);
    }

    public static boolean logsContainCheckpoint(DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config)
            throws IOException {
        Optional<CheckpointInfo> latestCheckpoint = getLatestCheckpointInfo(dbLayout, fs, config);
        return latestCheckpoint.isPresent();
    }

    public static CheckpointInfo getLatestCheckpoint(DatabaseLayout dbLayout, FileSystemAbstraction fs)
            throws IOException {
        return getLatestCheckpoint(dbLayout, fs, null);
    }

    public static CheckpointInfo getLatestCheckpoint(DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config)
            throws IOException {
        Optional<CheckpointInfo> latestCheckpoint = getLatestCheckpointInfo(dbLayout, fs, config);
        return latestCheckpoint.orElseThrow();
    }

    public static boolean runRecovery(DatabaseLayout layout, FileSystemAbstraction fileSystem, Config config)
            throws IOException {
        try (JobScheduler jobScheduler = JobSchedulerFactory.createInitialisedScheduler(Clocks.nanoClock());
                PageCache pageCache = createPageCache(fileSystem, config, jobScheduler, PageCacheTracer.NULL)) {
            return Recovery.performRecovery(Recovery.contextWithNoLogTail(
                            fileSystem,
                            pageCache,
                            DatabaseTracers.EMPTY,
                            config,
                            layout,
                            EmptyMemoryTracker.INSTANCE,
                            IOController.DISABLED,
                            NullLogProvider.getInstance(),
                            KernelVersionProvider.THROWING_PROVIDER))
                    .recoveryPerformed();
        }
    }

    private static Optional<CheckpointInfo> getLatestCheckpointInfo(
            DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config) throws IOException {
        LogFiles logFiles = buildLogFiles(dbLayout, fs, config);
        var checkpointFile = logFiles.getCheckpointFile();
        return checkpointFile.findLatestCheckpoint();
    }

    private static LogFiles buildLogFiles(DatabaseLayout dbLayout, FileSystemAbstraction fs, Config config)
            throws IOException {
        return LogFilesBuilder.logFilesBasedOnlyBuilder(dbLayout.getTransactionLogsDirectory(), fs)
                .withConfig(config)
                .build();
    }
}
