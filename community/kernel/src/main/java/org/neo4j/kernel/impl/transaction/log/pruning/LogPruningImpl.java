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
package org.neo4j.kernel.impl.transaction.log.pruning;

import static org.apache.commons.lang3.ArrayUtils.isNotEmpty;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_keep_threshold;
import static org.neo4j.configuration.GraphDatabaseSettings.keep_logical_logs;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.time.Clock;
import java.util.concurrent.locks.Lock;
import java.util.function.LongConsumer;
import org.neo4j.configuration.Config;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;

/**
 * This class listens for rotations and does log pruning.
 */
public class LogPruningImpl implements LogPruning {
    private final Lock pruneLock;
    private final FileSystemAbstraction fs;
    private final LogFiles logFiles;
    private final InternalLog log;
    private final LogPruneStrategyFactory strategyFactory;
    private final Clock clock;
    private final InternalLogProvider logProvider;
    private final int checkpointFilesToKeep;
    private volatile LogPruneStrategy pruneStrategy;

    public LogPruningImpl(
            FileSystemAbstraction fs,
            LogFiles logFiles,
            InternalLogProvider logProvider,
            LogPruneStrategyFactory strategyFactory,
            Clock clock,
            Config config,
            Lock pruneLock) {
        this.fs = fs;
        this.logFiles = logFiles;
        this.logProvider = logProvider;
        this.log = logProvider.getLog(getClass());
        this.strategyFactory = strategyFactory;
        this.clock = clock;
        this.pruneLock = pruneLock;
        this.pruneStrategy = strategyFactory.strategyFromConfigValue(
                fs, logFiles, logProvider, clock, config.get(keep_logical_logs));
        this.checkpointFilesToKeep = config.get(checkpoint_logical_log_keep_threshold);

        // Register listener for updates
        config.addListener(keep_logical_logs, (prev, update) -> updateConfiguration(update));
    }

    private void updateConfiguration(String pruningConf) {
        LogPruneStrategy strategy =
                strategyFactory.strategyFromConfigValue(fs, logFiles, logProvider, clock, pruningConf);
        this.pruneStrategy = strategy;
        log.info("Retention policy updated to '" + strategy
                + "', which will take effect next time a checkpoint completes.");
    }

    @Override
    public void pruneLogs(long upToVersion) throws IOException {
        pruneLock.lock();
        try {
            LogFile logFile = logFiles.getLogFile();
            LogPruneStrategy strategy = this.pruneStrategy;

            CountingDeleter deleter = new CountingDeleter(logFile);
            LogPruneStrategy.VersionRange versionsToDelete = strategy.findLogVersionsToDelete(upToVersion);
            // make sure to do the arithmetics only for non-empty ranges
            if (versionsToDelete.isNotEmpty()) {
                logFile.terminateExternalReaders(versionsToDelete.toExclusive() - 1);
                versionsToDelete.forEachOrdered(deleter);
            }
            log.info(deleter.describeResult(strategy));

            cleanupCheckpointLogFiles();
        } finally {
            pruneLock.unlock();
        }
    }

    private void cleanupCheckpointLogFiles() throws IOException {
        var checkpointFile = logFiles.getCheckpointFile();
        var checkpointFiles = checkpointFile.getDetachedCheckpointFiles();
        if (isNotEmpty(checkpointFiles) && checkpointFiles.length > checkpointFilesToKeep) {
            long highestVersionToRemove = checkpointFile.getCurrentDetachedLogVersion() - checkpointFilesToKeep;
            int filesDeleted = 0;
            for (Path file : checkpointFiles) {
                if (checkpointFile.getDetachedCheckpointLogFileVersion(file) <= highestVersionToRemove) {
                    fs.deleteFile(file);
                    filesDeleted++;
                }
            }
            log.info("Pruned " + filesDeleted + " checkpoint log files. Lowest preserved version: "
                    + (highestVersionToRemove + 1));
        }
    }

    @Override
    public boolean mightHaveLogsToPrune(long upToVersion) {
        LogPruneStrategy.VersionRange versionsToDelete = pruneStrategy.findLogVersionsToDelete(upToVersion);
        return versionsToDelete.isNotEmpty();
    }

    @Override
    public String describeCurrentStrategy() {
        return pruneStrategy.toString();
    }

    private static class CountingDeleter implements LongConsumer {
        private static final int NO_VERSION = -1;
        private final LogFile logFile;
        private long fromVersion;
        private long toVersion;

        private CountingDeleter(LogFile logFile) {
            this.logFile = logFile;
            fromVersion = NO_VERSION;
            toVersion = NO_VERSION;
        }

        @Override
        public void accept(long version) {
            fromVersion = fromVersion == NO_VERSION ? version : Math.min(fromVersion, version);
            toVersion = toVersion == NO_VERSION ? version : Math.max(toVersion, version);
            try {
                logFile.delete(version);
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }

        String describeResult(LogPruneStrategy strategy) {
            String pruned = fromVersion == NO_VERSION
                    ? "No log version pruned"
                    : fromVersion == toVersion
                            ? "Pruned log version " + fromVersion
                            : "Pruned log versions " + fromVersion + " through " + toVersion;
            return pruned + ". The strategy used was '" + strategy + "'. ";
        }
    }
}
