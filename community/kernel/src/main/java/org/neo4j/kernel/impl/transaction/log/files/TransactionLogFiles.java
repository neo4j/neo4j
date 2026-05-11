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
package org.neo4j.kernel.impl.transaction.log.files;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Path;
import org.neo4j.internal.helpers.ArrayUtil;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointLogFile;
import org.neo4j.kernel.lifecycle.LifeSupport;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.LogMetadataProviderImpl;

/**
 * Used to figure out what logical log file to open when the database
 * starts up.
 */
public class TransactionLogFiles extends LifecycleAdapter implements LogFiles {
    public static final DirectoryStream.Filter<Path> DEFAULT_FILENAME_FILTER =
            TransactionLogFilesHelper.DEFAULT_FILENAME_FILTER;

    private final CheckpointFile checkpointLogFile;
    private final TransactionLogFilesOverrides overrides;
    private final TransactionLogFile logFile;
    private final Path logsDirectory;
    private volatile LogMetadataProviderImpl logMetadataProvider;
    private LifeSupport logFilesLife;

    TransactionLogFiles(
            Path logsDirectory, TransactionLogFilesContext context, TransactionLogFilesOverrides overrides) {
        this.logsDirectory = logsDirectory;
        this.logFile = new TransactionLogFile(this, context);
        this.checkpointLogFile =
                new CheckpointLogFile(this, context, overrides.externalLogTail(), overrides.tailReadingMaxPosition());
        this.overrides = overrides;
        if (!overrides.noInit()) {
            LogTailMetadata tailMetadata = checkpointLogFile.getTailMetadata();
            logMetadataProvider = new LogMetadataProviderImpl(tailMetadata, context.recoveryOutcome());
            TransactionLogFilesProviders transactionLogFilesProviders =
                    new TransactionLogFilesProviders(logMetadataProvider, this.overrides);
            checkpointLogFile.initialize(transactionLogFilesProviders);
            logFile.initialize(transactionLogFilesProviders);
        }
    }

    @Override
    public LogMetadataProvider logMetadataProvider() {
        if (logMetadataProvider == null) {
            throw new IllegalStateException("Current version of log files can't perform any "
                    + "operation that require availability of LogMetadataProvider. Please "
                    + "build full version of log files "
                    + "to be able to use them.");
        }
        return logMetadataProvider;
    }

    @Override
    public void init() throws IOException {
        if (overrides.noInit() || overrides.notStartable()) {
            throw new RuntimeException("Current version of log files can't be started. Please "
                    + "build full version of log files to be able to start them.");
        }

        // life support is not restartable so we need to create a new one each time
        logFilesLife = new LifeSupport();
        logFilesLife.add(logFile);
        logFilesLife.add(checkpointLogFile);
        logFilesLife.init();
    }

    @Override
    public void start() throws IOException {
        logFilesLife.start();
    }

    @Override
    public void stop() throws IOException {
        logFilesLife.stop();
    }

    @Override
    public void shutdown() {
        logFilesLife.shutdown();
    }

    @Override
    public Path[] logFiles() throws IOException {
        return ArrayUtil.concat(logFile.getMatchedFiles(), checkpointLogFile.getMatchedFiles());
    }

    @Override
    public boolean isLogFile(Path path) {
        try {
            return DEFAULT_FILENAME_FILTER.accept(path);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public LogTailMetadata getTailMetadata() {
        return checkpointLogFile.getTailMetadata();
    }

    @Override
    public Path logFilesDirectory() {
        return logsDirectory;
    }

    @Override
    public LogFile getLogFile() {
        return logFile;
    }

    @Override
    public CheckpointFile getCheckpointFile() {
        return checkpointLogFile;
    }
}
