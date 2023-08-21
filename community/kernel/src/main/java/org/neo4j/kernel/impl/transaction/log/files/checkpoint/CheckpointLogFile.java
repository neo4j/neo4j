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
package org.neo4j.kernel.impl.transaction.log.files.checkpoint;

import static java.util.Collections.emptyList;
import static org.neo4j.configuration.GraphDatabaseInternalSettings.checkpoint_logical_log_rotation_threshold;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfoFactory.ofLogEntry;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.checkpointLogRotation;
import static org.neo4j.storageengine.api.CommandReaderFactory.NO_COMMANDS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadLogChannel;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractVersionAwareLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.LogTailScannerMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.util.VisibleForTesting;

public class CheckpointLogFile extends LifecycleAdapter implements CheckpointFile {
    private final DetachedCheckpointAppender checkpointAppender;
    private final DetachedLogTailScanner logTailScanner;
    private final TransactionLogFilesHelper fileHelper;
    private final TransactionLogChannelAllocator channelAllocator;
    private final LogFiles logFiles;
    private final TransactionLogFilesContext context;
    private final InternalLog log;
    private final long rotationsSize;
    private final LogTailScannerMonitor monitor;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private LogVersionRepository logVersionRepository;

    public CheckpointLogFile(LogFiles logFiles, TransactionLogFilesContext context) {
        this.context = context;
        this.logFiles = logFiles;
        this.rotationsSize = context.getConfig().get(checkpoint_logical_log_rotation_threshold);
        this.fileHelper = new TransactionLogFilesHelper(
                context.getFileSystem(), logFiles.logFilesDirectory(), CHECKPOINT_FILE_PREFIX);
        this.channelAllocator = new CheckpointLogChannelAllocator(context, fileHelper);
        this.monitor = context.getMonitors().newMonitor(LogTailScannerMonitor.class);
        this.logTailScanner = new DetachedLogTailScanner(logFiles, context, this, monitor);
        this.log = context.getLogProvider().getLog(getClass());
        var rotationMonitor = context.getMonitors().newMonitor(LogRotationMonitor.class);
        var checkpointRotation = checkpointLogRotation(
                this, logFiles.getLogFile(), context.getClock(), context.getDatabaseHealth(), rotationMonitor);
        this.binarySupportedKernelVersions = context.getBinarySupportedKernelVersions();
        this.checkpointAppender = new DetachedCheckpointAppender(
                logFiles,
                channelAllocator,
                context,
                this,
                checkpointRotation,
                logTailScanner,
                binarySupportedKernelVersions);
    }

    @Override
    public void start() throws Exception {
        checkpointAppender.start();
        logVersionRepository = context.getLogVersionRepositoryProvider().logVersionRepository(logFiles);
    }

    @Override
    public void shutdown() throws Exception {
        checkpointAppender.shutdown();
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint() throws IOException {
        return findLatestCheckpoint(log);
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint(InternalLog log) throws IOException {
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept(versionVisitor);
        long highestVersion = versionVisitor.getHighestVersion();
        if (highestVersion < 0) {
            return Optional.empty();
        }

        byte lastObservedKernelVersion = 0;
        LogPosition lastCheckpointLocation = null;
        long lowestVersion = versionVisitor.getLowestVersion();
        long currentVersion = highestVersion;

        var checkpointReader = new VersionAwareLogEntryReader(NO_COMMANDS, true, binarySupportedKernelVersions);
        while (currentVersion >= lowestVersion) {
            CheckpointEntryInfo checkpointEntry = null;
            try (var channel = channelAllocator.openLogChannel(currentVersion);
                    var reader = new ReadAheadLogChannel(channel, NO_MORE_CHANNELS, context.getMemoryTracker());
                    var logEntryCursor = new LogEntryCursor(checkpointReader, reader)) {
                log.info("Scanning log file with version %d for checkpoint entries", currentVersion);
                try {
                    lastCheckpointLocation = reader.getCurrentLogPosition();
                    while (logEntryCursor.next()) {
                        var checkpoint = (AbstractVersionAwareLogEntry) logEntryCursor.get();
                        lastObservedKernelVersion = checkpoint.kernelVersion().version();
                        checkpointEntry = new CheckpointEntryInfo(
                                checkpoint, lastCheckpointLocation, reader.getCurrentLogPosition());
                        lastCheckpointLocation = reader.getCurrentLogPosition();
                    }
                    if (checkpointEntry != null) {
                        return Optional.of(createCheckpointInfo(checkpointEntry, reader));
                    }
                } catch (Error | ClosedByInterruptException e) {
                    throw e;
                } catch (Throwable t) {
                    if (t instanceof UnsupportedLogVersionException e) {
                        lastObservedKernelVersion = e.getKernelVersion();
                    }
                    monitor.corruptedCheckpointFile(currentVersion, t);
                    if (checkpointEntry != null) {
                        return Optional.of(createCheckpointInfo(checkpointEntry, reader));
                    }
                }
                currentVersion--;
            }
        }
        if (lastObservedKernelVersion != 0) {
            return Optional.of(new CheckpointInfo(
                    LogPosition.UNSPECIFIED,
                    null,
                    lastCheckpointLocation,
                    lastCheckpointLocation,
                    LogPosition.UNSPECIFIED,
                    null,
                    lastObservedKernelVersion,
                    null,
                    "Corrupt checkpoint file"));
        }
        return Optional.empty();
    }

    private CheckpointInfo createCheckpointInfo(CheckpointEntryInfo checkpointEntry, ReadAheadLogChannel reader)
            throws IOException {
        return ofLogEntry(
                checkpointEntry.checkpoint,
                checkpointEntry.checkpointEntryPosition,
                checkpointEntry.channelPositionAfterCheckpoint,
                reader.getCurrentLogPosition(),
                context,
                logFiles.getLogFile());
    }

    @Override
    public List<CheckpointInfo> reachableCheckpoints() throws IOException {
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept(versionVisitor);
        long highestVersion = versionVisitor.getHighestVersion();
        if (highestVersion < 0) {
            return emptyList();
        }

        long currentVersion = versionVisitor.getLowestVersion();

        var checkpointReader = new VersionAwareLogEntryReader(NO_COMMANDS, true, binarySupportedKernelVersions);
        var checkpoints = new ArrayList<CheckpointInfo>();
        while (currentVersion <= highestVersion) {
            try (var channel = channelAllocator.openLogChannel(currentVersion);
                    var reader = new ReadAheadLogChannel(channel, NO_MORE_CHANNELS, context.getMemoryTracker());
                    var logEntryCursor = new LogEntryCursor(checkpointReader, reader)) {
                log.info("Scanning log file with version %d for checkpoint entries", currentVersion);
                LogEntry checkpoint;
                var lastCheckpointLocation = reader.getCurrentLogPosition();
                var lastLocation = lastCheckpointLocation;
                while (logEntryCursor.next()) {
                    lastCheckpointLocation = lastLocation;
                    checkpoint = logEntryCursor.get();
                    lastLocation = reader.getCurrentLogPosition();
                    checkpoints.add(ofLogEntry(
                            checkpoint,
                            lastCheckpointLocation,
                            lastLocation,
                            lastLocation,
                            context,
                            logFiles.getLogFile()));
                }
                currentVersion++;
            }
        }
        return checkpoints;
    }

    @Override
    public List<CheckpointInfo> getReachableDetachedCheckpoints() throws IOException {
        return reachableCheckpoints();
    }

    @Override
    public CheckpointAppender getCheckpointAppender() {
        return checkpointAppender;
    }

    @Override
    public LogTailMetadata getTailMetadata() {
        return logTailScanner.getTailMetadata();
    }

    @Override
    public Path getCurrentFile() throws IOException {
        return fileHelper.getLogFileForVersion(getCurrentDetachedLogVersion());
    }

    @Override
    public Path getDetachedCheckpointFileForVersion(long logVersion) {
        return fileHelper.getLogFileForVersion(logVersion);
    }

    @Override
    public Path[] getDetachedCheckpointFiles() throws IOException {
        return fileHelper.getMatchedFiles();
    }

    @Override
    public long getCurrentDetachedLogVersion() throws IOException {
        if (logVersionRepository != null) {
            return logVersionRepository.getCheckpointLogVersion();
        }
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept(versionVisitor);
        return versionVisitor.getHighestVersion();
    }

    @Override
    public long getDetachedCheckpointLogFileVersion(Path checkpointLogFile) {
        return TransactionLogFilesHelper.getLogVersion(checkpointLogFile);
    }

    @Override
    public boolean rotationNeeded() {
        long position = checkpointAppender.getCurrentPosition();
        return position >= rotationsSize;
    }

    @Override
    public synchronized Path rotate() throws IOException {
        return checkpointAppender.rotate();
    }

    @Override
    public long rotationSize() {
        return rotationsSize;
    }

    @Override
    public long getLowestLogVersion() {
        return visitLogFiles(new RangeLogVersionVisitor()).getLowestVersion();
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion(long checkpointLogVersion) throws IOException {
        return channelAllocator.openLogChannel(checkpointLogVersion);
    }

    @Override
    public long getHighestLogVersion() {
        return visitLogFiles(new RangeLogVersionVisitor()).getHighestVersion();
    }

    @VisibleForTesting
    public DetachedLogTailScanner getLogTailScanner() {
        return logTailScanner;
    }

    private <V extends LogVersionVisitor> V visitLogFiles(V visitor) {
        try {
            for (Path file : fileHelper.getMatchedFiles()) {
                visitor.visit(file, TransactionLogFilesHelper.getLogVersion(file));
            }
            return visitor;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record CheckpointEntryInfo(
            LogEntry checkpoint, LogPosition checkpointEntryPosition, LogPosition channelPositionAfterCheckpoint) {}
}
