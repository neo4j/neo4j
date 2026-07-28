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
import static org.neo4j.internal.helpers.Numbers.safeCastLongToInt;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.BIGGEST_HEADER;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
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
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractVersionAwareLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.TailUtils;
import org.neo4j.kernel.impl.transaction.log.entry.UnsupportedLogVersionException;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.LogRangeInfo;
import org.neo4j.kernel.impl.transaction.log.files.LogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.RangeLogVersionVisitor;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesProviders;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.recovery.LogTailScannerMonitor;
import org.neo4j.logging.InternalLog;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

public class CheckpointLogFile extends LifecycleAdapter implements CheckpointFile {
    private volatile DetachedCheckpointAppender checkpointAppender;
    private final DetachedLogTailScanner logTailScanner;
    private final SequentialFileNameHelper fileHelper;
    private final TransactionLogChannelAllocator channelAllocator;
    private final LogFiles logFiles;
    private final TransactionLogFilesContext context;
    private final InternalLog log;
    private final long rotationsSize;
    private final LogTailScannerMonitor monitor;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private LogVersionRepository logVersionRepository;
    private volatile boolean started;
    private volatile TransactionLogFilesProviders transactionLogFilesProviders;

    public CheckpointLogFile(
            LogFiles logFiles,
            TransactionLogFilesContext context,
            LogTailMetadata externalLogTail,
            LogPosition tailReadingMaxPosition) {
        this.context = context;
        this.logFiles = logFiles;
        this.rotationsSize = context.checkpointRotationThreshold();
        this.fileHelper = TransactionLogFilesHelper.forCheckpoints(logFiles.logFilesDirectory());
        this.channelAllocator = new CheckpointLogChannelAllocator(context, fileHelper);
        this.monitor = context.monitors().newMonitor(LogTailScannerMonitor.class);
        this.logTailScanner =
                new DetachedLogTailScanner(logFiles, context, this, monitor, externalLogTail, tailReadingMaxPosition);
        this.log = context.logProvider().getLog(getClass());
        this.binarySupportedKernelVersions = context.binarySupportedKernelVersions();
    }

    @Override
    public void initialize(TransactionLogFilesProviders transactionLogFilesProviders) {
        Preconditions.checkArgument(!started, "Initialize must be called before start");
        this.transactionLogFilesProviders = transactionLogFilesProviders;
    }

    @Override
    public void start() throws Exception {
        var checkpointRotation = checkpointLogRotation(
                this,
                logFiles.getLogFile(),
                context.clock(),
                context.databaseHealth(),
                context.monitors().newMonitor(LogRotationMonitor.class),
                transactionLogFilesProviders.getKernelVersionProvider());
        checkpointAppender = new DetachedCheckpointAppender(
                logFiles,
                channelAllocator,
                context,
                this,
                checkpointRotation,
                logTailScanner,
                binarySupportedKernelVersions,
                transactionLogFilesProviders);
        checkpointAppender.start();
        logVersionRepository = transactionLogFilesProviders.getLogVersionRepository();
        started = true;
    }

    @Override
    public void shutdown() throws Exception {
        if (checkpointAppender != null) {
            checkpointAppender.shutdown();
        }
        started = false;
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint() throws IOException {
        return findLatestCheckpoint(log);
    }

    @Override
    public Optional<CheckpointInfo> findLatestCheckpoint(InternalLog log) throws IOException {
        LogRangeInfo logRangeInfo = getLogRangeInfo();
        long highestVersion = logRangeInfo.highestVersion();
        if (highestVersion < 0) {
            return Optional.empty();
        }

        long lowestVersion = logRangeInfo.lowestVersion();
        long currentVersion = highestVersion;

        var checkpointReader =
                new VersionAwareLogEntryReader(NO_COMMANDS, binarySupportedKernelVersions, context.memoryTracker());
        while (currentVersion >= lowestVersion) {
            CheckpointEntryInfo checkpointEntry = null;
            Path currentCheckpointFile = getLogFileForVersion(currentVersion);
            FileSystemAbstraction fileSystem = context.fileSystem();
            var header = readLogHeader(fileSystem, currentCheckpointFile, false, context.memoryTracker());
            if (header != null) {
                final var readerBridge = ReaderLogVersionBridge.forFile(this);
                try (var reader =
                                ReadAheadUtils.newChannel(this, currentVersion, readerBridge, context.memoryTracker());
                        var logEntryCursor = new LogEntryCursor(checkpointReader, reader)) {
                    log.info("Scanning log file with version %d for checkpoint entries", currentVersion);
                    try {
                        LogPosition lastCheckpointLocation = reader.getCurrentLogPosition();
                        while (logEntryCursor.next()) {
                            var checkpoint = (AbstractVersionAwareLogEntry) logEntryCursor.get();
                            checkpointEntry = new CheckpointEntryInfo(
                                    checkpoint, lastCheckpointLocation, reader.getCurrentLogPosition());
                            lastCheckpointLocation = checkpointEntry.channelPositionAfterCheckpoint;
                        }
                        if (checkpointEntry != null) {
                            return Optional.of(createCheckpointInfo(checkpointEntry, reader));
                        }
                    } catch (Error | ClosedByInterruptException | UnsupportedLogVersionException e) {
                        throw e;
                    } catch (Throwable t) {
                        monitor.corruptedCheckpointFile(currentVersion, t);
                        if (checkpointEntry != null) {
                            return Optional.of(createCheckpointInfo(checkpointEntry, reader));
                        }
                    }
                }
            } else {
                if (!context.readOnly()) {
                    // So since file does not have readable header by our contract this means that it's or empty or
                    // corrupted.
                    // In cases when file is empty or was not able to write at least header we should not request users
                    // to
                    // do recovery workflow and try to resolve it
                    // on our own. Here we need to make sure that we are the last file in a sequence and that there are
                    // not
                    // data after non-readable header to be sure
                    log.info(
                            "Checkpoint log file `%s` does not have any readable header available.",
                            currentCheckpointFile);

                    // we should make sure that we are not running yet
                    if (started) {
                        throw new IllegalStateException(
                                "When checkpoint file was already started we should never be in the state to remove partially created files. But file: "
                                        + currentCheckpointFile + " claims to have no header.");
                    }
                    // we need to make sure that we are the last one
                    verifyLastFile(fileSystem, currentVersion, currentCheckpointFile);
                    verifyNoMoreDataAvailableInFile(fileSystem, currentCheckpointFile);

                    log.info(
                            "Checkpoint log file `%s` is present but does not contain any data. Cleaning up.",
                            currentCheckpointFile);

                    // if all checks are good we can remove empty file
                    fileSystem.deleteFile(currentCheckpointFile);
                }
            }
            currentVersion--;
        }
        return Optional.empty();
    }

    private void verifyNoMoreDataAvailableInFile(FileSystemAbstraction fileSystem, Path currentCheckpointFile)
            throws IOException {
        try (StoreChannel channel = fileSystem.read(currentCheckpointFile)) {
            TailUtils.checkNonZerosAfterOffset(
                    BIGGEST_HEADER,
                    channel,
                    context.memoryTracker(),
                    safeCastLongToInt(ByteUnit.kibiBytes(10)),
                    false,
                    (offset, data) -> {
                        throw new IllegalStateException(
                                "Checkpoint file: `" + currentCheckpointFile
                                        + "` has unreadable header but looks like it also contains some checkpoint data. Restore from the backup is required.");
                    });
        }
    }

    private void verifyLastFile(FileSystemAbstraction fileSystem, long currentVersion, Path currentCheckpointFile) {
        if (fileSystem.fileExists(getLogFileForVersion(currentVersion + 1))) {
            throw new IllegalStateException(
                    "Not the last checkpoint file in a sequence contains corrupted header. File with corrupted header : "
                            + currentCheckpointFile);
        }
    }

    private CheckpointInfo createCheckpointInfo(CheckpointEntryInfo checkpointEntry, ReadableLogChannel reader)
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
        LogRangeInfo logRangeInfo = getLogRangeInfo();
        if (logRangeInfo.highestVersion() < 0) {
            return emptyList();
        }

        long currentVersion = logRangeInfo.lowestVersion();
        var checkpointReader =
                new VersionAwareLogEntryReader(NO_COMMANDS, binarySupportedKernelVersions, context.memoryTracker());
        var checkpoints = new ArrayList<CheckpointInfo>();

        final var readerBridge = ReaderLogVersionBridge.forFile(this);
        try (var reader = ReadAheadUtils.newChannel(this, currentVersion, readerBridge, context.memoryTracker());
                var logEntryCursor = new LogEntryCursor(checkpointReader, reader)) {
            log.info("Start scanning log files from version %d for checkpoint entries", currentVersion);
            readCheckpoints(reader, logEntryCursor, checkpoints);
        }

        return checkpoints;
    }

    private void readCheckpoints(
            ReadableLogChannel reader, LogEntryCursor logEntryCursor, List<CheckpointInfo> checkpoints)
            throws IOException {
        LogEntry checkpoint;
        var lastCheckpointLocation = reader.getCurrentLogPosition();
        var lastLocation = lastCheckpointLocation;
        while (logEntryCursor.next()) {
            lastCheckpointLocation = lastLocation;
            checkpoint = logEntryCursor.get();
            lastLocation = reader.getCurrentLogPosition();
            checkpoints.add(ofLogEntry(
                    checkpoint, lastCheckpointLocation, lastLocation, lastLocation, context, logFiles.getLogFile()));
        }
    }

    @Override
    public CheckpointAppender getCheckpointAppender() {
        assert started : "CheckpointAppender only available after start";
        return checkpointAppender;
    }

    @Override
    public LogTailMetadata getTailMetadata() {
        return logTailScanner.getTailMetadata();
    }

    @Override
    public Path getCurrentFile() throws IOException {
        return fileHelper.getFileForVersion(getCurrentLogVersion());
    }

    @Override
    public Path getLogFileForVersion(long logVersion) {
        return fileHelper.getFileForVersion(logVersion);
    }

    @Override
    public Path[] getMatchedFiles() throws IOException {
        return fileHelper.getFiles(context.fileSystem());
    }

    @Override
    public long getCurrentLogVersion() {
        if (logVersionRepository != null) {
            return logVersionRepository.getCheckpointLogVersion();
        }

        return getLogRangeInfo().highestVersion();
    }

    @Override
    public long getLogVersion(Path checkpointLogFile) {
        return SequentialFileNameHelper.getVersion(checkpointLogFile);
    }

    @Override
    public boolean rotationNeeded() {
        long position = checkpointAppender.getCurrentPosition();
        return position >= rotationsSize;
    }

    @Override
    public synchronized RotationInfo rotate() throws IOException {
        return checkpointAppender.rotate();
    }

    @Override
    public RotationInfo rotate(
            KernelVersion kernelVersion, long lastAppendIndex, int checksum, long lastTerm, LogFormat logFormat)
            throws IOException {
        // Checkpoint log handles checksums and append indexes internally, this one should not ever be needed for
        // checkpoint log file.
        throw new UnsupportedOperationException("Checkpoint log does not support this type of rotation");
    }

    @Override
    public RotationInfo rotate(KernelVersion kernelVersion, long lastAppendIndex, int checksum) throws IOException {
        // Checkpoint files are written with a fixed lastAppendIndex in the file header, so this is ignored
        return checkpointAppender.rotate(kernelVersion, checksum);
    }

    @Override
    public RotationInfo rotate(KernelVersion kernelVersion) throws IOException {
        return checkpointAppender.rotate(kernelVersion);
    }

    @Override
    public long rotationSize() {
        return rotationsSize;
    }

    @Override
    public LogRangeInfo getLogRangeInfo() {
        return visitLogFiles(new RangeLogVersionVisitor()).getLogRangeInfo();
    }

    @Override
    public LogHeader extractHeader(long version) throws IOException {
        return readLogHeader(context.fileSystem(), getLogFileForVersion(version), true, context.memoryTracker());
    }

    @Override
    public ReadableLogChannel getReader(LogPosition position, LogVersionBridge logVersionBridge) throws IOException {
        PhysicalLogVersionedStoreChannel logChannel = openForVersion(position.getLogVersion());
        logChannel.position(position.getByteOffset());
        final var logHeader = extractHeader(logChannel.getLogVersion());
        return ReadAheadUtils.newChannel(logChannel, logVersionBridge, logHeader, context.memoryTracker());
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion(long checkpointLogVersion) throws IOException {
        return channelAllocator.openLogChannel(checkpointLogVersion);
    }

    @VisibleForTesting
    public DetachedLogTailScanner getLogTailScanner() {
        return logTailScanner;
    }

    private <V extends LogVersionVisitor> V visitLogFiles(V visitor) {
        try {
            for (Path file : fileHelper.getFiles(context.fileSystem())) {
                visitor.visit(file, SequentialFileNameHelper.getVersion(file));
            }
            return visitor;
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    private record CheckpointEntryInfo(
            LogEntry checkpoint, LogPosition checkpointEntryPosition, LogPosition channelPositionAfterCheckpoint) {}
}
