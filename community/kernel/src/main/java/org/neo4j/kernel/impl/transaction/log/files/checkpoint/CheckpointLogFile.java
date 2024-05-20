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
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.BIGGEST_HEADER;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesHelper.CHECKPOINT_FILE_PREFIX;
import static org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointInfoFactory.ofLogEntry;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.checkpointLogRotation;
import static org.neo4j.storageengine.AppendIndexProvider.BASE_APPEND_INDEX;
import static org.neo4j.storageengine.api.CommandReaderFactory.NO_COMMANDS;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteOrder;
import java.nio.channels.ClosedByInterruptException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.CheckpointInfo;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.checkpoint.DetachedCheckpointAppender;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractVersionAwareLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
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
    private volatile boolean started;

    public CheckpointLogFile(LogFiles logFiles, TransactionLogFilesContext context) {
        this.context = context;
        this.logFiles = logFiles;
        this.rotationsSize = context.getCheckpointRotationThreshold();
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
        started = true;
    }

    @Override
    public void shutdown() throws Exception {
        checkpointAppender.shutdown();
        started = false;
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
            Path currentCheckpointFile = getDetachedCheckpointFileForVersion(currentVersion);
            FileSystemAbstraction fileSystem = context.getFileSystem();
            var header = readLogHeader(fileSystem, currentCheckpointFile, false, context.getMemoryTracker());
            if (header != null) {
                try (var channel = channelAllocator.openLogChannel(currentVersion);
                        var reader =
                                ReadAheadUtils.newChannel(channel, logHeader(channel), context.getMemoryTracker());
                        var logEntryCursor = new LogEntryCursor(checkpointReader, reader)) {
                    log.info("Scanning log file with version %d for checkpoint entries", currentVersion);
                    try {
                        lastCheckpointLocation = reader.getCurrentLogPosition();
                        while (logEntryCursor.next()) {
                            var checkpoint = (AbstractVersionAwareLogEntry) logEntryCursor.get();
                            lastObservedKernelVersion =
                                    checkpoint.kernelVersion().version();
                            checkpointEntry = new CheckpointEntryInfo(
                                    checkpoint, lastCheckpointLocation, reader.getCurrentLogPosition());
                            lastCheckpointLocation = checkpointEntry.channelPositionAfterCheckpoint;
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
                }
            } else {
                if (!context.isReadOnly()) {
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
                    BASE_APPEND_INDEX,
                    "Corrupt checkpoint file"));
        }
        return Optional.empty();
    }

    private void verifyNoMoreDataAvailableInFile(FileSystemAbstraction fileSystem, Path currentCheckpointFile)
            throws IOException {
        try (StoreChannel channel = fileSystem.read(currentCheckpointFile)) {
            try (var scopedBuffer = new HeapScopedBuffer(
                    (int) Math.min(fileSystem.getFileSize(currentCheckpointFile), ByteUnit.kibiBytes(10)),
                    ByteOrder.LITTLE_ENDIAN,
                    context.getMemoryTracker())) {
                var buffer = scopedBuffer.getBuffer();
                channel.readAll(buffer);
                buffer.flip();
                if (buffer.capacity() > BIGGEST_HEADER) {
                    buffer.position(BIGGEST_HEADER);
                    while (buffer.hasRemaining()) {
                        if (buffer.get() != 0) {
                            throw new IllegalStateException(
                                    "Checkpoint file: `" + currentCheckpointFile
                                            + "` has unreadable header but looks like it also contains some checkpoint data. Restore from the backup is required.");
                        }
                    }
                }
            }
        }
    }

    private void verifyLastFile(FileSystemAbstraction fileSystem, long currentVersion, Path currentCheckpointFile) {
        if (fileSystem.fileExists(getDetachedCheckpointFileForVersion(currentVersion + 1))) {
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
        var versionVisitor = new RangeLogVersionVisitor();
        fileHelper.accept(versionVisitor);
        if (versionVisitor.getHighestVersion() < 0) {
            return emptyList();
        }

        long currentVersion = versionVisitor.getLowestVersion();
        var checkpointReader = new VersionAwareLogEntryReader(NO_COMMANDS, true, binarySupportedKernelVersions);
        var checkpoints = new ArrayList<CheckpointInfo>();

        final var readerBridge = ReaderLogVersionBridge.forFile(this);
        try (var channel = channelAllocator.openLogChannel(currentVersion);
                var reader = ReadAheadUtils.newChannel(
                        channel, readerBridge, logHeader(channel), context.getMemoryTracker());
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

    private LogHeader logHeader(PhysicalLogVersionedStoreChannel channel) throws IOException {
        return channelAllocator.readLogHeaderForVersion(channel.getLogVersion());
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
    public Path rotate(KernelVersion kernelVersion, long lastTransactionId, long lastAppendIndex, int checksum)
            throws IOException {
        return checkpointAppender.rotate(kernelVersion, lastTransactionId, lastAppendIndex);
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
