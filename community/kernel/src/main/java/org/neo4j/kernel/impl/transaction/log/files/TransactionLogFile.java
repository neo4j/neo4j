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

import static org.neo4j.kernel.KernelVersionProviders.fixed;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogFormat.writeLogHeader;
import static org.neo4j.kernel.impl.transaction.log.entry.LogHeaderReader.readLogHeader;
import static org.neo4j.kernel.impl.transaction.log.rotation.FileLogRotation.transactionLogRotation;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.util.Preconditions.checkArgument;

import java.io.Flushable;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.LockSupport;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.IntSupplier;
import java.util.function.LongSupplier;
import org.eclipse.collections.api.block.procedure.primitive.LongObjectProcedure;
import org.eclipse.collections.api.map.primitive.LongObjectMap;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.internal.nativeimpl.NativeAccessProvider;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.filename.SequentialFileNameHelper;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogForceEvent;
import org.neo4j.kernel.impl.transaction.log.LogForceEvents;
import org.neo4j.kernel.impl.transaction.log.LogForceWaitEvent;
import org.neo4j.kernel.impl.transaction.log.LogFormatVersionProvider;
import org.neo4j.kernel.impl.transaction.log.LogHeaderCache;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogTermProvider;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.ReaderLogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.StoreChannelNativeAccessor;
import org.neo4j.kernel.impl.transaction.log.TransactionLogWriter;
import org.neo4j.kernel.impl.transaction.log.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryCommit;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryStart;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeReadChannel;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeWriteChannel;
import org.neo4j.kernel.impl.transaction.log.enveloped.InvalidEndOfFileReadException;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.monitoring.DatabaseHealth;
import org.neo4j.storageengine.AppendIndexProvider;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

/**
 * {@link LogFile} backed by one or more files in a {@link FileSystemAbstraction}.
 */
public class TransactionLogFile extends LifecycleAdapter implements LogFile {
    private final AtomicReference<ThreadLink> threadLinkHead = new AtomicReference<>(ThreadLink.END);
    private final Lock forceLock = new ReentrantLock();
    private final AtomicLong rotateAtSize;
    private final SequentialFileNameHelper fileHelper;
    private final TransactionLogFilesContext context;
    private final LogVersionBridge readerLogVersionBridge;
    private final MemoryTracker memoryTracker;
    private final TransactionLogChannelAllocator channelAllocator;
    private final DatabaseHealth databaseHealth;
    private volatile LogRotation logRotation;
    private final LogHeaderCache logHeaderCache;
    private final FileSystemAbstraction fileSystem;
    private final ConcurrentMap<Long, List<StoreChannel>> externalFileReaders = new ConcurrentHashMap<>();
    private final LogFileVersionTracker versionTracker;
    private final InternalLog logger;
    private final LogRotationMonitor rotationMonitor;
    private volatile PhysicalLogVersionedStoreChannel channel;
    private PhysicalFlushableLogPositionAwareChannel writer;
    private LogVersionRepository logVersionRepository;
    private volatile TransactionLogFilesProviders transactionLogFilesProviders;
    private TransactionLogWriter transactionLogWriter;
    private volatile boolean initCalled;

    TransactionLogFile(LogFiles logFiles, TransactionLogFilesContext context) {
        this.context = context;
        this.rotateAtSize = context.rotationThreshold();
        this.fileSystem = context.fileSystem();
        this.databaseHealth = context.databaseHealth();
        this.versionTracker = context.versionTracker();
        this.fileHelper = TransactionLogFilesHelper.forTransactions(logFiles.logFilesDirectory());
        this.logHeaderCache = new LogHeaderCache(1000);
        this.channelAllocator = new TransactionLogChannelAllocator(context, fileHelper, logHeaderCache, rotateAtSize);
        this.readerLogVersionBridge = ReaderLogVersionBridge.forFile(this);
        this.rotationMonitor = context.monitors().newMonitor(LogRotationMonitor.class);

        this.memoryTracker = context.memoryTracker();
        this.logger = context.logProvider().getLog(TransactionLogFile.class);
    }

    void initialize(TransactionLogFilesProviders transactionLogFilesProviders) {
        Preconditions.checkArgument(!initCalled, "Initialize must be called before init");
        this.transactionLogFilesProviders = transactionLogFilesProviders;
    }

    @Override
    public void init() throws IOException {
        this.logRotation = transactionLogRotation(
                this,
                context.clock(),
                databaseHealth,
                rotationMonitor,
                transactionLogFilesProviders.getKernelVersionProvider());
        this.logVersionRepository = transactionLogFilesProviders.getLogVersionRepository();
        initCalled = true;
    }

    @Override
    public void start() throws IOException {
        long currentLogVersion = logVersionRepository.getCurrentLogVersion();
        channel = createLogChannelForVersion(
                currentLogVersion,
                transactionLogFilesProviders::appendIndex,
                transactionLogFilesProviders.getKernelVersionProvider(),
                transactionLogFilesProviders.getLastCommittedChecksumProvider().getLastCommittedChecksum(),
                transactionLogFilesProviders.getLogFormatVersionProvider(),
                transactionLogFilesProviders.getLogTermProvider());

        LogHeader logHeader = extractHeader(currentLogVersion);
        KernelVersion currentKernelVersion =
                transactionLogFilesProviders.getKernelVersionProvider().kernelVersion();
        // In the unlikely case that upgrade transaction was last tx (with or without recovery), we need to rotate
        // to a new file with correct header.
        // The header doesn't contain a kernel version before envelopes, but this corner case can safely be
        // ignored before envelopes since the format doesn't change.
        if (rotationNeededBecauseOfVersionMismatch(currentKernelVersion, logHeader)) {
            logger.debug("Rotation needed because of version mismatch. currentKernelVersion=" + currentKernelVersion
                    + ", currentLogFormat="
                    + transactionLogFilesProviders.getLogFormatVersionProvider().getCurrentLogFormat()
                    + ", file LogFormat=" + logHeader.getLogFormatVersion() + ", file KernelVersion="
                    + logHeader.getKernelVersion());
            KernelVersion logHeaderKernelVersion = logHeader.getKernelVersion();
            assert logHeaderKernelVersion == null || currentKernelVersion.isGreaterThan(logHeaderKernelVersion);
            rotateOnStart(logHeader);
            currentLogVersion = logVersionRepository.getCurrentLogVersion();
        }

        context.monitors()
                .newMonitor(LogRotationMonitor.class)
                .started(channel.getPath(), LogRotationMonitor.LogType.TRANSACTIONS, currentLogVersion, logHeader);

        // try to set position
        seekChannelPosition(currentLogVersion);

        final var channelProvider =
                new PhysicalFlushableLogPositionAwareChannel.VersionedPhysicalFlushableLogChannelProvider(
                        logRotation, context.databaseTracers().getDatabaseTracer(), createScopedBuffer());

        writer = new PhysicalFlushableLogPositionAwareChannel(
                channel, channelAllocator.readLogHeaderForVersion(currentLogVersion), channelProvider);
        if (!context.readOnly()) {
            transactionLogWriter = new TransactionLogWriter(
                    writer,
                    transactionLogFilesProviders.getKernelVersionProvider(),
                    transactionLogFilesProviders.getLogTermProvider(),
                    context.binarySupportedKernelVersions(),
                    logRotation);
        }
    }

    private boolean rotationNeededBecauseOfVersionMismatch(KernelVersion currentKernelVersion, LogHeader logHeader) {
        LogFormat currentLogFormat =
                transactionLogFilesProviders.getLogFormatVersionProvider().getCurrentLogFormat();
        if (logHeader.getLogFormatVersion().getVersionByte() > currentLogFormat.getVersionByte()) {
            throw new IllegalStateException(
                    "The current log format provider on transaction log start up would downgrade the format. The log format config is incorrectly configured. Current %s, last file %s"
                            .formatted(currentLogFormat, logHeader.getLogFormatVersion()));
        }
        if (context.config().get(GraphDatabaseInternalSettings.merged_log)
                && currentLogFormat.usesSegments()
                && logHeader.getKernelVersion().isGreaterThan(currentKernelVersion)) {
            // Mergedlog can have appended a version we have not yet applied and rotated the log. Let that pass without
            // rotation
            // TODO MERGELOG have another look at this when further along with unifying
            //  TransactionLogFile/EnvelopedLogFiles maybe need a better way to say no changes allowed by this class?
            return false;
        }
        return (currentLogFormat.usesSegments() && logHeader.getKernelVersion() != currentKernelVersion)
                || currentLogFormat != logHeader.getLogFormatVersion();
    }

    @Override
    public NativeScopedBuffer createScopedBuffer() {
        return new NativeScopedBuffer(context.bufferSizeBytes(), ByteOrder.LITTLE_ENDIAN, memoryTracker);
    }

    /**
     * For the special case where we need to rotate to a new file directly on start-up.
     * At that point not everything is set up and there is no reason to do any flushing of the old file.
     * This alerts the monitor about rotation since we don't use the regular path through {@link LogRotation}.
     */
    private void rotateOnStart(LogHeader logHeader) throws IOException {
        long startTimeMillis = context.clock().millis();
        rotationMonitor.startRotation(LogRotationMonitor.LogType.TRANSACTIONS, logHeader.getLogVersion());
        long newLogVersion = logVersionRepository.incrementAndGetVersion();

        // Should truncate away any pre-allocated space, so let's find the end.
        seekChannelPosition(logHeader.getLogVersion());
        final var endSize = channel.position();
        channel.truncate(endSize);

        PhysicalLogVersionedStoreChannel newLog = createLogChannelForVersion(
                newLogVersion,
                transactionLogFilesProviders::appendIndex,
                transactionLogFilesProviders.getKernelVersionProvider(),
                transactionLogFilesProviders.getLastCommittedChecksumProvider().getLastCommittedChecksum(),
                transactionLogFilesProviders.getLogFormatVersionProvider(),
                transactionLogFilesProviders.getLogTermProvider());
        channel.close();
        channel = newLog;

        long rotationElapsedTime = context.clock().millis() - startTimeMillis;
        rotationMonitor.finishLogRotation(
                channel.getPath(),
                LogRotationMonitor.LogType.TRANSACTIONS,
                logHeader.getLogVersion(),
                extractHeader(newLogVersion),
                transactionLogFilesProviders.appendIndex(),
                rotationElapsedTime,
                0);
    }

    // In order to be able to write into a logfile after life.stop during shutdown sequence
    // we will close channel and writer only during shutdown phase when all pending changes (like last
    // checkpoint) are already in
    @Override
    public void shutdown() throws IOException {
        IOUtils.closeAll(writer);
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion(long version) throws IOException {
        return openForVersion(version, false);
    }

    @Override
    public PhysicalLogVersionedStoreChannel openForVersion(long version, boolean raw) throws IOException {
        return channelAllocator.openLogChannel(version, raw);
    }

    /**
     * Creates a new channel for the specified version, creating the backing file if it doesn't already exist.
     * If the file exists then the header is verified to be of correct version. Having an existing file there
     * could happen after a previous crash in the middle of rotation, where the new file was created,
     * but the incremented log version changed hadn't made it to persistent storage.
     *
     * @param version log version for the file/channel to create.
     * @param kernelVersionProvider kernel version that should be written down to the log header
     * @return {@link PhysicalLogVersionedStoreChannel} for newly created/opened log file.
     * @throws IOException if there's any I/O related error.
     */
    @Override
    public PhysicalLogVersionedStoreChannel createLogChannelForVersion(
            long version,
            LongSupplier lastAppendIndexSupplier,
            KernelVersionProvider kernelVersionProvider,
            int previousLogFileChecksum,
            LogFormatVersionProvider logFormatVersionProvider,
            LogTermProvider logTermProvider)
            throws IOException {
        return channelAllocator.createLogChannel(
                version,
                lastAppendIndexSupplier.getAsLong(),
                previousLogFileChecksum,
                kernelVersionProvider,
                logFormatVersionProvider,
                logTermProvider);
    }

    /**
     * Creates a new channel for the specified version - assumes the backing file already exists.
     * Verifies the header is complete.
     *
     * @param version log version for the file/channel to create.
     * @return {@link PhysicalLogVersionedStoreChannel} for opened log file.
     * @throws IOException if there's any I/O related error.
     * @throws NoSuchFileException if the backing file didn't exist.
     */
    @Override
    public PhysicalLogVersionedStoreChannel createLogChannelForExistingVersion(long version) throws IOException {
        return channelAllocator.createLogChannelExistingVersion(version);
    }

    @Override
    public void initializeMissingLogFile() throws IOException {
        long currentLogVersion =
                transactionLogFilesProviders.getLogVersionRepository().getCurrentLogVersion();
        channelAllocator.initializeLogFile(
                currentLogVersion,
                transactionLogFilesProviders.appendIndex(),
                transactionLogFilesProviders.getLastCommittedChecksumProvider().getLastCommittedChecksum(),
                transactionLogFilesProviders.getKernelVersionProvider(),
                transactionLogFilesProviders.getLogFormatVersionProvider(),
                transactionLogFilesProviders.getLogTermProvider());
    }

    @Override
    public boolean rotationNeeded() throws IOException {
        return writer.getCurrentLogPosition().getByteOffset() >= rotateAtSize.get();
    }

    @Override
    public void truncate() throws IOException {
        truncate(writer.getCurrentLogPosition());
    }

    @Override
    public synchronized void truncate(LogPosition targetPosition) throws IOException {
        long currentVersion = writer.getCurrentLogPosition().getLogVersion();
        long targetVersion = targetPosition.getLogVersion();
        if (currentVersion < targetVersion) {
            throw new IllegalArgumentException(
                    "Log position requested for restore points to the log file that is higher than "
                            + "existing available highest log file. Requested restore position: "
                            + targetPosition + ", " + "current log file version: "
                            + currentVersion + ".");
        }

        LogPosition lastClosed = transactionLogFilesProviders
                .getLastClosedTransactionPositionProvider()
                .lastClosedPosition();
        if (isCoveredByCommittedTransaction(targetPosition, targetVersion, lastClosed)) {
            throw new IllegalArgumentException(
                    "Log position requested to be used for restore belongs to the log file that "
                            + "was already appended by transaction and cannot be restored. "
                            + "Last closed position: "
                            + lastClosed + ", requested restore: " + targetPosition);
        }

        writer.prepareForFlush().flush();
        if (currentVersion != targetVersion) {
            var oldChannel = channel;
            channel = createLogChannelForExistingVersion(targetVersion);
            writer.setChannel(channel, channelAllocator.readLogHeaderForVersion(targetVersion));
            oldChannel.close();

            if (logVersionRepository != null) {
                logVersionRepository.setCurrentLogVersion(targetVersion);
            }

            // delete newer files
            for (long i = currentVersion; i > targetVersion; i--) {
                delete(i);
            }
        }

        // truncate current file
        channel.truncate(targetPosition.getByteOffset());
        channel.position(channel.size());
    }

    @Override
    public synchronized LogPosition append(
            ByteBuffer byteBuffer,
            OptionalLong appendIndex,
            Optional<Byte> kernelVersionByte,
            int checksum,
            long offset,
            Optional<Byte> logFormatVersion)
            throws IOException {
        checkArgument(byteBuffer.isDirect(), "It is required for byte buffer to be direct.");
        var transactionLogWriter = getTransactionLogWriter();

        try (var logAppendEvent = context.databaseTracers().getDatabaseTracer().logAppend()) {
            long totalAppended = transactionLogWriter.append(
                    byteBuffer, logAppendEvent, appendIndex, kernelVersionByte, checksum, offset, logFormatVersion);
            logAppendEvent.appendedBytes(totalAppended);
            return transactionLogWriter.beforeAppendPosition();
        }
    }

    @Override
    public synchronized RotationInfo rotate() throws IOException {
        return rotate(transactionLogFilesProviders::appendIndex);
    }

    @Override
    public synchronized RotationInfo rotate(
            KernelVersion kernelVersion, long lastAppendIndex, int checksum, long lastTerm, LogFormat logFormat)
            throws IOException {
        channel = rotate(
                channel, () -> lastAppendIndex, () -> kernelVersion, () -> checksum, () -> logFormat, () -> lastTerm);
        LogHeader logHeader = channelAllocator.readLogHeaderForVersion(channel.getLogVersion());
        writer.setChannel(channel, logHeader);
        return new RotationInfo(channel.getPath(), logHeader);
    }

    @Override
    public synchronized RotationInfo rotate(KernelVersion kernelVersion, long lastAppendIndex, int checksum)
            throws IOException {
        channel = rotate(
                channel,
                () -> lastAppendIndex,
                fixed(kernelVersion),
                () -> checksum,
                transactionLogFilesProviders.getLogFormatVersionProvider(),
                transactionLogFilesProviders.getLogTermProvider());
        LogHeader logHeader = channelAllocator.readLogHeaderForVersion(channel.getLogVersion());
        writer.setChannel(channel, logHeader);
        return new RotationInfo(channel.getPath(), logHeader);
    }

    @Override
    public synchronized RotationInfo rotate(KernelVersion kernelVersion) throws IOException {
        throw new UnsupportedOperationException("Transaction log does not support this type of rotation");
    }

    @Override
    public long rotationSize() {
        return rotateAtSize.get();
    }

    public synchronized RotationInfo rotate(long appendIndex) throws IOException {
        return rotate(() -> appendIndex);
    }

    @Override
    public LogRotation getLogRotation() {
        assert initCalled : "LogRotation only available after init";
        return logRotation;
    }

    @Override
    public TransactionLogWriter getTransactionLogWriter() {
        if (context.readOnly()) {
            throw new UnsupportedOperationException("Trying to create writer in read only mode.");
        }
        return transactionLogWriter;
    }

    @Override
    public void flush() throws IOException {
        writer.prepareForFlush().flush();
    }

    @Override
    public ReadableLogChannel getReader(LogPosition position) throws IOException {
        return getReader(position, readerLogVersionBridge);
    }

    @Override
    public ReadableLogChannel getRawReader(LogPosition position) throws IOException {
        return getReader(position, readerLogVersionBridge, true);
    }

    @Override
    public ReadableLogChannel getReader(LogPosition position, LogVersionBridge logVersionBridge) throws IOException {
        return getReader(position, logVersionBridge, false);
    }

    private ReadableLogChannel getReader(LogPosition position, LogVersionBridge logVersionBridge, boolean raw)
            throws IOException {
        PhysicalLogVersionedStoreChannel logChannel = openForVersion(position.getLogVersion(), raw);
        logChannel.position(position.getByteOffset());
        final var logHeader = extractHeader(logChannel.getLogVersion());
        return ReadAheadUtils.newChannel(logChannel, logVersionBridge, logHeader, memoryTracker, raw);
    }

    @Override
    public void accept(LogFileVisitor visitor, LogPosition startingFromPosition) throws IOException {
        try (ReadableLogChannel reader = getReader(startingFromPosition)) {
            visitor.visit(reader);
        }
    }

    @Override
    public long getLogVersion(Path file) {
        return SequentialFileNameHelper.getVersion(file);
    }

    @Override
    public Path getLogFileForVersion(long version) {
        return fileHelper.getFileForVersion(version);
    }

    @Override
    public boolean versionExists(long version) {
        return fileSystem.fileExists(getLogFileForVersion(version));
    }

    @Override
    public LogHeader extractHeader(long version) throws IOException {
        return extractHeader(version, true);
    }

    @Override
    public boolean hasAnyEntries(long version) {
        try {
            Path logFile = getLogFileForVersion(version);
            var logHeader = extractHeader(version, false);
            if (logHeader == null) {
                return false;
            }
            int headerSize = Math.toIntExact(logHeader.getStartPosition().getByteOffset());
            if (fileSystem.getFileSize(logFile) <= headerSize) {
                return false;
            }
            try (StoreChannel channel = fileSystem.read(logFile)) {
                try (var scopedBuffer =
                        new HeapScopedBuffer(headerSize + 5, ByteOrder.LITTLE_ENDIAN, context.memoryTracker())) {
                    var buffer = scopedBuffer.getBuffer();
                    channel.readAll(buffer);
                    buffer.flip();
                    // Read int + byte in case an envelope header has a zero checksum
                    return buffer.getInt(headerSize) != 0 || buffer.get(headerSize + 4) != 0;
                }
            }
        } catch (IOException e) {
            return false;
        }
    }

    @Override
    public long getCurrentLogVersion() {
        if (logVersionRepository != null) {
            return logVersionRepository.getCurrentLogVersion();
        }
        return getLogRangeInfo().highestVersion();
    }

    @Override
    public LogRangeInfo getLogRangeInfo() {
        RangeLogVersionVisitor visitor = new RangeLogVersionVisitor();
        accept(visitor);
        return visitor.getLogRangeInfo();
    }

    @Override
    public void accept(LogVersionVisitor visitor) {
        try {
            for (Path file : fileHelper.getFiles(fileSystem)) {
                visitor.visit(file, getLogVersion(file));
            }
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void accept(LogHeaderVisitor visitor) throws IOException {
        // Start from the where we're currently at and go backwards in time (versions)
        long logVersion = getLogRangeInfo().highestVersion();
        long highAppendIndex = transactionLogFilesProviders.appendIndex();
        while (versionExists(logVersion)) {
            LogHeader logHeader = extractHeader(logVersion, false);
            if (logHeader != null) {
                // This is not necessarily true, there can be one tx/chunk spanning multiple log files in which case
                // the last append index is still the lowest. It is correct that this is not the file with the
                // start of that tx/chunk tough, and the current implementations of the visitor will still find
                // the correct file based on this.
                long lowAppendIndex = logHeader.getLastAppendIndex() + 1;
                LogPosition position = logHeader.getStartPosition();
                if (!visitor.visit(logHeader, position, lowAppendIndex, highAppendIndex)) {
                    break;
                }
                highAppendIndex = logHeader.getLastAppendIndex();
            }
            logVersion--;
        }
    }

    @Override
    public Path[] getMatchedFiles() throws IOException {
        return fileHelper.getFiles(fileSystem);
    }

    @Override
    public void combine(Path additionalLogFilesDirectory, long overLappingAppendIndex) throws IOException {
        if (overLappingAppendIndex != AppendIndexProvider.UNKNOWN_APPEND_INDEX) {
            combineOverlapping(additionalLogFilesDirectory, overLappingAppendIndex);
        }

        long highestLogVersion = getLogRangeInfo().highestVersion();
        var logHelper = TransactionLogFilesHelper.forTransactions(additionalLogFilesDirectory);
        for (Path matchedFile : logHelper.getFiles(fileSystem)) {
            long newFileVersion = ++highestLogVersion;
            Path newFileName = fileHelper.getFileForVersion(newFileVersion);
            fileSystem.renameFile(matchedFile, newFileName);
            try (StoreChannel channel = fileSystem.write(newFileName)) {
                LogHeader logHeader = readLogHeader(fileSystem, newFileName, memoryTracker);
                LogHeader writeHeader = new LogHeader(logHeader, newFileVersion);
                writeLogHeader(channel, writeHeader, memoryTracker);
            }
        }
    }

    private PhysicalLogVersionedStoreChannel createPhysicalChannel(
            StoreChannel storeChannel, LogHeader logHeader, Path filePath) throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                storeChannel,
                logHeader,
                filePath,
                new StoreChannelNativeAccessor(
                        fileSystem, NativeAccessProvider.getNativeAccess(), NullLogProvider.getInstance(), s -> {}),
                DatabaseTracer.NULL);
    }

    private void combineOverlapping(Path additionalLogFilesDirectory, long fromAppendIndex) throws IOException {
        long nextFileVersion = getLogRangeInfo().highestVersion() + 1;
        var entryReader = new VersionAwareLogEntryReader(
                context.commandReaderFactory(), context.binarySupportedKernelVersions(), memoryTracker);

        var logHelper = TransactionLogFilesHelper.forTransactions(additionalLogFilesDirectory);
        var transactionLogFiles = logHelper.getFiles(fileSystem);

        Path matchedFile = null;
        LogHeader matchedFileHeader = null;
        int matchedIndex = -1;
        long sourceOffset = 0L;
        int previousChecksum = 0;

        boolean cutFound = false;

        outer:
        for (var fileIndex = transactionLogFiles.length - 1; fileIndex >= 0; fileIndex--) {
            var file = transactionLogFiles[fileIndex];
            var header = readLogHeader(fileSystem, file, memoryTracker);
            long headerLastAppendIndex = header.getLastAppendIndex();

            if (headerLastAppendIndex >= fromAppendIndex) {
                continue;
            }

            try (StoreChannel channel = fileSystem.read(file)) {
                readLogHeader(channel, true, file, memoryTracker);
                try (var physicalChannel = createPhysicalChannel(channel, header, file)) {
                    var readable = ReadAheadUtils.newChannel(physicalChannel, NO_MORE_CHANNELS, header, memoryTracker);
                    int lastCommitEntryChecksum = header.getPreviousLogFileChecksum();
                    long positionBeforeEntry = readable.alignWithStartEntry();
                    if (readable instanceof EnvelopeReadChannel env) {
                        lastCommitEntryChecksum = env.getPreviousChecksum();
                    }
                    try (var cursor = new LogEntryCursor(entryReader, readable)) {
                        while (cursor.next()) {
                            var entry = cursor.get();
                            long positionAfterEntry =
                                    readable.getCurrentLogPosition().getByteOffset();
                            if (entry instanceof LogEntryStart startEntry) {
                                long currentAppendIndex = startEntry.getAppendIndex();
                                if (currentAppendIndex >= fromAppendIndex) {
                                    sourceOffset = positionBeforeEntry;
                                    matchedFile = file;
                                    matchedFileHeader = header;
                                    matchedIndex = fileIndex;
                                    previousChecksum = lastCommitEntryChecksum;
                                    cutFound = true;
                                    break outer;
                                }
                            } else if (entry instanceof LogEntryCommit commit) {
                                lastCommitEntryChecksum = commit.getChecksum();
                                positionBeforeEntry = positionAfterEntry;
                            }
                        }
                    }
                } catch (ReadPastEndException ex) {
                    // if alignWithStartEntry moved off this file then continue backward search
                }
            }
        }

        if (!cutFound || matchedFile == null) {
            return;
        }

        for (int i = 0; i < matchedIndex; i++) {
            fileSystem.deleteFile(transactionLogFiles[i]);
        }

        int segmentOffset = 0;
        if (matchedFileHeader.getLogFormatVersion().usesSegments()) {
            long segmentBlockSize = matchedFileHeader.getSegmentBlockSize();
            segmentOffset = (int) (sourceOffset % segmentBlockSize);
        }
        var newLogHeader = new LogHeader(matchedFileHeader, nextFileVersion, fromAppendIndex - 1, previousChecksum);
        long destOffset = initializeNewLogFile(newLogHeader, segmentOffset);

        transferData(fileHelper.getFileForVersion(nextFileVersion), matchedFile, destOffset, sourceOffset);
        fileSystem.deleteFile(matchedFile);
    }

    private long initializeNewLogFile(LogHeader newLogHeader, int segmentOffset) throws IOException {
        var newPath = fileHelper.getFileForVersion(newLogHeader.getLogVersion());
        long position;
        try (PhysicalLogVersionedStoreChannel physicalChannel =
                createPhysicalChannel(fileSystem.write(newPath), newLogHeader, newPath)) {

            writeLogHeader(physicalChannel, newLogHeader, memoryTracker);
            position = physicalChannel.position();

            if (newLogHeader.getLogFormatVersion().usesSegments()) {
                try (EnvelopeWriteChannel dest = new EnvelopeWriteChannel(
                        physicalChannel,
                        createScopedBuffer(),
                        newLogHeader.getSegmentBlockSize(),
                        newLogHeader.getPreviousLogFileChecksum(),
                        newLogHeader.getLastAppendIndex(),
                        newLogHeader.getLastTerm(),
                        LogTracers.NULL,
                        LogRotation.NO_ROTATION)) {
                    if (segmentOffset > 0) {
                        dest.insertStartOffset(segmentOffset);
                    }
                    position = dest.position();
                }
            }
            return position;
        }
    }

    private void transferData(Path destFile, Path sourceFile, long destPosition, long sourcePosition)
            throws IOException {
        try (FileChannel dest = FileChannel.open(destFile, StandardOpenOption.WRITE);
                FileChannel source = FileChannel.open(sourceFile, StandardOpenOption.READ)) {
            source.position(sourcePosition);
            dest.position(destPosition);
            dest.transferFrom(source, destPosition, source.size() - sourcePosition);
        }
    }

    /**
     * Called by the appender that just appended a transaction to the log.
     *
     * @param logForceEvents A trace event for the given log append operation.
     * @return {@code true} if we got lucky and were the ones forcing the log.
     */
    @Override
    public boolean forceAfterAppend(LogForceEvents logForceEvents) throws IOException {
        // There's a benign race here, where we add our link before we update our next pointer.
        // This is okay, however, because unparkAll() spins when it sees a null next pointer.
        ThreadLink threadLink = new ThreadLink(Thread.currentThread());
        threadLink.next = threadLinkHead.getAndSet(threadLink);
        boolean attemptedForce = false;

        try (LogForceWaitEvent ignored = logForceEvents.beginLogForceWait()) {
            do {
                if (forceLock.tryLock()) {
                    attemptedForce = true;
                    try {
                        forceLog(logForceEvents);
                        // In the event of any failure a database panic will be raised and thrown here
                    } finally {
                        forceLock.unlock();

                        // We've released the lock, so unpark anyone who might have decided park while we were working.
                        // The most recently parked thread is the one most likely to still have warm caches, so that's
                        // the one we would prefer to unpark. Luckily, the stack nature of the ThreadLinks makes it easy
                        // to get to.
                        ThreadLink nextWaiter = threadLinkHead.get();
                        nextWaiter.unpark();
                    }
                } else {
                    waitForLogForce();
                }
            } while (!threadLink.done);

            // If there were many threads committing simultaneously and I wasn't the lucky one
            // actually doing the forcing (where failure would throw panic exception) I need to
            // explicitly check if everything is OK before considering this transaction committed.
            if (!attemptedForce) {
                databaseHealth.assertNoPanic(IOException.class);
            }
        }
        return attemptedForce;
    }

    @Override
    public void locklessForce(LogForceEvents logForceEvents) throws IOException {
        try (LogForceEvent ignored = logForceEvents.beginLogForce()) {
            flush();
        } catch (final Throwable panic) {
            databaseHealth.panic(panic);
            throw panic;
        }
    }

    @Override
    public void delete(Long version) throws IOException {
        logHeaderCache.clear();
        fileSystem.deleteFile(getLogFileForVersion(version));
        try {
            versionTracker.logDeleted(version);
        } catch (Throwable throwable) {
            logger.error("Error occurred whilst calling logDeleted in the LogFileVersionTracker", throwable);
        }
    }

    @Override
    public void registerExternalReaders(LongObjectMap<StoreChannel> internalChannels) {
        internalChannels.forEachKeyValue((LongObjectProcedure<StoreChannel>) (version, channel) -> externalFileReaders
                .computeIfAbsent(version, any -> new CopyOnWriteArrayList<>())
                .add(channel));
    }

    @Override
    public void unregisterExternalReader(long version, StoreChannel channel) {
        externalFileReaders.computeIfPresent(version, (aLong, storeChannels) -> {
            storeChannels.remove(channel);
            if (storeChannels.isEmpty()) {
                return null;
            }
            return storeChannels;
        });
    }

    @Override
    public void terminateExternalReaders(long maxDeletedVersion) {
        externalFileReaders.entrySet().removeIf(entry -> {
            if (entry.getKey() <= maxDeletedVersion) {
                IOUtils.closeAllSilently(entry.getValue());
                return true;
            }
            return false;
        });
    }

    @VisibleForTesting
    public ConcurrentMap<Long, List<StoreChannel>> getExternalFileReaders() {
        return externalFileReaders;
    }

    private synchronized RotationInfo rotate(LongSupplier appendIndexSupplier) throws IOException {
        channel = rotate(
                channel,
                appendIndexSupplier,
                transactionLogFilesProviders.getKernelVersionProvider(),
                () -> writer.currentChecksum().orElse(BASE_TX_CHECKSUM),
                transactionLogFilesProviders.getLogFormatVersionProvider(),
                transactionLogFilesProviders.getLogTermProvider());
        LogHeader logHeader = channelAllocator.readLogHeaderForVersion(channel.getLogVersion());
        writer.setChannel(channel, logHeader);
        return new RotationInfo(channel.getPath(), logHeader);
    }

    /**
     * This is really only for error handling with the rawTxPull protocol, used for finding a safe truncation point
     * in enveloped logs.
     * There is no good way of knowing if the latest file contains a start point of a tx/chunk.
     * Instead of possibly reading more than one file, the latest file that is guaranteed to contain a starting point
     * is the one searched.
     * </p>
     * With the enveloped log format it is not guaranteed that the start of a file is also the start of a
     * new appended batch (tx or chunk). The protocol needs to find a safe truncation point, and it must be at the
     * end of a completed appended batch to be able to continue writing following pulled transactions correctly to the
     * log.
     * This finds the last completed batch in a previous file.
     * Falls back to the basePosition.
     */
    @Override
    public PositionWithPrevAppendIndex findSafeTruncationPointInPreviousFile(PositionWithPrevAppendIndex basePosition) {
        LogPosition baseLogPosition = basePosition.position();
        checkArgument(baseLogPosition != LogPosition.UNSPECIFIED, "Base position must exist");
        checkArgument(
                basePosition.prevAppendIndexAtPosition() > UNKNOWN_APPEND_INDEX, "Base last append index must exist");
        long highestLogVersion = getLogRangeInfo().highestVersion();
        long startingLogVersion = baseLogPosition.getLogVersion();

        LogPosition safePoint = baseLogPosition;
        long knownAppendIndexAtSafePoint = basePosition.prevAppendIndexAtPosition();
        if (highestLogVersion != startingLogVersion) {
            try {
                long lastAppendedLastFile = extractHeader(highestLogVersion).getLastAppendIndex();
                // Find a previous file with a different lastAppendedIndex = definitely has the start of one tx
                for (long i = highestLogVersion - 1; i >= startingLogVersion; i--) {
                    LogHeader logHeader = extractHeader(i);
                    if (lastAppendedLastFile != logHeader.getLastAppendIndex()) {
                        if (!logHeader.getLogFormatVersion().usesSegments()) {
                            // Looks like the format switch happened in this pull. We know that there is a
                            // transaction boundary on the start of the next file.
                            LogHeader nextFile = extractHeader(i + 1);
                            safePoint = nextFile.getStartPosition();
                            knownAppendIndexAtSafePoint = nextFile.getLastAppendIndex();
                            break;
                        }

                        long seenStart = -1;
                        try (ReadableLogChannel channel = getReader(logHeader.getStartPosition(), NO_MORE_CHANNELS)) {
                            assert channel instanceof EnvelopeReadChannel;
                            EnvelopeReadChannel readChannel = (EnvelopeReadChannel) channel;

                            // Find the last start entry in this file - then we are
                            // guaranteed a full transaction just before that point.
                            long nextStart = readChannel.alignWithStartEntry();
                            while (nextStart > seenStart) {
                                knownAppendIndexAtSafePoint = readChannel.entryIndex() - 1;
                                seenStart = nextStart;
                                nextStart = readChannel.goToNextEntry();
                            }
                        } catch (ReadPastEndException | InvalidEndOfFileReadException e) {
                            if (seenStart != -1) {
                                safePoint = new LogPosition(i, seenStart);
                            }
                        }
                        break;
                    }
                }
            } catch (Exception ignore) {
                // If anything except the expected ReadPastEnd or
                // InvalidEndOfFileRead (read not ending with a full tx) goes wrong, let's just fall back to
                // our starting point instead.
            }
        }

        // Make sure it is after the basePosition
        if (safePoint.getLogVersion() >= baseLogPosition.getLogVersion()
                && (safePoint.getLogVersion() > baseLogPosition.getLogVersion()
                        || safePoint.getByteOffset() >= baseLogPosition.getByteOffset())) {
            return new PositionWithPrevAppendIndex(safePoint, knownAppendIndexAtSafePoint);
        }

        return basePosition;
    }

    @Override
    public long getLastEntryAppendIndexInLogFiles() {
        return transactionLogFilesProviders.appendIndex();
    }

    /**
     * Rotates the current log file, continuing into next (version) log file.
     * This method must be recovery safe, which means a crash at any point should be recoverable.
     * Concurrent readers must also be able to parry for concurrent rotation.
     * Concurrent writes will not be an issue since rotation and writing contends on the same monitor.
     * <br>
     * Steps during rotation are:
     * <ol>
     * <li>1: Increment log version, {@link LogVersionRepository#incrementAndGetVersion()} (also flushes the store)</li>
     * <li>2: Flush current log</li>
     * <li>3: Create new log file</li>
     * <li>4: Write header</li>
     * </ol>
     *
     * Recovery: what happens if crash between:
     * <ol>
     * <li>1-2: New log version has been set, starting the writer will create the new log file idempotently.
     * At this point there may have been half-written transactions in the previous log version,
     * although they haven't been considered committed and so they will be truncated from log during recovery</li>
     * <li>2-3: New log version has been set, starting the writer will create the new log file idempotently.
     * At this point there may be complete transactions in the previous log version which may not have been
     * acknowledged to be committed back to the user, but will be considered committed anyway.</li>
     * <li>3-4: New log version has been set, starting the writer will see that the new file exists and
     * will be forgiving when trying to read the header of it, so that if it isn't complete a fresh
     * header will be set.</li>
     * </ol>
     *
     * Reading: what happens when rotation is between:
     * <ol>
     * <li>1-2: Reader bridge will see that there's a new version (when asking {@link LogVersionRepository}
     * and try to open it. The log file doesn't exist yet though. The bridge can parry for this by catching
     * {@link NoSuchFileException} and tell the reader that the stream has ended</li>
     * <li>2-3: Same as (1-2)</li>
     * <li>3-4: Here the new log file exists, but the header may not be fully written yet.
     * the reader will fail when trying to read the header since it's reading it strictly and bridge
     * catches that exception, treating it the same as if the file didn't exist.</li>
     * </ol>
     *
     * @param currentLog current {@link LogVersionedStoreChannel channel} to flush and close.
     * @param lastAppendIndexSupplier append index supplier
     * @param kernelVersionProvider kernel version provider
     * @param checksumProvider latest checksum provider
     * @return the channel of the newly opened/created log file.
     * @throws IOException if an error regarding closing or opening log files occur.
     */
    private PhysicalLogVersionedStoreChannel rotate(
            LogVersionedStoreChannel currentLog,
            LongSupplier lastAppendIndexSupplier,
            KernelVersionProvider kernelVersionProvider,
            IntSupplier checksumProvider,
            LogFormatVersionProvider logFormatVersionProvider,
            LogTermProvider logTermProvider)
            throws IOException {
        /*
         * The store is now flushed. If we fail now the recovery code will open the
         * current log file and replay everything. That's unnecessary but totally ok.
         */
        long newLogVersion = logVersionRepository.incrementAndGetVersion();

        /*
         * Rotation can happen at any point, although not concurrently with an append,
         * although an append may have (most likely actually) left at least some bytes left
         * in the buffer for future flushing. Flushing that buffer now makes the last appended
         * transaction complete in the log we're rotating away. Awesome.
         */
        writer.prepareForFlush().flush();

        final var logVersion = currentLog.getLogVersion();
        final var endSize = currentLog.position();
        currentLog.truncate(endSize);

        /*
         * The log version is now in the store, flushed and persistent. If we crash
         * now, on recovery we'll attempt to open the version we're about to create
         * (but haven't yet), discover it's not there. That will lead to creating
         * the file, setting the header and continuing.
         * We using committing transaction id as a source of last transaction id here since
         * we can have transactions that are not yet published as committed but were already stored
         * into transaction log that was just rotated.
         */
        PhysicalLogVersionedStoreChannel newLog = createLogChannelForVersion(
                newLogVersion,
                lastAppendIndexSupplier,
                kernelVersionProvider,
                checksumProvider.getAsInt(),
                logFormatVersionProvider,
                logTermProvider);
        currentLog.close();

        try {
            versionTracker.logCompleted(new LogPosition(logVersion, endSize));
        } catch (Throwable throwable) {
            logger.error("Error occurred whilst calling logCompleted in the LogFileVersionTracker", throwable);
        }

        return newLog;
    }

    private static boolean isCoveredByCommittedTransaction(
            LogPosition targetPosition, long targetVersion, LogPosition lastClosed) {
        return lastClosed.getLogVersion() > targetVersion
                || lastClosed.getLogVersion() == targetVersion
                        && lastClosed.getByteOffset() > targetPosition.getByteOffset();
    }

    private void seekChannelPosition(long currentLogVersion) throws IOException {
        jumpToTheLastClosedTxPosition(currentLogVersion);
        LogPosition position;
        try {
            position = scanToEndOfLastLogEntry(false);
        } catch (Exception e) {
            // If we can't read the log, it could be that the last-closed-transaction position in the meta-data store is
            // wrong.
            // We can try again by scanning the log file from the start.
            logger.info("Last closed tx position not found in logVersion " + currentLogVersion
                    + " read ended with " + e.getMessage()
                    + " Rescan for append position from log start");
            jumpToLogStart(currentLogVersion);
            try {
                position = scanToEndOfLastLogEntry(true);
                logger.info("Transaction log file append position: " + position);
            } catch (Exception exception) {
                exception.addSuppressed(e);
                throw exception;
            }
        }
        channel.position(position.getByteOffset());
    }

    private LogPosition scanToEndOfLastLogEntry(boolean alignWithStart) throws IOException {
        // scroll all over possible checkpoints
        final var logHeader = extractHeader(channel.getLogVersion());
        try (var readAheadLogChannel =
                ReadAheadUtils.newChannel(new UnclosableChannel(channel), logHeader, memoryTracker)) {
            if (alignWithStart) {
                readAheadLogChannel.alignWithStartEntry();
            }
            final var logEntryReader = new VersionAwareLogEntryReader(
                    context.commandReaderFactory(), context.binarySupportedKernelVersions(), memoryTracker);
            LogEntry entry;
            do {
                // seek to the end the records.
                entry = logEntryReader.readLogEntry(readAheadLogChannel);
            } while (entry != null);
            return logEntryReader.lastPosition();
        }
    }

    private void jumpToTheLastClosedTxPosition(long currentLogVersion) throws IOException {
        LogPosition logPosition = transactionLogFilesProviders
                .getLastClosedTransactionPositionProvider()
                .lastClosedPosition();
        long lastTxOffset = logPosition.getByteOffset();
        long lastTxLogVersion = logPosition.getLogVersion();
        long startPosition = extractHeader(currentLogVersion).getStartPosition().getByteOffset();
        if (lastTxOffset < startPosition || channel.size() < lastTxOffset) {
            return;
        }
        if (lastTxLogVersion == currentLogVersion) {
            channel.position(lastTxOffset);
        }
    }

    private void jumpToLogStart(long currentLogVersion) throws IOException {
        channel.position(extractHeader(currentLogVersion).getStartPosition().getByteOffset());
    }

    private LogHeader extractHeader(long version, boolean strict) throws IOException {
        LogHeader logHeader = logHeaderCache.getLogHeader(version);
        if (logHeader == null) {
            logHeader = readLogHeader(fileSystem, getLogFileForVersion(version), strict, context.memoryTracker());
            if (logHeader == null) {
                return null;
            }
            logHeaderCache.putHeader(version, logHeader);
        }

        return logHeader;
    }

    private void forceLog(LogForceEvents logForceEvents) throws IOException {
        ThreadLink links = threadLinkHead.getAndSet(ThreadLink.END);
        try (LogForceEvent ignored = logForceEvents.beginLogForce()) {
            force();
        } catch (final Throwable panic) {
            databaseHealth.panic(panic);
            throw panic;
        } finally {
            unparkAll(links);
        }
    }

    private static void unparkAll(ThreadLink links) {
        do {
            links.done = true;
            links.unpark();
            ThreadLink tmp;
            do {
                // Spin because of the race:y update when consing.
                tmp = links.next;
            } while (tmp == null);
            links = tmp;
        } while (links != ThreadLink.END);
    }

    private void waitForLogForce() {
        long parkTime = TimeUnit.MILLISECONDS.toNanos(100);
        LockSupport.parkNanos(this, parkTime);
    }

    private void force() throws IOException {
        // Empty buffer into writer. We want to synchronize with appenders somehow so that they
        // don't append while we're doing that. The way rotation is coordinated we can't synchronize
        // on logFile because it would cause deadlocks. Synchronizing on writer assumes that appenders
        // also synchronize on writer.
        Flushable flushable;
        synchronized (this) {
            databaseHealth.assertNoPanic(IOException.class);
            flushable = writer.prepareForFlush();
        }
        // Force the writer outside of the lock.
        // This allows other threads access to the buffer while the writer is being forced.
        try {
            flushable.flush();
        } catch (ClosedChannelException ignored) {
            // This is ok, we were already successful in emptying the buffer, so the channel being closed here means
            // that some other thread is rotating the log and has closed the underlying channel. But since we were
            // successful in emptying the buffer *UNDER THE LOCK* we know that the rotating thread included the changes
            // we emptied into the channel, and thus it is already flushed by that thread.
        }
    }
}
