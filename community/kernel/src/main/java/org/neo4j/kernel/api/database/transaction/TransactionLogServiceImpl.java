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
package org.neo4j.kernel.api.database.transaction;

import static java.util.Objects.requireNonNull;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_CONSENSUS_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_CHECKSUM;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requirePositive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.locks.Lock;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.LogMetadataProvider;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.wal.AppendBatchInfo;
import org.neo4j.wal.AppendedChunkConsensusIndexLocator;
import org.neo4j.wal.AppendedChunkLogVersionLocator;
import org.neo4j.wal.AppendedChunkPositionLocator;
import org.neo4j.wal.CommandBatchCursor;
import org.neo4j.wal.LogFile;
import org.neo4j.wal.LogPosition;
import org.neo4j.wal.LogicalTransactionStore;
import org.neo4j.wal.NoSuchLogEntryException;
import org.neo4j.wal.WALService;
import org.neo4j.wal.checkpoint.CheckPointer;
import org.neo4j.wal.checkpoint.SimpleTriggerInfo;
import org.neo4j.wal.entry.LogHeader;
import org.neo4j.wal.entry.VersionAwareLogEntryReader;

public class TransactionLogServiceImpl implements TransactionLogService {
    private final LogicalTransactionStore transactionStore;
    private final LogMetadataProvider metadataProvider;

    private final Lock pruneLock;
    private final LogFile logFile;
    private final AvailabilityGuard availabilityGuard;
    private final InternalLog log;
    private final CheckPointer checkPointer;
    private final CommandReaderFactory commandReaderFactory;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;

    public TransactionLogServiceImpl(
            LogMetadataProvider metadataProvider,
            WALService walService,
            LogicalTransactionStore transactionStore,
            Lock pruneLock,
            AvailabilityGuard availabilityGuard,
            InternalLogProvider logProvider,
            CheckPointer checkPointer,
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this.metadataProvider = metadataProvider;
        this.transactionStore = transactionStore;
        this.pruneLock = pruneLock;
        this.logFile = walService.getLogFile();
        this.availabilityGuard = availabilityGuard;
        this.log = logProvider.getLog(getClass());
        this.checkPointer = checkPointer;
        this.commandReaderFactory = commandReaderFactory;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
    }

    @Override
    public TransactionLogChannels logFilesChannels(long startAppendIndex) throws IOException {
        requirePositive(startAppendIndex);
        LogPosition minimalLogPosition = getLogPosition(startAppendIndex);
        // prevent pruning while we build log channels to avoid cases when we will actually prevent pruning to remove
        // files (on some file systems and OSs), or unexpected exceptions while traversing files
        pruneLock.lock();
        try {
            long minimalVersion = minimalLogPosition.getLogVersion();
            var lastAppendBatch = getLastAppendBatch();
            var channels = collectChannels(
                    startAppendIndex,
                    minimalLogPosition,
                    minimalVersion,
                    lastAppendBatch.appendIndex(),
                    lastAppendBatch.logPositionAfter());
            return new TransactionLogChannels(channels);
        } finally {
            pruneLock.unlock();
        }
    }

    @Override
    public LogPosition append(
            ByteBuffer byteBuffer,
            OptionalLong appendIndex,
            Optional<Byte> kernelVersionByte,
            int checksum,
            long offset,
            Optional<Byte> logFormatByte)
            throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        return logFile.append(byteBuffer, appendIndex, kernelVersionByte, checksum, offset, logFormatByte);
    }

    @Override
    public void restore(LogPosition position) throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        logFile.truncate(position);
    }

    @Override
    public void appendCheckpoint(TransactionId transactionId, long appendIndex, String reason) throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        long appendIndexToLookup = appendIndex + 1;
        var logHeader =
                requireNonNull(logFile.extractHeader(logFile.getLogRangeInfo().highestVersion()));

        var lastHeaderPosition = logHeader.getStartPosition();
        var versionLocator = new AppendedChunkLogVersionLocator(appendIndexToLookup);
        logFile.accept(versionLocator);

        var logEntryReader = new VersionAwareLogEntryReader(
                commandReaderFactory, binarySupportedKernelVersions, EmptyMemoryTracker.INSTANCE);
        var transactionPositionLocator = new AppendedChunkPositionLocator(appendIndexToLookup, logEntryReader);
        logFile.accept(
                transactionPositionLocator,
                versionLocator.getOptionalLogPosition().orElse(lastHeaderPosition));
        var position = transactionPositionLocator.getLogPositionOrThrow();
        var consensusIndex = lookupConsensusIndex(appendIndex, logEntryReader);

        log.info(
                "Writing checkpoint to force recovery from append index:`%d` from specific position:`%s` with transaction id:'%s' and consensus index:`%d`.",
                appendIndex, position, transactionId, consensusIndex);

        checkPointer.forceCheckPoint(
                transactionId, appendIndex, consensusIndex, position, new SimpleTriggerInfo(reason));
    }

    private long lookupConsensusIndex(long appendIndex, VersionAwareLogEntryReader logEntryReader) throws IOException {
        var versionLocator = new AppendedChunkLogVersionLocator(appendIndex);
        logFile.accept(versionLocator);
        var startPosition = versionLocator.getOptionalLogPosition();
        if (startPosition.isEmpty()) {
            // The batch isn't in the logs, which is expected when catching up from a member on an older version.
            // Consensus index of the last checkpointed batch is used for mvcc databases.
            // For normal databases, we'll use another consensus index, so it can safely be UNKNOWN_CONSENSUS_INDEX.
            return UNKNOWN_CONSENSUS_INDEX;
        }
        var consensusIndexLocator = new AppendedChunkConsensusIndexLocator(appendIndex, logEntryReader);
        logFile.accept(consensusIndexLocator, startPosition.get());
        return consensusIndexLocator.getConsensusIndex();
    }

    private AppendBatchInfo getLastAppendBatch() throws IOException {
        var lastBatchInfo = metadataProvider.getLastCommittedBatch();
        if (!LogPosition.UNSPECIFIED.equals(lastBatchInfo.logPositionAfter())) {
            return lastBatchInfo;
        }
        return getAppendBatchInfo(lastBatchInfo.appendIndex());
    }

    private ArrayList<LogChannel> collectChannels(
            long startingAppendIndex,
            LogPosition minimalLogPosition,
            long minimalVersion,
            long highestAppendIndex,
            LogPosition highestLogPosition)
            throws IOException {
        var highestLogVersion = highestLogPosition.getLogVersion();
        int exposedChannels = (int) ((highestLogVersion - minimalVersion) + 1);
        var channels = new ArrayList<LogChannel>(exposedChannels);
        var internalChannels = LongObjectMaps.mutable.<StoreChannel>ofInitialCapacity(exposedChannels);
        for (long version = minimalVersion; version <= highestLogVersion; version++) {
            LogHeader logHeader = logFile.extractHeader(version);
            var startPositionAppendIndex = logFileAppendIndex(startingAppendIndex, minimalVersion, version, logHeader);
            var kernelVersion = getKernelVersion(startPositionAppendIndex, logHeader);
            var readOnlyStoreChannel = new ReadOnlyStoreChannel(logFile, version);
            if (version == minimalVersion) {
                readOnlyStoreChannel.position(minimalLogPosition.getByteOffset());
            }
            internalChannels.put(version, readOnlyStoreChannel);
            var endOffset =
                    version < highestLogVersion ? readOnlyStoreChannel.size() : highestLogPosition.getByteOffset();
            var lastAppendIndex = version < highestLogVersion
                    ? logFile.extractHeader(version + 1).getLastAppendIndex()
                    : highestAppendIndex;
            channels.add(new LogChannel(
                    startPositionAppendIndex,
                    kernelVersion,
                    readOnlyStoreChannel,
                    readOnlyStoreChannel.position(),
                    endOffset,
                    lastAppendIndex,
                    logFilePrevChecksum(startPositionAppendIndex, minimalVersion, version, logHeader),
                    logHeader.getLogFormatVersion()));
        }
        logFile.registerExternalReaders(internalChannels);
        return channels;
    }

    private long logFileAppendIndex(long startingAppendIndex, long minimalVersion, long version, LogHeader logHeader) {
        return version == minimalVersion ? startingAppendIndex : logHeader.getLastAppendIndex() + 1;
    }

    private int logFilePrevChecksum(long startingAppendIndex, long minimalVersion, long version, LogHeader logHeader)
            throws IOException {
        return version != minimalVersion
                ? logHeader.getPreviousLogFileChecksum()
                : logFilePrevChecksumMiddleOfFile(logHeader, startingAppendIndex);
    }

    private int logFilePrevChecksumMiddleOfFile(LogHeader logHeader, long startingAppendIndex) throws IOException {
        if (!logHeader.getLogFormatVersion().usesSegments()) {
            // Pre-envelope files doesn't need the checksum for the header on the receiver anyway, let's skip this
            // lookup
            return UNKNOWN_TX_CHECKSUM;
        }
        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(startingAppendIndex)) {
            commandBatchCursor.next();
            return commandBatchCursor.get().previousChecksum();
        } catch (NoSuchLogEntryException e) {
            throw new IllegalArgumentException(
                    "Append index " + startingAppendIndex + " not found in transaction logs.", e);
        }
    }

    private LogPosition getLogPosition(long appendIndex) throws IOException {
        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(appendIndex)) {
            return commandBatchCursor.position();
        } catch (NoSuchLogEntryException e) {
            throw new IllegalArgumentException("Append index " + appendIndex + " not found in transaction logs.", e);
        }
    }

    private AppendBatchInfo getAppendBatchInfo(long appendIndex) throws IOException {
        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(appendIndex)) {
            commandBatchCursor.next();
            long consensusIndex = commandBatchCursor.get().commandBatch().consensusIndex();
            return new AppendBatchInfo(appendIndex, commandBatchCursor.position(), consensusIndex);
        } catch (NoSuchLogEntryException e) {
            throw new IllegalArgumentException("Append index " + appendIndex + " not found in transaction logs.", e);
        }
    }

    private KernelVersion getKernelVersion(long appendIndex, LogHeader logHeader) throws IOException {
        KernelVersion logHeaderKernelVersion = logHeader.getKernelVersion();
        // From the log format with kernel version in the header, each file is guaranteed to only contain entries on one
        // version.
        if (logHeaderKernelVersion != null) {
            return logHeaderKernelVersion;
        }
        // The older logs can contain more than one kernel version per file so let's find the exact entry and get
        // version from there.
        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(appendIndex)) {
            if (!commandBatchCursor.next()) {
                throw new NoSuchLogEntryException(appendIndex);
            }
            return commandBatchCursor.get().commandBatch().kernelVersion();
        } catch (NoSuchLogEntryException e) {
            throw new IllegalArgumentException(
                    "Couldn't get kernel version for append index " + appendIndex
                            + " as it can't be found in transaction logs.",
                    e);
        }
    }

    private static class ReadOnlyStoreChannel extends DelegatingStoreChannel<StoreChannel> {
        private final LogFile logFile;
        private final long version;

        ReadOnlyStoreChannel(LogFile logFile, long version) throws IOException {
            super(logFile.openForVersion(version));
            this.logFile = logFile;
            this.version = version;
        }

        @Override
        public long write(ByteBuffer[] srcs, int offset, int length) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public int write(ByteBuffer src) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public void writeAll(ByteBuffer src) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public void writeAll(ByteBuffer src, long position) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public StoreChannel truncate(long size) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public long write(ByteBuffer[] srcs) {
            throw new UnsupportedOperationException("Read only channel does not support any write operations.");
        }

        @Override
        public void close() throws IOException {
            logFile.unregisterExternalReader(version, this);
            super.close();
        }
    }
}
