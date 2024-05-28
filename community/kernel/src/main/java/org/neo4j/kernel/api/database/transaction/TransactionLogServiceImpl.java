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
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requirePositive;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.OptionalLong;
import java.util.concurrent.locks.Lock;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.io.fs.DelegatingStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.availability.AvailabilityGuard;
import org.neo4j.kernel.impl.transaction.log.AppendBatchInfo;
import org.neo4j.kernel.impl.transaction.log.AppendedChunkLogVersionLocator;
import org.neo4j.kernel.impl.transaction.log.AppendedChunkPositionLocator;
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchLogEntryException;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.MetadataProvider;
import org.neo4j.storageengine.api.TransactionId;

public class TransactionLogServiceImpl implements TransactionLogService {
    private final LogicalTransactionStore transactionStore;
    private final MetadataProvider metadataProvider;

    private final Lock pruneLock;
    private final LogFile logFile;
    private final AvailabilityGuard availabilityGuard;
    private final InternalLog log;
    private final CheckPointer checkPointer;
    private final CommandReaderFactory commandReaderFactory;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;

    public TransactionLogServiceImpl(
            MetadataProvider metadataProvider,
            LogFiles logFiles,
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
        this.logFile = logFiles.getLogFile();
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
    public LogPosition append(ByteBuffer byteBuffer, OptionalLong transactionId, OptionalLong appendIndex)
            throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        return logFile.append(byteBuffer, transactionId, appendIndex);
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
        var logHeader = requireNonNull(logFile.extractHeader(logFile.getHighestLogVersion()));

        var lastHeaderPosition = logHeader.getStartPosition();
        var versionLocator = new AppendedChunkLogVersionLocator(appendIndexToLookup);
        logFile.accept(versionLocator);

        var logEntryReader = new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
        var transactionPositionLocator = new AppendedChunkPositionLocator(appendIndexToLookup, logEntryReader);
        logFile.accept(
                transactionPositionLocator,
                versionLocator.getOptionalLogPosition().orElse(lastHeaderPosition));
        var position = transactionPositionLocator.getLogPositionOrThrow();

        log.info(
                "Writing checkpoint to force recovery from append index:`%d` from specific position:`%s` with transaction id:'%s'.",
                appendIndex, position, transactionId);

        // Write checkpoint at the end of txId
        checkPointer.forceCheckPoint(transactionId, appendIndex, position, new SimpleTriggerInfo(reason));
    }

    private AppendBatchInfo getLastAppendBatch() throws IOException {
        var lastBatchInfo = metadataProvider.lastBatch();
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
            var startPositionAppendIndex = logFileAppendIndex(startingAppendIndex, minimalVersion, version);
            var kernelVersion = getKernelVersion(startPositionAppendIndex);
            var readOnlyStoreChannel = new ReadOnlyStoreChannel(logFile, version);
            if (version == minimalVersion) {
                readOnlyStoreChannel.position(minimalLogPosition.getByteOffset());
            }
            internalChannels.put(version, readOnlyStoreChannel);
            var endOffset =
                    version < highestLogVersion ? readOnlyStoreChannel.size() : highestLogPosition.getByteOffset();
            var lastAppendIndex =
                    version < highestLogVersion ? getHeaderLastAppendIndex(version + 1) : highestAppendIndex;
            channels.add(new LogChannel(
                    startPositionAppendIndex, kernelVersion, readOnlyStoreChannel, endOffset, lastAppendIndex));
        }
        logFile.registerExternalReaders(internalChannels);
        return channels;
    }

    private long logFileAppendIndex(long startingAppendIndex, long minimalVersion, long version) throws IOException {
        return version == minimalVersion ? startingAppendIndex : getHeaderLastAppendIndex(version) + 1;
    }

    private long getHeaderLastAppendIndex(long version) throws IOException {
        return logFile.extractHeader(version).getLastAppendIndex();
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
            return new AppendBatchInfo(appendIndex, commandBatchCursor.position());
        } catch (NoSuchLogEntryException e) {
            throw new IllegalArgumentException("Append index " + appendIndex + " not found in transaction logs.", e);
        }
    }

    private KernelVersion getKernelVersion(long appendIndex) throws IOException {
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
