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
import org.neo4j.kernel.impl.transaction.log.CommandBatchCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogicalTransactionStore;
import org.neo4j.kernel.impl.transaction.log.NoSuchTransactionException;
import org.neo4j.kernel.impl.transaction.log.TransactionLogVersionLocator;
import org.neo4j.kernel.impl.transaction.log.TransactionOrEndPositionLocator;
import org.neo4j.kernel.impl.transaction.log.checkpoint.CheckPointer;
import org.neo4j.kernel.impl.transaction.log.checkpoint.SimpleTriggerInfo;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.files.LogFile;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.logging.InternalLog;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.storageengine.api.CommandReaderFactory;
import org.neo4j.storageengine.api.TransactionId;
import org.neo4j.storageengine.api.TransactionIdStore;

public class TransactionLogServiceImpl implements TransactionLogService {
    private final LogicalTransactionStore transactionStore;
    private final TransactionIdStore transactionIdStore;

    private final Lock pruneLock;
    private final LogFile logFile;
    private final AvailabilityGuard availabilityGuard;
    private final InternalLog log;
    private final CheckPointer checkPointer;
    private final CommandReaderFactory commandReaderFactory;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;

    public TransactionLogServiceImpl(
            TransactionIdStore transactionIdStore,
            LogFiles logFiles,
            LogicalTransactionStore transactionStore,
            Lock pruneLock,
            AvailabilityGuard availabilityGuard,
            InternalLogProvider logProvider,
            CheckPointer checkPointer,
            CommandReaderFactory commandReaderFactory,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this.transactionIdStore = transactionIdStore;
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
    public TransactionLogChannels logFilesChannels(long startingTxId) throws IOException {
        requirePositive(startingTxId);
        LogPosition minimalLogPosition = getLogPosition(startingTxId, false);
        // prevent pruning while we build log channels to avoid cases when we will actually prevent pruning to remove
        // files (on some file systems and OSs),
        // or unexpected exceptions while traversing files
        pruneLock.lock();
        try {
            long minimalVersion = minimalLogPosition.getLogVersion();
            var highestTxId = transactionIdStore.getLastCommittedTransactionId();
            var highestLogPosition = getLogPosition(highestTxId, true);
            var channels =
                    collectChannels(startingTxId, minimalLogPosition, minimalVersion, highestTxId, highestLogPosition);
            return new TransactionLogChannels(channels);
        } finally {
            pruneLock.unlock();
        }
    }

    @Override
    public LogPosition append(ByteBuffer byteBuffer, OptionalLong transactionId) throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        return logFile.append(byteBuffer, transactionId);
    }

    @Override
    public void restore(LogPosition position) throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        logFile.truncate(position);
    }

    @Override
    public void appendCheckpoint(TransactionId transactionId, String reason) throws IOException {
        checkState(!availabilityGuard.isAvailable(), "Database should not be available.");
        long txId = transactionId.id() + 1;
        var logHeader = requireNonNull(logFile.extractHeader(logFile.getHighestLogVersion()));

        var lastHeaderPosition = logHeader.getStartPosition();
        var versionLocator = new TransactionLogVersionLocator(txId);
        logFile.accept(versionLocator);

        var logEntryReader = new VersionAwareLogEntryReader(commandReaderFactory, binarySupportedKernelVersions);
        var transactionPositionLocator = new TransactionOrEndPositionLocator(txId, logEntryReader);
        logFile.accept(
                transactionPositionLocator,
                versionLocator.getOptionalLogPosition().orElse(lastHeaderPosition));
        var position = transactionPositionLocator.getLogPosition();

        log.info(
                "Writing checkpoint to force recovery from transaction id:`%d` from specific position:`%s`.",
                txId, position);

        // Write checkpoint at the end of txId
        checkPointer.forceCheckPoint(transactionId, position, new SimpleTriggerInfo(reason));
    }

    private ArrayList<LogChannel> collectChannels(
            long startingTxId,
            LogPosition minimalLogPosition,
            long minimalVersion,
            long highestTxId,
            LogPosition highestLogPosition)
            throws IOException {
        var highestLogVersion = highestLogPosition.getLogVersion();
        int exposedChannels = (int) ((highestLogVersion - minimalVersion) + 1);
        var channels = new ArrayList<LogChannel>(exposedChannels);
        var internalChannels = LongObjectMaps.mutable.<StoreChannel>ofInitialCapacity(exposedChannels);
        for (long version = minimalVersion; version <= highestLogVersion; version++) {
            var startPositionTxId = logFileTransactionId(startingTxId, minimalVersion, version);
            var kernelVersion = getKernelVersion(startPositionTxId);
            var readOnlyStoreChannel = new ReadOnlyStoreChannel(logFile, version);
            if (version == minimalVersion) {
                readOnlyStoreChannel.position(minimalLogPosition.getByteOffset());
            }
            internalChannels.put(version, readOnlyStoreChannel);
            var endOffset =
                    version < highestLogVersion ? readOnlyStoreChannel.size() : highestLogPosition.getByteOffset();
            var lastTxId = version < highestLogVersion ? getHeaderLastCommittedTx(version + 1) : highestTxId;
            channels.add(new LogChannel(startPositionTxId, kernelVersion, readOnlyStoreChannel, endOffset, lastTxId));
        }
        logFile.registerExternalReaders(internalChannels);
        return channels;
    }

    private long logFileTransactionId(long startingTxId, long minimalVersion, long version) throws IOException {
        return version == minimalVersion ? startingTxId : getHeaderLastCommittedTx(version) + 1;
    }

    private long getHeaderLastCommittedTx(long version) throws IOException {
        return logFile.extractHeader(version).getLastCommittedTxId();
    }

    private LogPosition getLogPosition(long startingTxId, boolean returnEndPosition) throws IOException {

        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(startingTxId)) {
            if (returnEndPosition) {
                commandBatchCursor.next();
            }
            return commandBatchCursor.position();
        } catch (NoSuchTransactionException e) {
            throw new IllegalArgumentException("Transaction id " + startingTxId + " not found in transaction logs.", e);
        }
    }

    private KernelVersion getKernelVersion(long txId) throws IOException {
        try (CommandBatchCursor commandBatchCursor = transactionStore.getCommandBatches(txId)) {
            if (!commandBatchCursor.next()) {
                throw new NoSuchTransactionException(txId);
            }
            return commandBatchCursor.get().commandBatch().kernelVersion();
        } catch (NoSuchTransactionException e) {
            throw new IllegalArgumentException(
                    "Couldn't get kernel version for transaction id " + txId
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
