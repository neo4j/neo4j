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
package org.neo4j.kernel.impl.transaction.log.checkpoint;

import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.KernelVersion.VERSION_APPEND_INDEX_INTRODUCED;
import static org.neo4j.kernel.KernelVersion.VERSION_CHECKPOINT_NOT_COMPLETED_POSITION_INTRODUCED;
import static org.neo4j.kernel.impl.transaction.log.LogVersionBridge.NO_MORE_CHANNELS;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEntrySerializationSets.serializationSet;
import static org.neo4j.storageengine.api.CommandReaderFactory.NO_COMMANDS;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;

import java.io.IOException;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.time.Instant;
import org.neo4j.io.IOUtils;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.kernel.BinarySupportedKernelVersions;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.KernelVersionProvider;
import org.neo4j.kernel.impl.transaction.UnclosableChannel;
import org.neo4j.kernel.impl.transaction.log.LogEntryCursor;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogTailMetadata;
import org.neo4j.kernel.impl.transaction.log.PhysicalFlushableLogPositionAwareChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadAheadUtils;
import org.neo4j.kernel.impl.transaction.log.entry.AbstractVersionAwareLogEntry;
import org.neo4j.kernel.impl.transaction.log.entry.LogEntryTypeCodes;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.entry.VersionAwareLogEntryReader;
import org.neo4j.kernel.impl.transaction.log.entry.v50.LogEntryDetachedCheckpointV5_0;
import org.neo4j.kernel.impl.transaction.log.entry.v520.LogEntryDetachedCheckpointV5_20;
import org.neo4j.kernel.impl.transaction.log.entry.v522.LogEntryDetachedCheckpointV5_22;
import org.neo4j.kernel.impl.transaction.log.files.LogFiles;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogChannelAllocator;
import org.neo4j.kernel.impl.transaction.log.files.TransactionLogFilesContext;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.CheckpointFile;
import org.neo4j.kernel.impl.transaction.log.files.checkpoint.DetachedLogTailScanner;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.log.rotation.monitor.LogRotationMonitor;
import org.neo4j.kernel.impl.transaction.tracing.LogCheckPointEvent;
import org.neo4j.kernel.impl.transaction.tracing.LogForceEvent;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.InternalLog;
import org.neo4j.monitoring.Panic;
import org.neo4j.storageengine.api.LogVersionRepository;
import org.neo4j.storageengine.api.StoreId;
import org.neo4j.storageengine.api.TransactionId;

public class DetachedCheckpointAppender extends LifecycleAdapter implements CheckpointAppender {
    private final LogFiles logFiles;
    private final CheckpointFile checkpointFile;
    private final TransactionLogChannelAllocator channelAllocator;
    private final TransactionLogFilesContext context;
    private final Panic databasePanic;
    private final LogRotation logRotation;
    private final BinarySupportedKernelVersions binarySupportedKernelVersions;
    private final InternalLog log;
    private final DetachedLogTailScanner logTailScanner;
    private StoreId storeId;
    private PhysicalFlushableLogPositionAwareChannel writer;
    private NativeScopedBuffer buffer;
    private PhysicalLogVersionedStoreChannel channel;
    private LogVersionRepository logVersionRepository;

    public DetachedCheckpointAppender(
            LogFiles logFiles,
            TransactionLogChannelAllocator channelAllocator,
            TransactionLogFilesContext context,
            CheckpointFile checkpointFile,
            LogRotation checkpointRotation,
            DetachedLogTailScanner logTailScanner,
            BinarySupportedKernelVersions binarySupportedKernelVersions) {
        this.logFiles = logFiles;
        this.checkpointFile = requireNonNull(checkpointFile);
        this.context = requireNonNull(context);
        this.channelAllocator = requireNonNull(channelAllocator);
        this.databasePanic = requireNonNull(context.getDatabaseHealth());
        this.logRotation = requireNonNull(checkpointRotation);
        this.log = context.getLogProvider().getLog(DetachedCheckpointAppender.class);
        this.logTailScanner = logTailScanner;
        this.binarySupportedKernelVersions = binarySupportedKernelVersions;
    }

    @Override
    public void start() throws IOException {
        this.storeId = context.getStoreId();
        this.logVersionRepository =
                requireNonNull(context.getLogVersionRepositoryProvider().logVersionRepository(logFiles));

        long currentLogVersion = logVersionRepository.getCheckpointLogVersion();
        channel = channelAllocator.createLogChannel(
                currentLogVersion, context.appendIndex(), BASE_TX_CHECKSUM, context.getKernelVersionProvider());

        context.getMonitors().newMonitor(LogRotationMonitor.class).started(channel.getPath(), currentLogVersion);
        seekCheckpointChannel(currentLogVersion);

        buffer = new NativeScopedBuffer(
                context.getBufferSizeBytes(), ByteOrder.LITTLE_ENDIAN, context.getMemoryTracker());
        final var checksumChannelProvider =
                new PhysicalFlushableLogPositionAwareChannel.VersionedPhysicalFlushableLogChannelProvider(
                        logRotation, context.getDatabaseTracers().getDatabaseTracer(), buffer);
        writer = new PhysicalFlushableLogPositionAwareChannel(
                channel, logHeader(currentLogVersion), checksumChannelProvider);
    }

    private LogHeader logHeader(long logVersion) throws IOException {
        return channelAllocator.readLogHeaderForVersion(logVersion);
    }

    private void seekCheckpointChannel(long expectedVersion) throws IOException {
        LogTailMetadata tailMetadata = logTailScanner.getTailMetadata();
        if (tailMetadata.hasUnreadableBytesInCheckpointLogs()) {
            // we had unreadable bytes in the tail before recovery, those should have been truncated away by now
        }
        var lastCheckPoint = tailMetadata.getLastCheckPoint();
        if (lastCheckPoint.isEmpty()) {
            channel.position(lastReadablePosition());
            return;
        }
        LogPosition channelPosition = lastCheckPoint.get().channelPositionAfterCheckpoint();
        if (channelPosition.getLogVersion() != expectedVersion) {
            throw new IllegalStateException("Expected version of checkpoint log " + expectedVersion
                    + ", does not match to found tail version " + channelPosition.getLogVersion());
        }
        channel.position(channelPosition.getByteOffset());
    }

    private long lastReadablePosition() throws IOException {
        try (var reader = ReadAheadUtils.newChannel(
                        new UnclosableChannel(channel),
                        NO_MORE_CHANNELS,
                        logHeader(channel.getLogVersion()),
                        context.getMemoryTracker());
                var logEntryCursor = new LogEntryCursor(
                        new VersionAwareLogEntryReader(NO_COMMANDS, true, binarySupportedKernelVersions), reader)) {
            LogPositionMarker beforeEntry = new LogPositionMarker();
            reader.getCurrentLogPosition(beforeEntry);
            while (logEntryCursor.next()) {
                logEntryCursor.get();
                reader.getCurrentLogPosition(beforeEntry);
            }
            return beforeEntry.getByteOffset();
        }
    }

    @Override
    public void shutdown() throws Exception {
        IOUtils.closeAll(writer, buffer, channel);
        writer = null;
        buffer = null;
        channel = null;
    }

    @Override
    public void checkPoint(
            LogCheckPointEvent logCheckPointEvent,
            TransactionId transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpointedLogPosition,
            Instant checkpointTime,
            String reason)
            throws IOException {
        if (writer == null) {
            // we were not started but on a failure path someone tried to shutdown everything with checkpoint.
            log.warn("Checkpoint was attempted while appender is not started. No checkpoint record will be appended.");
            return;
        }
        synchronized (checkpointFile) {
            try {
                databasePanic.assertNoPanic(IOException.class);
                writer.resetAppendedBytesCounter();
                serializationSet(kernelVersion, binarySupportedKernelVersions)
                        .select(LogEntryTypeCodes.DETACHED_CHECK_POINT_V5_0)
                        .write(
                                writer,
                                createCheckpointEntry(
                                        transactionId,
                                        appendIndex,
                                        kernelVersion,
                                        oldestNotCompletedPosition,
                                        checkpointedLogPosition,
                                        checkpointTime,
                                        reason,
                                        storeId));

                logCheckPointEvent.appendedBytes(writer.getAppendedBytes());
                forceAfterAppend(logCheckPointEvent);
                logRotation.rotateLogIfNeeded(logCheckPointEvent);
            } catch (Throwable cause) {
                databasePanic.panic(cause);
                throw cause;
            }
        }
    }

    public long getCurrentPosition() {
        return channel.position();
    }

    private void forceAfterAppend(LogCheckPointEvent logCheckPointEvent) throws IOException {
        try (LogForceEvent ignored = logCheckPointEvent.beginLogForce()) {
            writer.prepareForFlush().flush();
        }
    }

    public Path rotate() throws IOException {
        channel = rotateChannel(channel, context.appendIndex(), context.getKernelVersionProvider());
        writer.setChannel(channel, logHeader(channel.getLogVersion()));
        return channel.getPath();
    }

    public Path rotate(KernelVersion kernelVersion, long lastAppendIndex) throws IOException {
        channel = rotateChannel(channel, lastAppendIndex, () -> kernelVersion);
        writer.setChannel(channel, logHeader(channel.getLogVersion()));
        return channel.getPath();
    }

    private PhysicalLogVersionedStoreChannel rotateChannel(
            PhysicalLogVersionedStoreChannel channel, long lastAppendIndex, KernelVersionProvider kernelVersionProvider)
            throws IOException {
        long newLogVersion = logVersionRepository.incrementAndGetCheckpointLogVersion();
        writer.prepareForFlush().flush();

        int checksum = writer.currentChecksum().orElse(BASE_TX_CHECKSUM);
        var newChannel =
                channelAllocator.createLogChannel(newLogVersion, lastAppendIndex, checksum, kernelVersionProvider);
        channel.close();
        return newChannel;
    }

    private static AbstractVersionAwareLogEntry createCheckpointEntry(
            TransactionId transactionId,
            long appendIndex,
            KernelVersion kernelVersion,
            LogPosition oldestNotCompletedPosition,
            LogPosition checkpoinedLogPosition,
            Instant checkpointTime,
            String reason,
            StoreId storeId) {
        if (kernelVersion.isAtLeast(VERSION_CHECKPOINT_NOT_COMPLETED_POSITION_INTRODUCED)) {
            return new LogEntryDetachedCheckpointV5_22(
                    kernelVersion,
                    transactionId,
                    appendIndex,
                    oldestNotCompletedPosition,
                    checkpoinedLogPosition,
                    checkpointTime.toEpochMilli(),
                    storeId,
                    reason);
        } else if (kernelVersion.isAtLeast(VERSION_APPEND_INDEX_INTRODUCED)) {
            return new LogEntryDetachedCheckpointV5_20(
                    kernelVersion,
                    transactionId,
                    appendIndex,
                    checkpoinedLogPosition,
                    checkpointTime.toEpochMilli(),
                    storeId,
                    reason);
        }
        return new LogEntryDetachedCheckpointV5_0(
                kernelVersion, transactionId, checkpoinedLogPosition, checkpointTime.toEpochMilli(), storeId, reason);
    }
}
