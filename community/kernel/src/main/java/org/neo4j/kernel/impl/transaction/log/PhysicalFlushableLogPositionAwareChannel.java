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
package org.neo4j.kernel.impl.transaction.log;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.ByteUnit.kibiBytes;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.OptionalInt;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableLogChannel;
import org.neo4j.io.fs.PhysicalLogChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * Decorator around a {@link LogVersionedStoreChannel} making it expose {@link FlushableLogPositionAwareChannel}. This
 * implementation uses a {@link PhysicalFlushableChannel}, which provides buffering for write operations over the
 * decorated channel.
 */
public class PhysicalFlushableLogPositionAwareChannel implements FlushableLogPositionAwareChannel {
    private final PhysicalFlushableLogChannelProvider channelProvider;
    private LogVersionedStoreChannel logVersionedStoreChannel;
    private PhysicalLogChannel checksumChannel;

    @VisibleForTesting
    public PhysicalFlushableLogPositionAwareChannel(
            LogVersionedStoreChannel logVersionedStoreChannel, LogHeader logHeader, MemoryTracker memoryTracker)
            throws IOException {
        this(logVersionedStoreChannel, logHeader, new SingleLogFileChannelProvider(memoryTracker));
    }

    public PhysicalFlushableLogPositionAwareChannel(
            LogVersionedStoreChannel logVersionedStoreChannel,
            LogHeader logHeader,
            PhysicalFlushableLogChannelProvider channelProvider)
            throws IOException {
        this.channelProvider = channelProvider;
        setChannel(logVersionedStoreChannel, logHeader);
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) throws IOException {
        positionMarker.mark(logVersionedStoreChannel.getLogVersion(), checksumChannel.position());
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentLogPosition() throws IOException {
        return new LogPosition(logVersionedStoreChannel.getLogVersion(), checksumChannel.position());
    }

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        if (positionMarker.getLogVersion() != logVersionedStoreChannel.getLogVersion()) {
            throw new IllegalArgumentException("Log position points log version %d but the current one is %d"
                    .formatted(positionMarker.getLogVersion(), logVersionedStoreChannel.getLogVersion()));
        }
        logVersionedStoreChannel.position(positionMarker.getByteOffset());
    }

    @Override
    public Flushable prepareForFlush() throws IOException {
        return checksumChannel.prepareForFlush();
    }

    @Override
    public int putChecksum() throws IOException {
        return checksumChannel.putChecksum();
    }

    @Override
    public void resetAppendedBytesCounter() {
        checksumChannel.resetAppendedBytesCounter();
    }

    @Override
    public void beginChecksumForWriting() {
        checksumChannel.beginChecksumForWriting();
    }

    @Override
    public long getAppendedBytes() {
        return checksumChannel.getAppendedBytes();
    }

    @Override
    public FlushableChannel put(byte value) throws IOException {
        return checksumChannel.put(value);
    }

    @Override
    public FlushableChannel putShort(short value) throws IOException {
        return checksumChannel.putShort(value);
    }

    @Override
    public FlushableChannel putInt(int value) throws IOException {
        return checksumChannel.putInt(value);
    }

    @Override
    public FlushableChannel putLong(long value) throws IOException {
        return checksumChannel.putLong(value);
    }

    @Override
    public FlushableChannel putFloat(float value) throws IOException {
        return checksumChannel.putFloat(value);
    }

    @Override
    public FlushableChannel putDouble(double value) throws IOException {
        return checksumChannel.putDouble(value);
    }

    @Override
    public FlushableChannel put(byte[] value, int offset, int length) throws IOException {
        return checksumChannel.put(value, offset, length);
    }

    @Override
    public FlushableChannel putAll(ByteBuffer src) throws IOException {
        return checksumChannel.putAll(src);
    }

    @Override
    public FlushableChannel putVersion(byte version) throws IOException {
        return checksumChannel.putVersion(version);
    }

    @Override
    public boolean isOpen() {
        return checksumChannel.isOpen();
    }

    @Override
    public void close() throws IOException {
        checksumChannel.close();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        int remaining = buffer.remaining();
        checksumChannel.putAll(buffer);
        return remaining;
    }

    public void setChannel(LogVersionedStoreChannel logChannel, LogHeader logHeader) throws IOException {
        final var prevLogChannel = logVersionedStoreChannel;
        logVersionedStoreChannel = logChannel;
        if (channelProvider.isNewChannelRequired(prevLogChannel, logChannel)) {
            checksumChannel = channelProvider.create(logChannel, logHeader);
        } else {
            checksumChannel.setChannel(logChannel);
        }
    }

    /**
     * @return the checksum for the channel (if supported)
     */
    public OptionalInt currentChecksum() {
        if (checksumChannel instanceof EnvelopeWriteChannel writeChannel) {
            return OptionalInt.of(writeChannel.currentChecksum());
        } else {
            return OptionalInt.empty();
        }
    }

    public interface PhysicalFlushableLogChannelProvider {
        /**
         * @param currentChannel the current log file or <code>null</code> if the channel does not yet exist
         * @param newLogChannel the new log file
         * @return <code>true</code> if this provider should create a new {@link PhysicalFlushableLogChannel}
         */
        boolean isNewChannelRequired(LogVersionedStoreChannel currentChannel, LogVersionedStoreChannel newLogChannel);

        /**
         *
         * @param logChannel the log channel to wrap as a {@link FlushableChannel}
         * @param logHeader metadata about the log channel
         * @return a new {@link PhysicalFlushableLogChannel}
         */
        PhysicalLogChannel create(LogVersionedStoreChannel logChannel, LogHeader logHeader) throws IOException;
    }

    public static class VersionedPhysicalFlushableLogChannelProvider implements PhysicalFlushableLogChannelProvider {

        private final LogRotation logRotation;
        private final DatabaseTracer databaseTracer;
        private final ScopedBuffer buffer;

        public VersionedPhysicalFlushableLogChannelProvider(
                LogRotation logRotation, DatabaseTracer databaseTracer, ScopedBuffer buffer) {
            this.logRotation = requireNonNull(logRotation);
            this.databaseTracer = requireNonNull(databaseTracer);
            this.buffer = requireNonNull(buffer);
        }

        @Override
        public boolean isNewChannelRequired(
                LogVersionedStoreChannel currentChannel, LogVersionedStoreChannel newLogChannel) {
            return currentChannel == null
                    || currentChannel.getLogFormatVersion().usesSegments()
                            != newLogChannel.getLogFormatVersion().usesSegments();
        }

        @Override
        public PhysicalLogChannel create(LogVersionedStoreChannel logChannel, LogHeader logHeader) throws IOException {
            if (logChannel.getLogFormatVersion().usesSegments()) {
                int previousChecksum = logHeader.getPreviousLogFileChecksum();

                // Apparently not at the start of the file - must update to the correct checksum
                long position = logChannel.position();
                if (position != logHeader.getStartPosition().getByteOffset()) {
                    // Providing our own buffer since we don't want to close the read channel - which would close the
                    // underlying channel.
                    try (var buffer = new NativeScopedBuffer(
                            logHeader.getSegmentBlockSize(), LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE)) {
                        EnvelopeReadChannel envelopeReadChannel = new EnvelopeReadChannel(
                                logChannel,
                                logHeader.getSegmentBlockSize(),
                                LogVersionBridge.NO_MORE_CHANNELS,
                                true,
                                buffer);
                        previousChecksum = envelopeReadChannel.temporaryFindPreviousChecksumBeforePosition(position);
                        logChannel.position(position);
                    }
                }

                return new EnvelopeWriteChannel(
                        logChannel,
                        buffer,
                        logHeader.getSegmentBlockSize(),
                        previousChecksum,
                        EnvelopeWriteChannel
                                .START_INDEX, // Not correct index from cluster perspective -  not needed yet.
                        databaseTracer,
                        logRotation);
            } else {
                return new PhysicalFlushableLogChannel(logChannel, buffer);
            }
        }
    }

    private record SingleLogFileChannelProvider(MemoryTracker memoryTracker)
            implements PhysicalFlushableLogChannelProvider {
        @Override
        public boolean isNewChannelRequired(
                LogVersionedStoreChannel currentChannel, LogVersionedStoreChannel newLogChannel) {
            return currentChannel == null;
        }

        @Override
        public PhysicalLogChannel create(LogVersionedStoreChannel logChannel, LogHeader logHeader) throws IOException {
            if (logChannel.getLogFormatVersion().usesSegments()) {
                return new EnvelopeWriteChannel(
                        logChannel,
                        new HeapScopedBuffer(logHeader.getSegmentBlockSize(), ByteOrder.LITTLE_ENDIAN, memoryTracker),
                        logHeader.getSegmentBlockSize(),
                        logHeader.getPreviousLogFileChecksum(),
                        EnvelopeWriteChannel
                                .START_INDEX, // Not correct index from cluster perspective -  not needed yet.
                        DatabaseTracer.NULL,
                        LogRotation.NO_ROTATION);
            } else {
                return new PhysicalFlushableLogChannel(
                        logChannel, new HeapScopedBuffer((int) kibiBytes(128), ByteOrder.LITTLE_ENDIAN, memoryTracker));
            }
        }
    }
}
