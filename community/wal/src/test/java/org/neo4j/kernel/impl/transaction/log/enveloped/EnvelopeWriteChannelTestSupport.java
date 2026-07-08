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
package org.neo4j.kernel.impl.transaction.log.enveloped;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;
import static org.neo4j.kernel.impl.transaction.log.rotation.LogRotation.NO_ROTATION;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.ChecksumWriter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.KernelVersion;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.PhysicalLogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotateEvents;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;

abstract class EnvelopeWriteChannelTestSupport {
    static final int SEGMENT_SIZE = 128;
    static final byte KERNEL_VERSION = 7;
    static final long TERM = 72L;
    static final byte CONTENT_TYPE = 1;
    static final long ROTATION_PERIOD = 42L;
    static final byte[] SMALL_BYTES = new byte[] {4, 5, 6, 7};
    static final long FIRST_INDEX = 0;

    // not injected as we need the checksums to be stable across each run of the tests and @Seed is per-method
    final RandomSupport random = random();

    @Inject
    DefaultFileSystemAbstraction fileSystem;

    @Inject
    TestDirectory directory;

    PhysicalLogVersionedStoreChannel storeChannel() throws IOException {
        return storeChannel(1L);
    }

    PhysicalLogVersionedStoreChannel storeChannel(long version) throws IOException {
        final var logPath = logPath(version);
        return new PhysicalLogVersionedStoreChannel(
                fileSystem.write(logPath),
                version,
                LatestVersions.LATEST_LOG_FORMAT,
                logPath,
                ChannelNativeAccessor.EMPTY_ACCESSOR,
                LogTracers.NULL);
    }

    Path logPath(long version) {
        return directory.homePath().resolve("log." + version);
    }

    // Captured boundary state from the most recent rotation driven through logRotation(...). The header bytes the
    // double writes are random, so these are how a test observes what the channel seeded for the rotated file.
    long lastRotatedAppendIndex = -1;
    long lastRotatedTerm = -1;
    int lastRotatedPreviousChecksum;

    LogRotationForChannel logRotation(
            LogVersionedStoreChannel initialChannel, Supplier<byte[]> logHeader, long maxFileSize) {
        final var currentVersion = new MutableInt(initialChannel.getLogVersion());
        // this is to mimic the behaviour in TransactionLogFile/DetachedCheckpointAppender where the writer
        // manages the updates to the channel on a rotation
        return new LogRotationForChannel() {

            private EnvelopeWriteChannel writeChannel;

            @Override
            public void bindWriteChannel(EnvelopeWriteChannel writeChannel) {
                this.writeChannel = writeChannel;
            }

            @Override
            public void rotateLogFile(LogRotateEvents logRotateEvents) throws IOException {
                try (var event = logRotateEvents.beginLogRotate()) {
                    final var logChannel = storeChannel(currentVersion.incrementAndGet());
                    final var header = logHeader.get();
                    if (header.length > 0) {
                        logChannel.write(ByteBuffer.wrap(header));
                        logChannel.flush();
                    }

                    writeChannel.setChannel(logChannel);
                    event.rotationCompleted(ROTATION_PERIOD);
                }
            }

            @Override
            public void locklessRotateLogFile(
                    LogRotateEvents logRotateEvents, long lastAppendIndex, int previousChecksum, long lastTerm)
                    throws IOException {
                // header is just test bytes, but capture the seeded boundary state so tests can assert on it
                lastRotatedAppendIndex = lastAppendIndex;
                lastRotatedPreviousChecksum = previousChecksum;
                lastRotatedTerm = lastTerm;
                rotateLogFile(logRotateEvents);
            }

            @Override
            public void locklessRotateLogFile(
                    LogRotateEvents logRotateEvents,
                    KernelVersion kernelVersion,
                    long lastAppendIndex,
                    int previousChecksum) {
                throw new UnsupportedOperationException();
            }

            @Override
            public void locklessRotateLogFile(
                    LogRotateEvents logRotateEvents,
                    KernelVersion kernelVersion,
                    long lastAppendIndex,
                    int previousChecksum,
                    LogFormat logFormat) {
                throw new UnsupportedOperationException();
            }

            @Override
            public long rotationSize() {
                return maxFileSize;
            }

            @Override
            public boolean rotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
            }

            @Override
            public boolean locklessBatchedRotateLogIfNeeded(
                    LogRotateEvents logRotateEvents,
                    long appendIndex,
                    KernelVersion kernelVersion,
                    int checksum,
                    LogFormat logFormat) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents) {
                return rotateLogIfNeeded(logRotateEvents);
            }

            @Override
            public boolean locklessRotateLogIfNeeded(
                    LogRotateEvents logRotateEvents, KernelVersion kernelVersion, boolean force) {
                throw new UnsupportedOperationException();
            }
        };
    }

    static EnvelopeWriteChannel writeChannel(StoreChannel channel, int segmentSize, ScopedBuffer scopedBuffer)
            throws IOException {
        return writeChannel(channel, segmentSize, BASE_TX_CHECKSUM, scopedBuffer);
    }

    static EnvelopeWriteChannel writeChannel(
            StoreChannel channel, int segmentSize, int checksum, ScopedBuffer scopedBuffer) throws IOException {
        return writeChannel(channel, segmentSize, checksum, scopedBuffer, NO_ROTATION, LogTracers.NULL);
    }

    static EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            LogTracers logTracers)
            throws IOException {
        return writeChannel(channel, segmentSize, BASE_TX_CHECKSUM, scopedBuffer, logRotation, logTracers);
    }

    static EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            int checksum,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            LogTracers logTracers)
            throws IOException {
        return writeChannel(
                channel, segmentSize, checksum, scopedBuffer, logRotation, logTracers, segmentSize, FIRST_INDEX - 1);
    }

    static EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            int checksum,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            LogTracers logTracers,
            int offset,
            long currentIndex)
            throws IOException {
        channel.position(offset);
        final var writeChannel = new EnvelopeWriteChannel(
                channel,
                scopedBuffer,
                segmentSize,
                checksum,
                currentIndex,
                LogEnvelopeHeader.UNSPECIFIED_TERM,
                logTracers,
                logRotation);
        if (logRotation instanceof LogRotationForChannel rotator) {
            rotator.bindWriteChannel(writeChannel);
        }
        return writeChannel;
    }

    Supplier<byte[]> header(int logHeaderSize) {
        return () -> bytes(random, logHeaderSize);
    }

    static HeapScopedBuffer buffer() {
        return new HeapScopedBuffer(SEGMENT_SIZE, LITTLE_ENDIAN, INSTANCE);
    }

    static HeapScopedBuffer buffer(int segmentSize) {
        return new HeapScopedBuffer(segmentSize, LITTLE_ENDIAN, INSTANCE);
    }

    static void assertBytesArray(ByteBuffer buffer, byte[] expected) {
        final var actualBytes = new byte[expected.length];
        buffer.get(actualBytes);
        assertThat(actualBytes).isEqualTo(expected);
    }

    static ByteBuffer slice(HeapScopedBuffer buffer) {
        return buffer.getBuffer().duplicate().order(LITTLE_ENDIAN).position(0);
    }

    static ByteBuffer slice(HeapScopedBuffer buffer, int segmentSize) {
        return buffer.getBuffer().duplicate().order(LITTLE_ENDIAN).position(segmentSize);
    }

    static ByteBuffer channelData(StoreChannel channel, int segmentSize) throws IOException {
        return channelData(channel, (int) channel.position(), segmentSize);
    }

    static ByteBuffer channelData(StoreChannel channel, int channelSize, int segmentSize) throws IOException {
        final var buffer = ByteBuffer.wrap(new byte[channelSize]).order(LITTLE_ENDIAN);
        channel.position(0).readAll(buffer);
        return buffer.flip().position(segmentSize);
    }

    static byte[] bytes(RandomSupport random, int size) {
        final var bytes = new byte[size];
        random.nextBytes(bytes);
        return bytes;
    }

    static void skipHeader(ByteBuffer data) {
        data.position(data.position() + HEADER_SIZE);
    }

    static RandomSupport random() {
        final var support = new RandomSupport();
        support.setSeed(1665587165007L);
        return support;
    }

    static void assertEnvelopeContents(ByteBuffer data, EnvelopeChunk... envelopeChunks) {
        assertEnvelopeContents(data, BASE_TX_CHECKSUM, envelopeChunks);
    }

    static void assertEnvelopeContents(ByteBuffer data, int initialChecksum, EnvelopeChunk... envelopeChunks) {
        int previousChecksum = initialChecksum;
        for (EnvelopeChunk chunk : envelopeChunks) {
            assertLogEnvelope(data, previousChecksum, chunk);
            if (chunk.type != EnvelopeType.ZERO && chunk.type != EnvelopeType.START_OFFSET) {
                previousChecksum = chunk.checksum;
            }
        }
    }

    static void assertLogEnvelope(ByteBuffer buffer, int previousChecksum, EnvelopeChunk chunk) {
        if (chunk.type == EnvelopeType.ZERO) {
            byte[] padding = new byte[chunk.data.length];
            buffer.get(padding);
            assertThat(padding).as("zero padding").isEqualTo(chunk.data);
            return;
        }

        int payloadChecksum = buffer.getInt();
        assertThat(buffer.get()).as("type").isEqualTo(chunk.type.typeValue);
        assertThat(buffer.getInt()).as("payloadLength").isEqualTo(chunk.data.length);
        assertThat(buffer.getLong()).as("entryIndex").isEqualTo(chunk.entryIndex);
        assertThat(buffer.get()).as("kernelVersion").isEqualTo(chunk.kernelVersion);
        int previousPayloadChecksum = buffer.getInt();
        if (chunk.type != EnvelopeType.START_OFFSET) {
            assertThat(previousPayloadChecksum).as("previousChecksum").isEqualTo(previousChecksum);
            assertThat(buffer.getLong()).as("term").isEqualTo(chunk.term);
        } else {
            // START_OFFSET envelopes do not participate in the checksum chain
            assertThat(previousPayloadChecksum).as("previousChecksum").isZero();
            assertThat(buffer.getLong()).as("term").isEqualTo(chunk.term);
        }
        assertThat(buffer.get()).as("contentType").isEqualTo(chunk.contentType);

        assertBytesArray(buffer, chunk.data);

        // We verify the checksum by last, because it is easier to track down bugs/errors when we first detect
        // the mismatched component above. If everything matches the expected, but the checksum doesn't then it
        // is a sign that something strange is happening with the checksum calculation.
        assertChecksum(payloadChecksum, chunk.checksum);
    }

    static void assertChecksum(int actual, int expected) {
        // We make the assertion as hex string, so if they don't match the produced error message is more clear
        // and easier to check against or update the current checksum values used on setting up the tests.
        assertThat(Integer.toHexString(actual)).as("checksum").isEqualTo(Integer.toHexString(expected));
    }

    static final class EnvelopeChunk {
        private final EnvelopeType type;
        private final int checksum;
        private final byte[] data;
        private final long entryIndex;
        private final byte kernelVersion;
        private final long term;
        private final byte contentType;

        private EnvelopeChunk(EnvelopeType type, long entryIndex, int checksum, byte[] data) {
            this(type, entryIndex, checksum, data, KERNEL_VERSION, TERM, CONTENT_TYPE);
        }

        private EnvelopeChunk(
                EnvelopeType type,
                long entryIndex,
                int checksum,
                byte[] data,
                byte kernelVersion,
                long term,
                byte contentType) {
            this.type = type;
            this.checksum = checksum;
            this.data = data;
            this.kernelVersion = kernelVersion;
            this.entryIndex = entryIndex;
            this.term = term;
            this.contentType = contentType;
        }

        @Override
        public String toString() {
            return String.format(
                    "EnvelopeChunk[type=%s,checksum=%s,length=%s,kernelVersion=%s,entryIndex=%s]",
                    type, checksum, data.length, kernelVersion, entryIndex);
        }
    }

    static EnvelopeChunk envelope(EnvelopeType type, long entryIndex, byte[] payload, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload);
    }

    static EnvelopeChunk envelope(
            EnvelopeType type,
            long entryIndex,
            byte[] payload,
            byte kernelVersion,
            int checksum,
            long term,
            byte contentType) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload, kernelVersion, term, contentType);
    }

    static EnvelopeChunk envelope(
            EnvelopeType type, long entryIndex, byte[] payload, byte kernelVersion, int checksum) {
        return new EnvelopeChunk(type, entryIndex, checksum, payload, kernelVersion, TERM, CONTENT_TYPE);
    }

    static EnvelopeChunk padding(int size) {
        return new EnvelopeChunk(EnvelopeType.ZERO, FIRST_INDEX, 0, new byte[size]);
    }

    static EnvelopeChunk startOffset(int length) {
        return new EnvelopeChunk(
                EnvelopeType.START_OFFSET,
                FIRST_INDEX,
                expectedStartOffsetChecksum(length),
                new byte[length],
                LogEnvelopeHeader.IGNORE_CONTENT_VERSION,
                UNSPECIFIED_TERM,
                UNSPECIFIED_CONTENT_TYPE);
    }

    /**
     * Checksums for start envelopes are quite easy to calculate, so we do it manually here to match what we see
     * from the writer channel.
     */
    static int expectedStartOffsetChecksum(int length) {
        // Full header minus the 4 bytes for checksum (that we're computing now) plus 0's for length.
        final int checksumFieldsLength = HEADER_SIZE - Integer.BYTES + length;
        final byte[] checksumBuffer = new byte[checksumFieldsLength];
        final ByteBuffer checksumView = ByteBuffer.wrap(checksumBuffer)
                .order(LITTLE_ENDIAN)
                // Write the header without the checksum, as we're calculating it right now:
                .put(EnvelopeType.START_OFFSET.typeValue)
                .putInt(length)
                .putLong(0)
                .put(LogEnvelopeHeader.IGNORE_CONTENT_VERSION)
                .putInt(0) // Previous checksum is 0, as start offset does not participate in checksum chain.
                .putLong(UNSPECIFIED_TERM)
                .put(UNSPECIFIED_CONTENT_TYPE);

        final var checksum = ChecksumWriter.CHECKSUM_FACTORY.get();
        checksum.reset();
        checksum.update(checksumView.clear().limit(checksumFieldsLength).position(0));
        return (int) checksum.getValue();
    }

    static void assertZeroHeaderBytes(ByteBuffer buffer) {
        var pos = 0;
        while (pos++ < HEADER_SIZE) {
            assertThat(buffer.get()).isZero();
        }
    }

    interface LogRotationForChannel extends LogRotation {
        void bindWriteChannel(EnvelopeWriteChannel channel);
    }

    record LogContinuityInfo(int offset, long lastAppendIndex, int lastChecksum) {}

    LogContinuityInfo writeLogFileEntries(
            int segmentSize, int newEntryCount, int dataSize, LogContinuityInfo startState) throws IOException {
        int lastChecksum = BASE_TX_CHECKSUM;
        long lastAppendIndex = BASE_TX_ID;
        try (var writer = writeChannel(
                storeChannel(),
                segmentSize,
                startState.lastChecksum(),
                buffer(segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                startState.offset(),
                startState.lastAppendIndex())) {
            for (int i = 0; i < newEntryCount; i++) {
                writer.beginChecksumForWriting();
                writer.putVersion(KERNEL_VERSION);
                writer.putTerm(TERM);
                writer.putContentType(CONTENT_TYPE);
                var byteData = new byte[dataSize];
                Arrays.fill(byteData, (byte) i);
                writer.put(byteData, byteData.length);
                lastChecksum = writer.putChecksum();
                lastAppendIndex = writer.currentIndex();
            }
            return new LogContinuityInfo((int) writer.position(), lastAppendIndex, lastChecksum);
        }
    }

    void directCopyLogData(int segmentSize, long startingAppendIndex, EnvelopeWriteChannel copyWriter)
            throws IOException {
        var readChannel = storeChannel();
        try (var reader = new EnvelopeReadChannel(
                readChannel, segmentSize, LogVersionBridge.NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            long startPos = reader.alignWithStartEntry();
            while (reader.currentIndex < startingAppendIndex) {
                startPos = reader.goToNextEntry();
            }
            // align raw channel to correct offset
            readChannel.position(startPos);
            var readBuf = ByteBuffer.allocate(segmentSize);
            int sent = (int) startPos;
            int readBytes;
            // transfer everything until the current end of file
            while ((readBytes = readChannel.read(readBuf)) > 0) {
                readBuf.flip();
                copyWriter.directPutAll(readBuf, sent);
                sent += readBytes;
            }
            // ensure contents is externally visible
            copyWriter.prepareForFlush().flush();
        }
    }

    int getEndChecksum(int segmentSize) throws IOException {
        try (var reader = new EnvelopeReadChannel(
                storeChannel(2L), segmentSize, LogVersionBridge.NO_MORE_CHANNELS, EmptyMemoryTracker.INSTANCE, false)) {
            reader.alignWithStartEntry();
            try {
                while (true) {
                    reader.goToNextEntry();
                }
            } catch (ReadPastEndException ignore) {
                // Reached end
            }
            return reader.getChecksum();
        }
    }
}
