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
package org.neo4j.wal.enveloped;

import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.io.fs.ReadableChannel.BASE_TERM;
import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;
import static org.neo4j.storageengine.AppendIndexProvider.UNKNOWN_APPEND_INDEX;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_ID;
import static org.neo4j.storageengine.api.TransactionIdStore.UNKNOWN_TX_ID;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;
import static org.neo4j.wal.entry.LogHeader.UNSPECIFIED_CREATION_TIME;
import static org.neo4j.wal.rotation.LogRotation.NO_ROTATION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import org.apache.commons.lang3.mutable.MutableInt;
import org.neo4j.io.fs.ChannelNativeAccessor;
import org.neo4j.io.fs.ChecksumWriter;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemUtils;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.storageengine.api.StoreIdentifier;
import org.neo4j.test.RandomSupport;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.utils.TestDirectory;
import org.neo4j.wal.LogTracers;
import org.neo4j.wal.LogVersionBridge;
import org.neo4j.wal.LogVersionedStoreChannel;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.entry.LogEnvelopeHeader;
import org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.wal.entry.LogFormat;
import org.neo4j.wal.rotation.LogRotateEvents;
import org.neo4j.wal.rotation.LogRotation;
import org.neo4j.wal.rotation.NextLogHeader;

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
            public void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
                    throws IOException {
                // header is just test bytes, but capture the seeded boundary state so tests can assert on it
                NextLogHeader nextHeader =
                        headerBuilder.build(new NextLogHeader(null, null, UNKNOWN_APPEND_INDEX, BASE_TX_CHECKSUM, -1));
                lastRotatedAppendIndex = nextHeader.lastAppendIndex();
                lastRotatedPreviousChecksum = nextHeader.previousChecksum();
                lastRotatedTerm = nextHeader.lastTerm();
                rotateLogFile(logRotateEvents);
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
            public boolean locklessRotateLogIfNeeded(
                    LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
                throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
            }

            @Override
            public boolean locklessBatchedRotateLogIfNeeded(
                    LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
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
        return writeChannel(
                channel, segmentSize, checksum, scopedBuffer, logRotation, logTracers, offset, currentIndex, BASE_TERM);
    }

    static EnvelopeWriteChannel writeChannel(
            StoreChannel channel,
            int segmentSize,
            int checksum,
            ScopedBuffer scopedBuffer,
            LogRotation logRotation,
            LogTracers logTracers,
            int offset,
            long currentIndex,
            long initialTerm)
            throws IOException {
        channel.position(offset);
        final var writeChannel = new EnvelopeWriteChannel(
                channel, scopedBuffer, segmentSize, checksum, currentIndex, initialTerm, logTracers, logRotation);
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

    record EnvelopeInfo(
            long beginPosition,
            long endPosition,
            long unpaddedEndPosition,
            int checksum,
            long appendIndex,
            long term) {}

    static Map<Long, EnvelopeInfo> readEnvelopeData(PhysicalLogVersionedStoreChannel storeChannel, int segmentSize)
            throws IOException {
        return readEnvelopeData(storeChannel, segmentSize, LogVersionBridge.NO_MORE_CHANNELS);
    }

    static Map<Long, EnvelopeInfo> readEnvelopeData(
            PhysicalLogVersionedStoreChannel storeChannel, int segmentSize, LogVersionBridge bridge)
            throws IOException {
        var info = new HashMap<Long, EnvelopeInfo>();
        storeChannel.position(0);
        try (var reader = new EnvelopeReadChannel(storeChannel, segmentSize, bridge, INSTANCE, false)) {
            long startPos = reader.alignWithStartEntry();
            long prevPos = startPos;
            long unpaddedEndPos = reader.currentSegment * segmentSize + reader.payloadEndOffset;
            long appendIndex = reader.getAppendIndex();
            int checksum = reader.getChecksum();
            long term = reader.getTerm();
            long i = 0;
            while (true) {
                try {
                    prevPos = startPos;
                    long lastSegment = reader.currentSegment;
                    startPos = reader.goToNextEnvelope();
                    // a position wrap means the reader bridged into the next file: this envelope ended its file
                    long endPos = startPos < prevPos ? (lastSegment + 1) * segmentSize : startPos;
                    info.put(i, new EnvelopeInfo(prevPos, endPos, unpaddedEndPos, checksum, appendIndex, term));
                    appendIndex = reader.getAppendIndex();
                    checksum = reader.getChecksum();
                    term = reader.getTerm();
                    unpaddedEndPos = reader.currentSegment * segmentSize + reader.payloadEndOffset;
                    ++i;
                } catch (ReadPastEndException e) {
                    info.put(
                            i,
                            new EnvelopeInfo(
                                    prevPos, reader.channel().size(), unpaddedEndPos, checksum, appendIndex, term));
                    break;
                }
            }
        }
        return info;
    }

    byte[] fileBytes(long version) throws IOException {
        return fileBytes(logPath(version));
    }

    static final int DATA_SIZE = 111;
    static final int ENTRY_COUNT = 1000;

    static long term(long appendIndex) {
        // decoupled from the index (but still monotonic) so a swapped index/term argument cannot pass
        return 3 * appendIndex + 7;
    }

    /**
     * How a generated entry's payload is written, which decides the padding runs the log ends up holding: a byte
     * array is split at a segment boundary, so its envelopes end flush against the grid, while a primitive is never
     * split and leaves up to seven unwritten bytes there — widening a tail padding run past {@link #HEADER_SIZE},
     * into the dead zone.
     */
    enum PayloadShape {
        BYTE_RUN {
            @Override
            void write(EnvelopeWriteChannel writer, long appendIndex) throws IOException {
                var bytes = new byte[DATA_SIZE];
                Arrays.fill(bytes, (byte) appendIndex);
                writer.put(bytes, 0, bytes.length);
            }
        },
        LONG_RUN {
            @Override
            void write(EnvelopeWriteChannel writer, long appendIndex) throws IOException {
                // the count varies so entries end at every offset in a segment, including the ones where the next
                // envelope's header still fits but its first long does not — the widest padding runs
                long longs = 1 + appendIndex % (DATA_SIZE / Long.BYTES);
                for (long i = 0; i < longs; i++) {
                    writer.putLong(appendIndex);
                }
            }
        };

        abstract void write(EnvelopeWriteChannel writer, long appendIndex) throws IOException;
    }

    static void writeEnvelopeData(PhysicalLogVersionedStoreChannel storeChannel, int segmentSize) throws IOException {
        writeEnvelopeData(storeChannel, segmentSize, PayloadShape.BYTE_RUN);
    }

    static void writeEnvelopeData(PhysicalLogVersionedStoreChannel storeChannel, int segmentSize, PayloadShape payload)
            throws IOException {
        writeV10Header(storeChannel, segmentSize, -1L, BASE_TERM, BASE_TX_CHECKSUM);
        try (var writer = createWriter(storeChannel, segmentSize, -1L)) {
            for (long appendIndex = 0; appendIndex < ENTRY_COUNT; ++appendIndex) {
                writer.beginChecksumForWriting();
                writer.putAppendIndex(appendIndex);
                writer.putContentType((byte) (appendIndex & 0x7F));
                writer.putVersion(LatestVersions.LATEST_KERNEL_VERSION.version());
                writer.putTerm(term(appendIndex));
                payload.write(writer, appendIndex);
                writer.putChecksum();
            }
            writer.prepareForFlush().flush();
        }
    }

    static void writeV10Header(
            PhysicalLogVersionedStoreChannel channel,
            int segmentSize,
            long previousAppendIndex,
            long previousTerm,
            int previousChecksum)
            throws IOException {
        var header = LogFormat.V10.newHeader(
                0L,
                previousAppendIndex,
                previousTerm,
                StoreIdentifier.UNKNOWN,
                segmentSize,
                previousChecksum,
                LatestVersions.LATEST_KERNEL_VERSION,
                UNSPECIFIED_CREATION_TIME,
                UNKNOWN_TX_ID);
        LogFormat.writeLogHeader(channel, header, EmptyMemoryTracker.INSTANCE);
    }

    static EnvelopeWriteChannel createWriter(PhysicalLogVersionedStoreChannel channel, int segmentSize, long lastIndex)
            throws IOException {
        return createWriter(channel, segmentSize, lastIndex, BASE_TX_CHECKSUM, BASE_TERM);
    }

    static EnvelopeWriteChannel createWriter(
            PhysicalLogVersionedStoreChannel channel, int segmentSize, long lastIndex, int lastChecksum, long lastTerm)
            throws IOException {
        return writeChannel(
                channel,
                segmentSize,
                lastChecksum,
                buffer(4 * segmentSize),
                NO_ROTATION,
                LogTracers.NULL,
                segmentSize,
                lastIndex,
                lastTerm);
    }

    static void copySegmentZero(
            PhysicalLogVersionedStoreChannel src, PhysicalLogVersionedStoreChannel dest, int segmentSize)
            throws IOException {
        var headerBuf = ByteBuffer.allocate(segmentSize);
        src.readAll(headerBuf);
        headerBuf.flip();
        dest.write(headerBuf);
    }

    static ByteBuffer chunk(PhysicalLogVersionedStoreChannel channel, long from, long to) throws IOException {
        var buf = ByteBuffer.allocate((int) (to - from));
        channel.readAll(buf, from);
        return buf.flip();
    }

    static ByteBuffer[] chunks(ByteBuffer... chunks) {
        return chunks;
    }

    /** Envelopes {@code [firstEnvelope, return)} are one entry: consecutive envelopes sharing firstEnvelope's append index. */
    static long entryEnd(Map<Long, EnvelopeInfo> envelopes, long firstEnvelope) {
        long appendIndex = envelopes.get(firstEnvelope).appendIndex();
        long end = firstEnvelope;
        while (end < envelopes.size() && envelopes.get(end).appendIndex() == appendIndex) {
            end++;
        }
        return end;
    }

    byte[] fileBytes(Path path) throws IOException {
        return FileSystemUtils.readAllBytes(fileSystem, path, EmptyMemoryTracker.INSTANCE);
    }

    PhysicalLogVersionedStoreChannel srcStoreChannel() throws IOException {
        return srcStoreChannel(1L);
    }

    PhysicalLogVersionedStoreChannel srcStoreChannel(long version) throws IOException {
        return storeChannel(version, srcLogPath(version));
    }

    PhysicalLogVersionedStoreChannel destStoreChannel() throws IOException {
        return storeChannel(1L, destLogPath());
    }

    PhysicalLogVersionedStoreChannel storeChannel(long version, Path logPath) throws IOException {
        return new PhysicalLogVersionedStoreChannel(
                fileSystem.write(logPath),
                version,
                LogFormat.V10,
                logPath,
                ChannelNativeAccessor.EMPTY_ACCESSOR,
                LogTracers.NULL);
    }

    Path srcLogPath() {
        return srcLogPath(1L);
    }

    Path srcLogPath(long version) {
        return directory.homePath().resolve("src_log." + version);
    }

    Path destLogPath() {
        return destLogPath(1L);
    }

    Path destLogPath(long version) {
        return directory.homePath().resolve("dest_log." + version);
    }

    static ByteBuffer zeroBased(ByteBuffer src) {
        // Copy the entry's envelopes into a position-0 buffer so srcIndex/offsets match the prod RawReplicatedContent
        // buffer (which starts at 0); the slice helpers hand back a buffer positioned at segmentSize.
        ByteBuffer out = ByteBuffer.allocate(src.remaining()).order(src.order());
        out.put(src.duplicate());
        return out.flip();
    }

    /** CRC over [OFFSET_ENVELOPE_TYPE, limit) of a zero-based single-envelope buffer, as the writer computes it. */
    static int envelopeCrc(ByteBuffer envelope) {
        var checksum = ChecksumWriter.CHECKSUM_FACTORY.get();
        checksum.update(envelope.duplicate().position(LogEnvelopeHeader.OFFSET_ENVELOPE_TYPE));
        return (int) checksum.getValue();
    }

    ByteBuffer rawEntry(int segmentSize, byte[] payload, long term) throws IOException {
        var srcFile = storeChannel(0);
        var buf = buffer(segmentSize * 8);
        try (var src = writeChannel(srcFile, segmentSize, buf)) {
            writeEntry(src, payload, term);
            src.prepareForFlush();
            // skip the reserved first (header) segment so we feed only the entry's envelopes
            return slice(buf, segmentSize).limit(buf.getBuffer().position());
        }
    }

    /** An entry whose envelopes chain from the given seed state, as if written right after that entry on the source. */
    ByteBuffer rawEntry(int segmentSize, byte[] payload, long term, int seedChecksum, long seedIndex, long seedTerm)
            throws IOException {
        var srcFile = storeChannel(0);
        var buf = buffer(segmentSize * 8);
        try (var src = writeChannel(
                srcFile,
                segmentSize,
                seedChecksum,
                buf,
                NO_ROTATION,
                LogTracers.NULL,
                segmentSize,
                seedIndex,
                seedTerm)) {
            writeEntry(src, payload, term);
            src.prepareForFlush();
            return slice(buf, segmentSize).limit(buf.getBuffer().position());
        }
    }

    ByteBuffer[] rawEntries(int segmentSize, byte[] payloadA, long termA, byte[] payloadB, long termB)
            throws IOException {
        var srcFile = storeChannel(0);
        var buf = buffer(segmentSize * 8);
        try (var src = writeChannel(srcFile, segmentSize, buf)) {
            writeEntry(src, payloadA, termA);
            int splitAt = buf.getBuffer().position();
            writeEntry(src, payloadB, termB);
            int end = buf.getBuffer().position();
            src.prepareForFlush();
            // skip the reserved first (header) segment; entryA envelopes are [segmentSize, splitAt), entryB [splitAt,
            // end)
            ByteBuffer first = slice(buf, segmentSize).limit(splitAt);
            ByteBuffer second = slice(buf).position(splitAt).limit(end);
            return new ByteBuffer[] {first, second};
        }
    }

    static void writeEntry(EnvelopeWriteChannel channel, byte[] payload, long term) throws IOException {
        channel.beginChecksumForWriting();
        channel.putVersion(KERNEL_VERSION);
        channel.putTerm(term);
        channel.putContentType(CONTENT_TYPE);
        channel.put(payload, payload.length);
        channel.endCurrentEntry();
    }

    static final long T1 = 5L;
    static final long T2 = 9L;
    static final long T3 = 13L;
    static final long T4 = 17L;

    static final long SOURCE_LOG_VERSION = 7L;

    record EntrySnapshot(int start, int unpaddedEnd, int checksum, long index, long term) {}

    record SourceLog(ByteBuffer data, List<EntrySnapshot> entries) {}

    /**
     * Writes the given entries through a single pojo channel into log {@value SOURCE_LOG_VERSION}, snapshotting the
     * channel's state and the entry's envelope range after each one. The snapshots are the source of truth the raw
     * replay must mirror.
     */
    SourceLog writeSourceEntries(int segmentSize, int bufferSegments, byte[][] payloads, long[] terms)
            throws IOException {
        var buf = buffer(segmentSize * bufferSegments);
        var entries = new ArrayList<EntrySnapshot>();
        try (var source = writeChannel(storeChannel(SOURCE_LOG_VERSION), segmentSize, buf)) {
            int previousEnd = segmentSize;
            for (int i = 0; i < payloads.length; i++) {
                writeEntry(source, payloads[i], terms[i]);
                int unpaddedEnd = buf.getBuffer().position();
                entries.add(new EntrySnapshot(
                        entryStart(buf.getBuffer(), previousEnd, segmentSize),
                        unpaddedEnd,
                        source.currentChecksum(),
                        source.currentIndex(),
                        source.currentTerm()));
                previousEnd = unpaddedEnd;
            }
            source.prepareForFlush();
            // slice before close: closing the scoped buffer invalidates the original's limit, not the duplicate's
            return new SourceLog(slice(buf), entries);
        }
    }

    /**
     * A source log whose second entry sits behind a dead-zone padding run: entry 1 ends 33 bytes short of the
     * second segment boundary and entry 2's first payload write is an int (as production raft entries: the
     * metadata block size), which cannot fit in the 2 bytes left after the header reservation — the writer
     * discards the empty envelope and zero-pads 33 bytes, more than HEADER_SIZE.
     */
    SourceLog writeDeadZoneSource(int segmentSize) throws IOException {
        int deadZoneTail = HEADER_SIZE + 2;
        var buf = buffer(segmentSize * 8);
        var entries = new ArrayList<EntrySnapshot>();
        try (var source = writeChannel(storeChannel(SOURCE_LOG_VERSION), segmentSize, buf)) {
            writeEntry(source, bytes(random, segmentSize - deadZoneTail - HEADER_SIZE), T1);
            int firstEnd = buf.getBuffer().position();
            entries.add(new EntrySnapshot(
                    segmentSize, firstEnd, source.currentChecksum(), source.currentIndex(), source.currentTerm()));

            source.beginChecksumForWriting();
            source.putVersion(KERNEL_VERSION);
            source.putTerm(T2);
            source.putContentType(CONTENT_TYPE);
            source.putInt(42);
            source.put(bytes(random, 16), 16);
            source.endCurrentEntry();
            entries.add(new EntrySnapshot(
                    entryStart(buf.getBuffer(), firstEnd, segmentSize),
                    buf.getBuffer().position(),
                    source.currentChecksum(),
                    source.currentIndex(),
                    source.currentTerm()));
            source.prepareForFlush();
            return new SourceLog(slice(buf), entries);
        }
    }

    /** A zero type byte after the previous entry's unpadded end means padding, so the entry starts at the boundary. */
    static int entryStart(ByteBuffer data, int previousUnpaddedEnd, int segmentSize) {
        if (data.get(previousUnpaddedEnd + LogEnvelopeHeader.OFFSET_ENVELOPE_TYPE) == 0) {
            return (previousUnpaddedEnd / segmentSize + 1) * segmentSize;
        }
        return previousUnpaddedEnd;
    }

    /** The entry's envelopes without any leading or trailing padding, as the log shipper cuts its chunks. */
    static ByteBuffer entryChunk(SourceLog source, int entry) {
        var snapshot = source.entries().get(entry);
        return zeroBased(source.data()
                .duplicate()
                .position(snapshot.start())
                .limit(snapshot.unpaddedEnd())
                .order(LITTLE_ENDIAN));
    }
}
