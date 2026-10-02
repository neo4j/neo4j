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

import static org.assertj.core.api.Assertions.assertThat;
import static org.neo4j.storageengine.api.TransactionIdStore.BASE_TX_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.enveloped.LatestVersions.LATEST_KERNEL_VERSION;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Map;
import java.util.function.LongFunction;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.neo4j.test.extension.testdirectory.TestDirectoryExtension;
import org.neo4j.wal.LogTracers;
import org.neo4j.wal.PhysicalLogVersionedStoreChannel;
import org.neo4j.wal.rotation.LogRotateEvents;
import org.neo4j.wal.rotation.NextLogHeader;

/**
 * Whole-log copies through {@link EnvelopeWriteChannel#appendRaw}, driven by an {@link EnvelopeReadChannel} walk
 * of the source: entries are appended as one call each, envelopes delivered as one chunk per envelope, and after every
 * call the channel state must track the entry's last envelope. The copied log
 * must reproduce the source — byte for byte when the rotation size matches (including rotated file headers, which
 * pins that rotation is seeded from the tracked state), structurally (checksums, indices, terms, intra-segment
 * offsets) when it does not. The whole-file, alternating and rotating copies each run over both
 * {@link PayloadShape}s, so they also see the wider padding runs that only word-granular payloads produce.
 * Ported from PR #38465, re-cut to envelope-aligned chunks and with the entry term decoupled from the append
 * index so an index/term field swap cannot pass.
 */
@TestDirectoryExtension
class EnvelopeWriteChannelRawCopyTest extends EnvelopeWriteChannelTestSupport {
    private static final int ROTATED_ENTRY_COUNT = 4000;

    @ParameterizedTest(name = "segmentSize={0}, payload={1}")
    @CsvSource({"128,BYTE_RUN", "128,LONG_RUN", "256,BYTE_RUN", "256,LONG_RUN"})
    void appendRawCopyOfFileTracksStateOfLastCompletedEnvelope(int segmentSize, PayloadShape payload)
            throws IOException {
        copyFileByEnvelopes(segmentSize, false, payload);
    }

    @ParameterizedTest(name = "segmentSize={0}, payload={1}")
    @CsvSource({"128,BYTE_RUN", "128,LONG_RUN", "256,BYTE_RUN", "256,LONG_RUN"})
    void appendRawWithFlushTracksStateOfLastCompletedEnvelope(int segmentSize, PayloadShape payload)
            throws IOException {
        copyFileByEnvelopes(segmentSize, true, payload);
    }

    /**
     * The fixture check behind the {@link PayloadShape#LONG_RUN} parameter above: a padding run wider than an
     * envelope header only exists in a log whose payloads are word-granular, and it is the run the copy's parser
     * has to classify as padding rather than as a header.
     */
    @ParameterizedTest
    @ValueSource(ints = {128, 256})
    void longRunPayloadsGeneratePaddingWiderThanAnEnvelopeHeader(int segmentSize) throws IOException {
        try (var storeChannel = srcStoreChannel()) {
            writeEnvelopeData(storeChannel, segmentSize, PayloadShape.LONG_RUN);
        }
        try (var storeChannel = srcStoreChannel()) {
            assertThat(widestPaddingRun(readEnvelopeData(storeChannel, segmentSize)))
                    .isGreaterThan(HEADER_SIZE);
        }
    }

    private static long widestPaddingRun(Map<Long, EnvelopeInfo> envelopes) {
        long widest = 0;
        for (var envelope : envelopes.values()) {
            widest = Math.max(widest, envelope.endPosition() - envelope.unpaddedEndPosition());
        }
        return widest;
    }

    private void copyFileByEnvelopes(int segmentSize, boolean flushAfterEveryCall, PayloadShape payload)
            throws IOException {
        try (var storeChannel = srcStoreChannel()) {
            writeEnvelopeData(storeChannel, segmentSize, payload);
        }
        Map<Long, EnvelopeInfo> envelopes;
        try (var storeChannel = srcStoreChannel()) {
            envelopes = readEnvelopeData(storeChannel, segmentSize);
        }

        try (var srcStoreChannel = srcStoreChannel();
                var destStoreChannel = destStoreChannel()) {
            copySegmentZero(srcStoreChannel, destStoreChannel, segmentSize);
            try (var writer = createWriter(destStoreChannel, segmentSize, -1L)) {
                for (long i = 0; i < envelopes.size(); ) {
                    long end = entryEnd(envelopes, i);
                    var chunks = new ByteBuffer[(int) (end - i)];
                    for (long f = i; f < end; f++) {
                        var env = envelopes.get(f);
                        chunks[(int) (f - i)] = chunk(srcStoreChannel, env.beginPosition(), env.endPosition());
                    }
                    var last = envelopes.get(end - 1);
                    writer.appendRaw(chunks, last.appendIndex(), last.term());
                    if (flushAfterEveryCall) {
                        writer.prepareForFlush().flush();
                    }
                    assertThat(writer.position()).isGreaterThanOrEqualTo(last.unpaddedEndPosition());
                    assertThat(writer.currentIndex()).isEqualTo(last.appendIndex());
                    assertChecksum(writer.currentChecksum(), last.checksum());
                    assertThat(writer.currentTerm()).isEqualTo(last.term());
                    i = end;
                }
            }
        }
        assertThat(fileBytes(destLogPath())).isEqualTo(fileBytes(srcLogPath()));
    }

    // alternate which parity is raw-copied so both handoff orders (pojo->raw and raw->pojo) and the padding
    // re-creation between them are exercised
    @ParameterizedTest(name = "rawParity={0}, payload={1}")
    @CsvSource({"0,BYTE_RUN", "0,LONG_RUN", "1,BYTE_RUN", "1,LONG_RUN"})
    void alternatingPojoAndRawAppendsReproduceSourceFile(int rawParity, PayloadShape payload) throws IOException {
        int segmentSize = 256;
        try (var storeChannel = srcStoreChannel()) {
            writeEnvelopeData(storeChannel, segmentSize, payload);
        }
        Map<Long, EnvelopeInfo> envelopes;
        try (var storeChannel = srcStoreChannel()) {
            envelopes = readEnvelopeData(storeChannel, segmentSize);
        }

        try (var srcStoreChannel = srcStoreChannel();
                var destStoreChannel = destStoreChannel()) {
            copySegmentZero(srcStoreChannel, destStoreChannel, segmentSize);
            try (var writer = createWriter(destStoreChannel, segmentSize, -1L)) {
                long cursor = 0;
                for (long appendIndex = 0; appendIndex < ENTRY_COUNT; appendIndex++) {
                    long firstEnvelope = cursor;
                    cursor = entryEnd(envelopes, firstEnvelope);
                    if (appendIndex % 2 == rawParity) {
                        // one chunk per envelope, as the production decoder cuts them
                        var chunks = new ByteBuffer[(int) (cursor - firstEnvelope)];
                        for (long f = firstEnvelope; f < cursor; f++) {
                            var env = envelopes.get(f);
                            chunks[(int) (f - firstEnvelope)] =
                                    chunk(srcStoreChannel, env.beginPosition(), env.endPosition());
                        }
                        writer.appendRaw(
                                chunks, appendIndex, envelopes.get(cursor - 1).term());
                    } else {
                        // no putAppendIndex: the index continues from the state mirrored off the raw envelopes
                        writer.beginChecksumForWriting();
                        writer.putContentType((byte) (appendIndex & 0x7F));
                        writer.putVersion(LATEST_KERNEL_VERSION.version());
                        writer.putTerm(term(appendIndex));
                        payload.write(writer, appendIndex);
                        writer.putChecksum();
                    }
                }
            }
        }
        assertThat(fileBytes(destLogPath())).isEqualTo(fileBytes(srcLogPath()));
    }

    @ParameterizedTest
    // typical envelope, envelope with trailing padding, envelope on a fresh segment after padding, later envelope
    @ValueSource(ints = {3, 7, 8, 500})
    void appendRawWithStartOffsetStaysInSync(int targetAppendIndex) throws IOException {
        int segmentSize = 256;
        try (var storeChannel = srcStoreChannel()) {
            writeEnvelopeData(storeChannel, segmentSize);
        }
        Map<Long, EnvelopeInfo> envelopes;
        try (var storeChannel = srcStoreChannel()) {
            envelopes = readEnvelopeData(storeChannel, segmentSize);
        }

        long startIdx = 0;
        while (envelopes.get(startIdx).appendIndex() < targetAppendIndex) {
            startIdx++;
        }
        // the last envelope before the copied range seeds the writer's checksum/index/term
        var seed = envelopes.get(startIdx - 1);
        long startOffsetSrc = seed.endPosition();
        long skippedBytes = segmentSize * (startOffsetSrc / segmentSize - 1);
        long startOffsetDest = startOffsetSrc - skippedBytes;

        try (var srcStoreChannel = srcStoreChannel();
                var destStoreChannel = destStoreChannel()) {
            copySegmentZero(srcStoreChannel, destStoreChannel, segmentSize);
            try (var writer =
                    createWriter(destStoreChannel, segmentSize, seed.appendIndex(), seed.checksum(), seed.term())) {
                int offsetIntoSegment = (int) (startOffsetSrc % segmentSize);
                if (offsetIntoSegment != 0) {
                    writer.insertStartOffset(offsetIntoSegment);
                }
                for (long i = startIdx; i < envelopes.size(); ) {
                    long end = entryEnd(envelopes, i);
                    var chunks = new ByteBuffer[(int) (end - i)];
                    for (long f = i; f < end; f++) {
                        var env = envelopes.get(f);
                        chunks[(int) (f - i)] = chunk(srcStoreChannel, env.beginPosition(), env.endPosition());
                    }
                    var last = envelopes.get(end - 1);
                    writer.appendRaw(chunks, last.appendIndex(), last.term());
                    assertThat(writer.position() + skippedBytes).isGreaterThanOrEqualTo(last.unpaddedEndPosition());
                    assertThat(writer.currentIndex()).isEqualTo(last.appendIndex());
                    assertChecksum(writer.currentChecksum(), last.checksum());
                    assertThat(writer.currentTerm()).isEqualTo(last.term());
                    i = end;
                }
            }
        }
        // the destination prefix is the log header plus, for a mid-segment start, a START_OFFSET filler — not
        // source bytes: compare the copied suffix
        var srcBytes = fileBytes(srcLogPath());
        var destBytes = fileBytes(destLogPath());
        assertThat(Arrays.copyOfRange(destBytes, (int) startOffsetDest, destBytes.length))
                .isEqualTo(Arrays.copyOfRange(srcBytes, (int) startOffsetSrc, srcBytes.length));
    }

    @ParameterizedTest
    @EnumSource(PayloadShape.class)
    void appendWithRotationBreaksIdentically(PayloadShape payload) throws IOException {
        int segmentSize = 256;
        long maxFileSize = 7L * segmentSize;
        writeRotatedEnvelopeData(segmentSize, maxFileSize, payload);
        var envelopes = readEnvelopeDataBridged(segmentSize, this::srcLogPath);

        try (var writer = createWriterWithRotation(this::destLogPath, segmentSize, maxFileSize, 1L)) {
            long srcVersion = 0;
            var srcChannel = srcStoreChannel(srcVersion);
            try {
                long prevBegin = -1;
                for (long i = 0; i < envelopes.size(); ) {
                    long end = entryEnd(envelopes, i);
                    var chunks = new ByteBuffer[(int) (end - i)];
                    for (long f = i; f < end; f++) {
                        var env = envelopes.get(f);
                        // bridged positions are per file: a position wrap means the envelope is in the next file
                        if (env.beginPosition() < prevBegin) {
                            srcChannel.close();
                            srcChannel = srcStoreChannel(++srcVersion);
                        }
                        prevBegin = env.beginPosition();
                        chunks[(int) (f - i)] = chunk(srcChannel, env.beginPosition(), env.endPosition());
                    }
                    var last = envelopes.get(end - 1);
                    writer.appendRaw(chunks, last.appendIndex(), last.term());
                    assertThat(writer.position()).isGreaterThanOrEqualTo(last.unpaddedEndPosition());
                    assertThat(writer.currentIndex()).isEqualTo(last.appendIndex());
                    assertChecksum(writer.currentChecksum(), last.checksum());
                    assertThat(writer.currentTerm()).isEqualTo(last.term());
                    i = end;
                }
            } finally {
                srcChannel.close();
            }
        }

        long srcFiles = fileCount(this::srcLogPath);
        assertThat(srcFiles).isGreaterThan(1);
        assertThat(fileCount(this::destLogPath)).isEqualTo(srcFiles);
        for (long version = 0; version < srcFiles; version++) {
            assertThat(fileBytes(destLogPath(version)))
                    .as("log file %d, headers included", version)
                    .isEqualTo(fileBytes(srcLogPath(version)));
        }
    }

    @ParameterizedTest
    @EnumSource(PayloadShape.class)
    void appendWithRotationWorksWithDifferentBreakPoints(PayloadShape payload) throws IOException {
        int segmentSize = 256;
        long srcMaxFileSize = 7L * segmentSize;
        long dstMaxFileSize = 5L * segmentSize;
        writeRotatedEnvelopeData(segmentSize, srcMaxFileSize, payload);
        var envelopes = readEnvelopeDataBridged(segmentSize, this::srcLogPath);

        try (var writer = createWriterWithRotation(this::destLogPath, segmentSize, dstMaxFileSize, 1L)) {
            long srcVersion = 0;
            var srcChannel = srcStoreChannel(srcVersion);
            try {
                long prevBegin = -1;
                for (long i = 0; i < envelopes.size(); ) {
                    long end = entryEnd(envelopes, i);
                    var chunks = new ByteBuffer[(int) (end - i)];
                    for (long f = i; f < end; f++) {
                        var env = envelopes.get(f);
                        if (env.beginPosition() < prevBegin) {
                            srcChannel.close();
                            srcChannel = srcStoreChannel(++srcVersion);
                        }
                        prevBegin = env.beginPosition();
                        // ship without the trailing padding, so the writer must re-create every padding run itself —
                        // including at its own file boundaries, which the source never rotated at
                        chunks[(int) (f - i)] = chunk(srcChannel, env.beginPosition(), env.unpaddedEndPosition());
                    }
                    var last = envelopes.get(end - 1);
                    writer.appendRaw(chunks, last.appendIndex(), last.term());
                    assertThat(writer.currentIndex()).isEqualTo(last.appendIndex());
                    assertChecksum(writer.currentChecksum(), last.checksum());
                    assertThat(writer.currentTerm()).isEqualTo(last.term());
                    i = end;
                }
            } finally {
                srcChannel.close();
            }
        }

        assertThat(fileCount(this::destLogPath))
                .as("the smaller rotation size must force file breaks the source never had")
                .isGreaterThan(fileCount(this::srcLogPath));
        var copied = readEnvelopeDataBridged(segmentSize, this::destLogPath);
        assertThat(copied).hasSize(envelopes.size());
        for (long i = 0; i < envelopes.size(); i++) {
            var src = envelopes.get(i);
            var dst = copied.get(i);
            assertThat(dst.appendIndex()).isEqualTo(src.appendIndex());
            assertChecksum(dst.checksum(), src.checksum());
            assertThat(dst.term()).isEqualTo(src.term());
            // same offsets within a segment despite rotating at different points
            assertThat(dst.beginPosition() % segmentSize).isEqualTo(src.beginPosition() % segmentSize);
            assertThat(dst.unpaddedEndPosition() % segmentSize).isEqualTo(src.unpaddedEndPosition() % segmentSize);
            assertThat(dst.endPosition() % segmentSize).isEqualTo(src.endPosition() % segmentSize);
        }
    }

    private void writeRotatedEnvelopeData(int segmentSize, long maxFileSize, PayloadShape payload) throws IOException {
        try (var writer = createWriterWithRotation(this::srcLogPath, segmentSize, maxFileSize, 1L)) {
            for (long appendIndex = 1; appendIndex <= ROTATED_ENTRY_COUNT; ++appendIndex) {
                writer.beginChecksumForWriting();
                writer.putContentType((byte) (appendIndex & 0x7F));
                writer.putVersion(LATEST_KERNEL_VERSION.version());
                writer.putTerm(term(appendIndex));
                payload.write(writer, appendIndex);
                writer.putChecksum();
            }
        }
    }

    private Map<Long, EnvelopeInfo> readEnvelopeDataBridged(int segmentSize, LongFunction<Path> pathProvider)
            throws IOException {
        return readEnvelopeData(storeChannel(0L, pathProvider.apply(0L)), segmentSize, (previousChannel, raw) -> {
            long nextVersion = previousChannel.getLogVersion() + 1;
            if (!fileSystem.fileExists(pathProvider.apply(nextVersion))) {
                return previousChannel;
            }
            var next = storeChannel(nextVersion, pathProvider.apply(nextVersion));
            previousChannel.close();
            return next;
        });
    }

    private EnvelopeWriteChannel createWriterWithRotation(
            LongFunction<Path> pathProvider, int segmentSize, long maxFileSize, long firstAppendIndex)
            throws IOException {
        var initialChannel = storeChannel(0L, pathProvider.apply(0L));
        writeV10Header(initialChannel, segmentSize, firstAppendIndex - 1, 1L, BASE_TX_CHECKSUM);
        var rotation = new EnvelopedLogRotation(maxFileSize, segmentSize, pathProvider, initialChannel);
        return writeChannel(
                initialChannel,
                segmentSize,
                BASE_TX_CHECKSUM,
                buffer(4 * segmentSize),
                rotation,
                LogTracers.NULL,
                segmentSize,
                firstAppendIndex - 1,
                1L);
    }

    private long fileCount(LongFunction<Path> pathProvider) {
        long version = 0;
        while (fileSystem.fileExists(pathProvider.apply(version))) {
            version++;
        }
        return version;
    }

    private class EnvelopedLogRotation implements LogRotationForChannel {
        private final long maxFileSize;
        private final int segmentSize;
        private final LongFunction<Path> pathProvider;
        private PhysicalLogVersionedStoreChannel currentChannel;
        private EnvelopeWriteChannel writer;

        EnvelopedLogRotation(
                long maxFileSize,
                int segmentSize,
                LongFunction<Path> pathProvider,
                PhysicalLogVersionedStoreChannel initialChannel) {
            this.maxFileSize = maxFileSize;
            this.segmentSize = segmentSize;
            this.pathProvider = pathProvider;
            this.currentChannel = initialChannel;
        }

        @Override
        public void bindWriteChannel(EnvelopeWriteChannel writer) {
            this.writer = writer;
        }

        @Override
        public void locklessRotateLogFile(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder)
                throws IOException {
            try (var event = logRotateEvents.beginLogRotate()) {
                long nextVersion = currentChannel.getLogVersion() + 1;
                writer.prepareForFlush().flush();
                var newChannel = storeChannel(nextVersion, pathProvider.apply(nextVersion));
                currentChannel.truncate(currentChannel.position());
                NextLogHeader nextHeader = headerBuilder.build(new NextLogHeader(
                        null, null, writer.currentIndex(), writer.currentChecksum(), writer.currentTerm()));
                writeV10Header(
                        newChannel,
                        segmentSize,
                        nextHeader.lastAppendIndex(),
                        nextHeader.lastTerm(),
                        nextHeader.previousChecksum());
                currentChannel.close();
                currentChannel = newChannel;
                writer.setChannel(newChannel);
                event.rotationCompleted(0);
            }
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
        public boolean locklessRotateLogIfNeeded(LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public boolean locklessBatchedRotateLogIfNeeded(
                LogRotateEvents logRotateEvents, NextLogHeader.Builder headerBuilder) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }

        @Override
        public void rotateLogFile(LogRotateEvents logRotateEvents) {
            throw new UnsupportedOperationException("envelope channel rotation checks are done internally");
        }
    }
}
