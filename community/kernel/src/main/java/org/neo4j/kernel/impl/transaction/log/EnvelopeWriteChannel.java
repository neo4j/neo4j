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

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.IGNORE_KERNEL_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireMultipleOf;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.zip.Checksum;
import org.neo4j.io.fs.PhysicalLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
import org.neo4j.kernel.impl.transaction.tracing.DatabaseTracer;
import org.neo4j.util.VisibleForTesting;

/**
 * A channel that will write data in segments.
 * Data will be wrapped in "envelopes" as defined by {@link LogEnvelopeHeader}.
 * <p/>
 * The reason for doing this so to allow the data to be chunked, and safety span multiple files.
 * It will also allow one to start reading from and arbitrary position in the files, which can be
 * beneficial when searching for a specific transaction.
 * <p/>
 * This gets a bit complex, so lets sum up.
 * <ul>
 *   <li>Each file will be divided into x numbers of "buffer windows".</li>
 *   <li>Each "buffer window" will contain one or more segments.</li>
 *   <li>Each segment contains one or more envelopes, with optional padding at the end.</li>
 *   <li>
 *       One or more envelopes are used to represent "logical units", entries, that are
 *       separated by calls to {@link #endCurrentEntry()}. Entries are e.g. transactions.
 *   </li>
 *   <li>The first segment of each file is reserved for the file header.</li>
 * </ul>
 * <pre>
 *     | <---                              file size                              ---> |
 *     | <---        buffer window        ---> | <---        buffer window        ---> |
 *     | <--- segment ---> | <--- segment ---> | <--- segment ---> | <--- segment ---> |
 *     | <- file header -> | [###][###][###]00 | [###############] | [####]            |
 *     | "envelope type"     FULL FULL FULL 00   BEGIN               END               |
 *     | "transactions"      |tx1||tx2||tx3|     | <---    tx 4     --->  |            |
 *                                           ↑                             ↑
 *                                        Padding                   Initial position
 * </pre>
 * The reason for keeping the {@code buffer window} aligned to the file boundaries is to better support direct IO.
 * <p/>
 * Since we write the envelope header as part of completing an envelope, calling {@link #prepareForFlush()} will
 * <strong>only</strong> flush up until the <em>last completed envelope</em>.
 */
public class EnvelopeWriteChannel implements PhysicalLogChannel {

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL =
            "offset size must be at least envelope header size (%d).";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE =
            "offset cannot be bigger than the segment size (%d) and must leave enough space for at least one "
                    + "envelope after it.";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT =
            "START_OFFSET envelopes can only be inserted at the start of the first segment";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_MUST_NOT_BE_INSIDE_ANOTHER_ENVELOPE =
            "START_OFFSET cannot be inserted while another envelope is still open. Close the current entry first.";

    public static final long START_INDEX = 0;
    private static final byte[] PADDING_ZEROES = new byte[MAX_ZERO_PADDING_SIZE];

    private final Checksum checksum = CHECKSUM_FACTORY.get();
    private final ScopedBuffer scopedBuffer;
    private final LogRotation logRotation;
    private final DatabaseTracer databaseTracer;
    private final ByteBuffer buffer;
    private final ByteBuffer checksumView;
    private final int segmentBlockSize;

    private StoreChannel channel;
    private int currentEnvelopeStart;
    private byte currentVersion = IGNORE_KERNEL_VERSION;
    // The index of the current entry. See LogEnvelopeHeader.index.
    private long currentIndex;

    private int lastWrittenPosition;
    private boolean begin = true;
    private int nextSegmentOffset;
    private int previousChecksum;
    private long rotateAtSize;
    private long appendedBytes;
    private volatile boolean closed;

    public EnvelopeWriteChannel(
            StoreChannel channel,
            ScopedBuffer scopedBuffer,
            int segmentBlockSize,
            int initialChecksum,
            long currentIndex,
            DatabaseTracer databaseTracer,
            LogRotation logRotation)
            throws IOException {
        this.channel = requireNonNull(channel);
        this.scopedBuffer = requireNonNull(scopedBuffer);
        this.previousChecksum = initialChecksum;
        requirePowerOfTwo(segmentBlockSize);
        this.segmentBlockSize = segmentBlockSize;
        this.logRotation = requireNonNull(logRotation);
        this.databaseTracer = requireNonNull(databaseTracer);
        this.buffer = scopedBuffer.getBuffer();
        this.checksumView = buffer.duplicate().order(buffer.order());
        this.currentIndex = currentIndex;
        requireMultipleOf("Buffer", buffer.capacity(), "segment block size", segmentBlockSize);

        initialPositions(channel.position());
    }

    public int currentChecksum() {
        return previousChecksum;
    }

    public void endCurrentEntry() throws IOException {
        checkState(currentPayloadLength() > 0, "Closing empty envelope is not allowed.");
        completeEnvelope(true);
        prepareNextEnvelope();
    }

    public void prepareNextEnvelope() throws IOException {
        if ((buffer.position() + LogEnvelopeHeader.HEADER_SIZE) >= nextSegmentOffset) {
            padSegmentAndGoToNext();
        }
        beginNewEnvelope();
    }

    @Override
    public void resetAppendedBytesCounter() {
        appendedBytes = 0;
    }

    @Override
    public long getAppendedBytes() {
        return appendedBytes;
    }

    /**
     * @param channel a newly allocated channel that should already contain a header. The channel must
     *                be position at the end of the header.
     */
    @Override
    public void setChannel(StoreChannel channel) throws IOException {
        checkArgument(
                channel != this.channel,
                "Must NOT update the channel to the same instance otherwise we're overwriting data!");
        this.channel = channel;
        checkState(channel.position() == segmentBlockSize, "must be positioned on first segment");
        initialPositions(segmentBlockSize);
    }

    /**
     * This value is only valid when called after a call to {@link #endCurrentEntry()}
     *
     * @return the position in the channel.
     * @throws IOException when unable to determine the position in the underlying log channel
     */
    @Override
    public long position() throws IOException {
        checkState(
                buffer.position() == currentEnvelopeStart + LogEnvelopeHeader.HEADER_SIZE,
                "position() must be called right after endCurrentEntry()");

        long bufferViewStart = channel.position() - lastWrittenPosition;
        return bufferViewStart + currentEnvelopeStart;
    }

    @Override
    public void beginChecksumForWriting() {
        // nothing
    }

    @Override
    public Flushable prepareForFlush() throws IOException {
        checkChannelClosed(null);
        if (lastWrittenPosition == currentEnvelopeStart) {
            return channel; // Nothing to flush
        }

        int oldPosition = buffer.position();

        // Since we write the header last, we can only flush until the start of the current envelope
        buffer.position(lastWrittenPosition).limit(currentEnvelopeStart);
        try {
            channel.writeAll(buffer);
        } catch (ClosedChannelException e) {
            handleClosedChannelException(e);
        }

        buffer.clear().position(oldPosition);
        lastWrittenPosition = currentEnvelopeStart;

        if (channel.position() >= rotateAtSize) {
            rotateLogFile();
            buffer.position(currentEnvelopeStart);
            // NOTE! 'channel' will be updated by 'setChannel'.
            // 'setChannel' will also update buffer and positions.
        } else if (currentEnvelopeStart >= buffer.capacity()) {
            // Buffer is exhausted, reset and start over
            buffer.clear();
            lastWrittenPosition = 0;
            currentEnvelopeStart = 0;
            nextSegmentOffset = segmentBlockSize;
        }

        return channel;
    }

    @Override
    public EnvelopeWriteChannel put(byte value) throws IOException {
        nextSegmentOnOverflow(Byte.BYTES);
        buffer.put(value);
        return updateBytesWritten(Byte.BYTES);
    }

    @Override
    public EnvelopeWriteChannel putShort(short value) throws IOException {
        nextSegmentOnOverflow(Short.BYTES);
        buffer.putShort(value);
        return updateBytesWritten(Short.BYTES);
    }

    @Override
    public EnvelopeWriteChannel putInt(int value) throws IOException {
        nextSegmentOnOverflow(Integer.BYTES);
        buffer.putInt(value);
        return updateBytesWritten(Integer.BYTES);
    }

    @Override
    public EnvelopeWriteChannel putLong(long value) throws IOException {
        nextSegmentOnOverflow(Long.BYTES);
        buffer.putLong(value);
        return updateBytesWritten(Long.BYTES);
    }

    @Override
    public EnvelopeWriteChannel putFloat(float value) throws IOException {
        nextSegmentOnOverflow(Float.BYTES);
        buffer.putFloat(value);
        return updateBytesWritten(Float.BYTES);
    }

    @Override
    public EnvelopeWriteChannel putDouble(double value) throws IOException {
        nextSegmentOnOverflow(Double.BYTES);
        buffer.putDouble(value);
        return updateBytesWritten(Double.BYTES);
    }

    @Override
    public EnvelopeWriteChannel put(byte[] value, int length) throws IOException {
        return put(value, 0, length);
    }

    @Override
    public EnvelopeWriteChannel put(byte[] src, int offset, int length) throws IOException {
        int srcIndex = offset;
        while (srcIndex < length) {
            int remainingPayloadSpace = nextSegmentOffset - buffer.position();
            int payloadChunk = min(length - srcIndex, remainingPayloadSpace);
            buffer.put(src, srcIndex, payloadChunk);
            srcIndex += payloadChunk;

            if (srcIndex != length) {
                // Still have data left to put
                completeEnvelopeAndGoToNextSegment();
            }
        }

        appendedBytes += length;
        return this;
    }

    @Override
    public EnvelopeWriteChannel putAll(ByteBuffer src) throws IOException {
        int length = src.remaining();
        int srcIndex = src.position();
        while (srcIndex < length) {
            int remainingPayloadSpace = nextSegmentOffset - buffer.position();
            int payloadChunk = min(length - srcIndex, remainingPayloadSpace);
            buffer.put(buffer.position(), src, srcIndex, payloadChunk);
            buffer.position(buffer.position() + payloadChunk);
            srcIndex += payloadChunk;

            if (srcIndex != length) {
                // Still have data left to put
                completeEnvelopeAndGoToNextSegment();
            }
        }
        appendedBytes += length;
        return this;
    }

    @Override
    public EnvelopeWriteChannel putVersion(byte version) {
        currentVersion = version;
        return this;
    }

    @Override
    public int putChecksum() throws IOException {
        endCurrentEntry();
        return previousChecksum;
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    public void truncateToPosition(long position, int previousChecksum, long currentIndex) throws IOException {
        requireNonNegative(position);
        checkArgument(position <= channel.position(), "Can only truncate written data.");
        checkArgument(position >= segmentBlockSize, "Truncating the first segment is not possible");
        this.previousChecksum = previousChecksum;
        channel.truncate(position);
        rotateLogFile();
        this.currentIndex = currentIndex;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            prepareForFlush().flush();
            this.closed = true;
            this.channel.close();
            this.scopedBuffer.close();
        }
    }

    /**
     * @param initialPosition initial position where we should start appending.
     */
    private void initialPositions(long initialPosition) {
        int bufferWindowOffset = (int) (initialPosition % buffer.capacity());
        currentEnvelopeStart = bufferWindowOffset;
        lastWrittenPosition = bufferWindowOffset;
        nextSegmentOffset = (bufferWindowOffset / segmentBlockSize + 1) * segmentBlockSize; // Round up to next
        rotateAtSize = logRotation.rotationSize();
        requireMultipleOf("Rotation size", rotateAtSize, "segment size", segmentBlockSize);
        if (rotateAtSize == 0) {
            rotateAtSize = Long.MAX_VALUE; // Rotation disabled
        }
        buffer.clear().position(bufferWindowOffset + LogEnvelopeHeader.HEADER_SIZE);
    }

    private void completeEnvelopeAndGoToNextSegment() throws IOException {
        completeEnvelope(false);
        padSegmentAndGoToNext();
        beginNewEnvelope();
    }

    /**
     * @param end if this is the last entry
     */
    private void completeEnvelope(boolean end) {
        EnvelopeType type = completedEnvelopeType(begin, end);
        final int payLoadLength = currentPayloadLength();
        if (payLoadLength == 0) {
            checkState(
                    (nextSegmentOffset - currentEnvelopeStart) <= MAX_ZERO_PADDING_SIZE,
                    "Empty envelopes can only be discarded at the end of the segment.");
            // Nothing to complete. This will be the case when we try to start a new entry at the end of the segment.
            // Reset back position to last start and let the padding zero out the rest.
            buffer.position(currentEnvelopeStart);
            return;
        }
        writeHeader(type, payLoadLength);
        begin = end;
        if (end) {
            currentIndex++;
        }
    }

    private void writeHeader(EnvelopeType type, int payloadLength) {
        final int payloadEndOffset = buffer.position();

        // Fill in the header
        final int checksumStartOffset = currentEnvelopeStart + Integer.BYTES;
        buffer.position(checksumStartOffset);
        assert currentVersion != -1;
        buffer.put(type.typeValue)
                .putInt(payloadLength)
                // START_OFFSET envelopes do not have an index, as they are skipped automatically when reading
                .putLong(type != EnvelopeType.START_OFFSET ? currentIndex : 0)
                .put(currentVersion)
                // START_OFFSET envelopes do not participate in the checksum chain
                .putInt(type != EnvelopeType.START_OFFSET ? previousChecksum : 0);

        // Calculate the checksum and insert
        checksum.reset();
        checksum.update(checksumView.clear().limit(payloadEndOffset).position(checksumStartOffset));
        final int thisEnvelopeChecksum = (int) checksum.getValue();
        buffer.putInt(currentEnvelopeStart, thisEnvelopeChecksum);
        if (type != EnvelopeType.START_OFFSET) {
            previousChecksum = thisEnvelopeChecksum;
        }

        // Now we're ready to position the buffer to start writing the next envelope.
        buffer.position(payloadEndOffset);
        currentEnvelopeStart = payloadEndOffset;
    }

    private static EnvelopeType completedEnvelopeType(boolean begin, boolean end) {
        if (begin && end) {
            return EnvelopeType.FULL;
        } else if (begin) {
            return EnvelopeType.BEGIN;
        } else if (end) {
            return EnvelopeType.END;
        } else {
            return EnvelopeType.MIDDLE;
        }
    }

    private EnvelopeWriteChannel updateBytesWritten(int count) {
        appendedBytes += count;
        return this;
    }

    private void nextSegmentOnOverflow(int spaceInBytes) throws IOException {
        if ((buffer.position() + spaceInBytes) > nextSegmentOffset) {
            // Add data would overflow the segment, write out the current envelop and continue in next segment
            completeEnvelopeAndGoToNextSegment();
        }
    }

    private void beginNewEnvelope() {
        currentEnvelopeStart = buffer.position();
        buffer.position(currentEnvelopeStart + LogEnvelopeHeader.HEADER_SIZE);
    }

    private void padSegmentAndGoToNext() throws IOException {
        int position = buffer.position();
        if (position < nextSegmentOffset) {
            buffer.put(PADDING_ZEROES, 0, nextSegmentOffset - position);
        }
        assert buffer.position() == nextSegmentOffset;

        currentEnvelopeStart = nextSegmentOffset;

        if (currentEnvelopeStart == buffer.capacity()
                || (channel.position() + currentEnvelopeStart - lastWrittenPosition) == rotateAtSize) {
            prepareForFlush();
        } else {
            nextSegmentOffset += segmentBlockSize;
        }
    }

    private void rotateLogFile() throws IOException {
        try (var logAppendEvent = databaseTracer.logAppend()) {
            logRotation.rotateLogFile(logAppendEvent);
        }
    }

    private void handleClosedChannelException(ClosedChannelException e) throws ClosedChannelException {
        // We don't want to check the closed flag every time we empty, instead we can avoid unnecessary the
        // volatile read and catch ClosedChannelException where we see if the channel being closed was
        // deliberate or not. If it was deliberately closed then throw IllegalStateException instead so
        // that callers won't treat this as a kernel panic.
        checkChannelClosed(e);

        // OK, this channel was closed without us really knowing about it, throw exception as is.
        throw e;
    }

    private void checkChannelClosed(ClosedChannelException e) throws IllegalStateException {
        if (closed) {
            throw new IllegalStateException("This log channel has been closed", e);
        }
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        int remaining = src.remaining();
        putAll(src);
        return remaining;
    }

    private int currentPayloadLength() {
        return buffer.position() - (currentEnvelopeStart + LogEnvelopeHeader.HEADER_SIZE);
    }

    /**
     * Writes a START_OFFSET envelope to the current segment, which will shift all
     * following envelopes by `size` bytes. This can be used to align the following
     * envelopes if necessary while replicating envelopes from other machines.
     * <p>
     * This method can only be called to insert an offset envelope at the beginning
     * of a segment and cannot be called while writing of another envelope (so, make
     * sure to close the current envelope with {@link #endCurrentEntry()}/{@link #putChecksum()}
     * before calling this method.
     *
     * @param size must be at least the length of one envelope header (see {@link LogEnvelopeHeader#HEADER_SIZE})
     *             and must leave enough space for another envelope to be added after it in the current segment.
     */
    public void insertStartOffset(int size) throws IOException {
        checkArgument(size >= HEADER_SIZE, ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL, HEADER_SIZE);
        checkArgument(
                size < segmentBlockSize - HEADER_SIZE, ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE, segmentBlockSize);
        checkState(
                currentEnvelopeStart == 0 && channel.position() == segmentBlockSize,
                ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT);
        checkState(
                (currentEnvelopeStart + HEADER_SIZE) == buffer.position(),
                ERROR_MSG_TEMPLATE_OFFSET_MUST_NOT_BE_INSIDE_ANOTHER_ENVELOPE);

        final int payloadLength = size - HEADER_SIZE;
        put(new byte[payloadLength], payloadLength);
        writeHeader(EnvelopeType.START_OFFSET, payloadLength);
        prepareNextEnvelope();
    }
}
