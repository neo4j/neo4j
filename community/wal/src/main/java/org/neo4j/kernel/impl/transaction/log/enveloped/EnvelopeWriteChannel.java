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

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.IGNORE_CONTENT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;
import static org.neo4j.storageengine.api.LogVersionRepository.UNKNOWN_LOG_OFFSET;
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
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.LogTracers;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.rotation.LogRotation;
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
            "offset size (%d) must be at least envelope header size (%d).";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE =
            "offset size (%d) cannot be bigger than the segment size (%d) and must leave enough space for at least one "
                    + "envelope after it.";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT =
            "START_OFFSET envelopes can only be inserted at the start of the first segment";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_MUST_NOT_BE_INSIDE_ANOTHER_ENVELOPE =
            "START_OFFSET cannot be inserted while another envelope is still open. Close the current entry first.";

    @VisibleForTesting
    static final String ERROR_MSG_TEMPLATE_OFFSET_NOT_CONSISTENT =
            "The provided offset is aligned on a segment, but the existing channel is at a different offset";

    private static final byte[] PADDING_ZEROES = new byte[MAX_ZERO_PADDING_SIZE];

    // Offsets of fields within an envelope frame header (matches completeEnvelope's write order / HEADER_SIZE).
    private static final int ENVELOPE_TYPE_OFFSET = Integer.BYTES; // type byte follows the 4-byte checksum
    private static final int ENVELOPE_PAYLOAD_LENGTH_OFFSET = ENVELOPE_TYPE_OFFSET + Byte.BYTES; // length follows type
    private static final int ENVELOPE_PREVIOUS_CHECKSUM_OFFSET = Integer.BYTES
            + Byte.BYTES
            + Integer.BYTES
            + Long.BYTES
            + Byte.BYTES; // past checksum,type,length,index,version

    private final Checksum checksum = CHECKSUM_FACTORY.get();
    private final ScopedBuffer scopedBuffer;
    private final LogRotation logRotation;
    private final LogTracers logTracers;
    private final ByteBuffer buffer;
    private final ByteBuffer checksumView;
    private final int segmentBlockSize;

    private StoreChannel channel;
    private int currentEnvelopeStart;
    private byte currentVersion = IGNORE_CONTENT_VERSION;
    private byte currentContentType = UNSPECIFIED_CONTENT_TYPE;
    // The index of the current entry. See LogEnvelopeHeader.index.
    private long currentIndex;
    private long currentTerm;
    private long nextTerm;

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
            long initialTerm,
            LogTracers logTracers,
            LogRotation logRotation)
            throws IOException {
        this.channel = requireNonNull(channel);
        this.scopedBuffer = requireNonNull(scopedBuffer);
        this.previousChecksum = initialChecksum;
        requirePowerOfTwo(segmentBlockSize);
        this.segmentBlockSize = segmentBlockSize;
        this.logRotation = requireNonNull(logRotation);
        this.logTracers = requireNonNull(logTracers);
        this.buffer = scopedBuffer.getBuffer();
        this.checksumView = buffer.duplicate().order(buffer.order());
        this.currentIndex = currentIndex;
        this.currentTerm = initialTerm;
        this.nextTerm = initialTerm;
        requireMultipleOf("Buffer", buffer.capacity(), "segment block size", segmentBlockSize);

        initialPositions(channel.position());
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

    public int currentChecksum() {
        return previousChecksum;
    }

    public void endCurrentEntry() {
        checkState(currentPayloadLength() > 0, "Closing empty envelope is not allowed.");
        completeEnvelope(true);
        currentContentType = UNSPECIFIED_CONTENT_TYPE; // expected to be set for every new entry.
    }

    public void prepareNextEnvelope() throws IOException {
        prepareWrite();
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
        checkState(
                channel.position() == segmentBlockSize,
                "The set channel must be positioned on first segment at %d, but was at %d",
                segmentBlockSize,
                channel.position());
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
                buffer.position() == currentEnvelopeStart,
                "position() must be called right after endCurrentEntry() at offset %d but positioned at %d",
                currentEnvelopeStart,
                channel.position());

        long bufferViewStart = channel.position() - lastWrittenPosition;
        return bufferViewStart + currentEnvelopeStart;
    }

    @Override
    public void beginChecksumForWriting() throws IOException {
        prepareNextEnvelope();
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

        if (currentEnvelopeStart >= buffer.capacity()) {
            // Buffer is exhausted, reset and start over
            buffer.clear();
            lastWrittenPosition = 0;
            currentEnvelopeStart = 0;
            nextSegmentOffset = 0; // Updated from padSegmentAndGoToNext
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

    /**
     * Writes all remaining data in the {@link ByteBuffer} to this channel.
     * The channel will handle chunking the data into envelopes if needed.
     *
     * @param src buffer with data to write to this channel.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    @Override
    public EnvelopeWriteChannel putAll(ByteBuffer src) throws IOException {
        int length = src.remaining();
        int srcIndex = src.position();
        int srcLimit = src.limit();
        while (srcIndex < srcLimit) {
            int remainingPayloadSpace = nextSegmentOffset - buffer.position();
            int payloadChunk = min(srcLimit - srcIndex, remainingPayloadSpace);
            buffer.put(buffer.position(), src, srcIndex, payloadChunk);
            buffer.position(buffer.position() + payloadChunk);
            srcIndex += payloadChunk;

            if (srcIndex != srcLimit) {
                // Still have data left to put
                completeEnvelopeAndGoToNextSegment();
            }
        }
        appendedBytes += length;
        return this;
    }

    /**
     * Writes all remaining data in the {@link ByteBuffer} to this channel overriding the channel's envelope chunking.
     * The data in the buffer must be already finished envelopes from another source, and it must be put on the same
     * offset within a segment for the envelope boundaries to become correct.
     * The method takes care of inserting a start offset envelope if necessary.
     * This method does not handle any file rotation, it must be done externally before this call if necessary.
     *
     * @param src buffer with data to write to this channel.
     * @param offset offset of the data on the origin. Will be used to figure out the offset to start on within the
     *               segment. -1 if the data should be put directly after previously written data.
     * @return this channel, for fluent usage.
     * @throws IOException if I/O error occurs.
     */
    @Override
    public PhysicalLogChannel directPutAll(ByteBuffer src, long offset) throws IOException {
        // Some magic to get around the buffer already being positioned one header after any previous envelope.
        if (offset != UNKNOWN_LOG_OFFSET) {
            int offsetIntoSegment =
                    (int) (offset & (segmentBlockSize - 1)); // segmentBlockSize is guaranteed power of 2
            // Should write a start offset envelope if there is not already data up to the offset
            if (offsetIntoSegment != 0 && (currentEnvelopeStart % segmentBlockSize != offsetIntoSegment)) {
                insertStartOffset(offsetIntoSegment);
            }
            // provided offset on segment boundary
            if (offsetIntoSegment == 0) {
                // writer about to zero pad, so add missing padding, or throw if state doesn't match
                int currentStartOffsetIntoSegment = currentEnvelopeStart % segmentBlockSize;
                if (currentStartOffsetIntoSegment >= (segmentBlockSize - HEADER_SIZE)) {
                    padSegmentAndGoToNext(false);
                } else if (currentStartOffsetIntoSegment != 0) {
                    throw new IllegalStateException(ERROR_MSG_TEMPLATE_OFFSET_NOT_CONSISTENT);
                }
            }
        }

        int length = src.remaining();
        int srcIndex = src.position();
        int srcEnd = srcIndex + length;
        while (srcIndex < srcEnd) {
            int remainingPayloadSpace = nextSegmentOffset - buffer.position();
            int payloadChunk = min(srcEnd - srcIndex, remainingPayloadSpace);
            buffer.put(buffer.position(), src, srcIndex, payloadChunk);
            buffer.position(buffer.position() + payloadChunk);
            srcIndex += payloadChunk;

            if (srcIndex != srcEnd) {
                // Still have data left to put. Make sure we flush if buffer is full
                padSegmentAndGoToNext(false);
            }
        }
        appendedBytes += length;
        // Update envelope start so that everything we have written will be flushed on next flush call
        currentEnvelopeStart = buffer.position();
        return this;
    }

    /**
     * Appends raw, already-enveloped bytes (shipped verbatim from another member) at the current position and, unlike
     * {@link #directPutAll(ByteBuffer, long)} which leaves rotation to the caller, rotates on {@link #rotateAtSize}
     * like the normal append path. Expects buffers are not cut in middle of header so it's always possible to peek
     * when rotation is needed.
     * <p>
     * {@code index}/{@code term} are supplied by the caller; the rest of the state a rotated file's
     * header needs — the chain's previous checksum and whether the boundary splits an entry — is peeked from the
     * next frame's header when, and only when, a rotation is actually due.
     * <p>
     * NOTE: This means that the write channel is still in a semi-detached state when this method is used since its
     * only keeping track of index and term
     */
    public PhysicalLogChannel appendRaw(ByteBuffer src, long index, long term) throws IOException {
        // Shipped buffers arrive big-endian (ByteBuffer.wrap default), but the envelope header ints we peek were
        // written in the log's byte order; read them as such. The bulk byte copies below are order-independent.
        src.order(buffer.order());
        padAndRotateBeforeRawFrame(src);
        currentIndex = index;
        currentTerm = term;
        nextTerm = term;
        final int length = src.remaining();
        final int srcEnd = src.position() + length;
        int srcIndex = src.position();
        while (srcIndex < srcEnd) {
            int payloadChunk = min(srcEnd - srcIndex, nextSegmentOffset - buffer.position());
            buffer.put(buffer.position(), src, srcIndex, payloadChunk);
            buffer.position(buffer.position() + payloadChunk);
            srcIndex += payloadChunk;
            if (srcIndex != srcEnd) {
                // A boundary reached mid-buffer must fall between frames: src[srcIndex] has to start a new frame.
                // Otherwise the shipped bytes are misaligned to our segment grid and we would write a frame
                // straddling the boundary, corrupting the log (invalid envelope type when later read at the segment).
                checkRawFrameStart(src, srcIndex);
                padSegmentAndGoToNext(false);
                rotateRawIfLimitReached(src, srcIndex);
            }
        }
        appendedBytes += length;
        currentEnvelopeStart = buffer.position();
        return this;
    }

    /**
     * Pads the current segment (and rotates on the size limit) before a shipped frame that would not fit in the bytes left to the boundary, folding in the
     * plain "no room for a header" check.
     * <p>
     * It is not possible to know if padding is needed without peeking, this is because padding depends on what content was written after the header and
     * therefor peeking is necessary.
     */
    private void padAndRotateBeforeRawFrame(ByteBuffer src) throws IOException {
        int frameStart = src.position();
        int tail = nextSegmentOffset - buffer.position();
        if (tail >= segmentBlockSize) {
            return; // already on a fresh segment boundary
        }
        boolean fits;
        if (src.remaining() >= HEADER_SIZE && isRawEntryStart(src.get(frameStart + ENVELOPE_TYPE_OFFSET))) {
            final int frameLength = HEADER_SIZE + src.getInt(frameStart + ENVELOPE_PAYLOAD_LENGTH_OFFSET);
            checkState(
                    frameLength <= tail || tail <= MAX_ZERO_PADDING_SIZE,
                    "Raw append would pad a %d-byte tail before a %d-byte frame, but a tail larger than the max "
                            + "zero-padding size (%d) cannot be padding; the shipped bytes are misaligned to the "
                            + "segment grid.",
                    tail,
                    frameLength,
                    MAX_ZERO_PADDING_SIZE);
            fits = frameLength <= tail;
        } else {
            // MIDDLE/END continuation, or a sub-header chunk: only advance the grid when on a boundary.
            fits = tail > HEADER_SIZE;
        }
        if (!fits) {
            padSegmentAndGoToNext(false);
            rotateRawIfLimitReached(src, frameStart);
        }
    }

    private static boolean isRawEntryStart(byte type) {
        return type == EnvelopeType.FULL.typeValue || type == EnvelopeType.BEGIN.typeValue;
    }

    /**
     * Peeks the checksum from the next header to perform a rotation. Expects index and term to be set correct.
     * {@code src}'s byte order is set to the log's in {@link #appendRaw}, so the absolute read uses the right order.
     */
    private void rotateRawIfLimitReached(ByteBuffer src, int nextHeaderOffset) throws IOException {
        if (channel.position() < rotateAtSize) {
            return;
        }
        previousChecksum = src.getInt(nextHeaderOffset + ENVELOPE_PREVIOUS_CHECKSUM_OFFSET);
        rotateLogFile();
    }

    /**
     * Tripwire for {@link #appendRaw}: a segment boundary reached part-way through {@code src} must land on a frame
     * header, not inside a frame or in padding. A frame may never straddle a segment boundary (entries that span
     * segments do so as separate BEGIN/MIDDLE/END frames), so the type byte at {@code frameStart} must be one of the
     * real frame types. Anything else means the shipped bytes are misaligned to this log's segment grid and we are
     * about to corrupt it.
     */
    private static void checkRawFrameStart(ByteBuffer src, int frameStart) {
        byte typeValue = src.get(frameStart + ENVELOPE_TYPE_OFFSET);
        boolean atFrameStart = typeValue == EnvelopeType.FULL.typeValue
                || typeValue == EnvelopeType.BEGIN.typeValue
                || typeValue == EnvelopeType.MIDDLE.typeValue
                || typeValue == EnvelopeType.END.typeValue;
        checkState(
                atFrameStart,
                "Raw append reached a segment boundary in the middle of a frame (envelope type byte %d at buffer "
                        + "offset %d); the shipped bytes are misaligned to this log's segment grid.",
                typeValue,
                frameStart);
    }

    @Override
    public boolean handlesRotationInternally() {
        return true;
    }

    @Override
    public void prepareWrite() throws IOException {
        if ((buffer.position() + LogEnvelopeHeader.HEADER_SIZE) >= nextSegmentOffset) {
            padSegmentAndGoToNext();
        }
    }

    public void putTerm(long term) {
        checkState(
                currentTerm <= term,
                "Failed to write entry to replication log, the entry's term was less than the "
                        + "last appended term, terms must be monotonically increasing. [lastTermAppended=%d, entryTerm=%d]",
                currentTerm,
                term);
        this.nextTerm = term;
    }

    @Override
    public EnvelopeWriteChannel putContentType(byte contentType) {
        assert contentType >= 0;
        this.currentContentType = contentType;
        return this;
    }

    @Override
    public WritableChannel putAppendIndex(long appendIndex) {
        // Tracked internally but this method still exist because of the old format.
        // Indexes need to match otherwise there could be some very weird errors with
        // saved positions for append indexes.
        // Let's treat it as an error if they don't
        // An offset for currentIndex not being increased until first header of a tx/chunk is written
        if (appendIndex != currentIndex + (begin ? 1 : 0)) {
            throw new IllegalStateException("Append index %s should be the same as current index %s"
                    .formatted(appendIndex, currentIndex + (begin ? 1 : 0)));
        }
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

    public void truncateToPosition(long position, int previousChecksum, long previousIndex, long previousTerm)
            throws IOException {
        requireNonNegative(position);
        checkArgument(position <= channel.position(), "Can only truncate written data.");
        checkArgument(position >= segmentBlockSize, "Truncating the first segment is not possible");
        this.previousChecksum = previousChecksum;
        this.currentIndex = previousIndex;
        this.currentTerm = previousTerm;
        channel.truncate(position);
        rotateLogFile();
    }

    /**
     * Updates the internal state (checksum, index, term) of the write channel without truncating.
     * This is used for recovery after directPutAll operations.
     *
     * @param previousChecksum the checksum of the last written envelope
     * @param index the index of the last written entry
     * @param term the term of the last written entry
     */
    public void recoverState(int previousChecksum, long index, long term) {
        this.previousChecksum = previousChecksum;
        this.currentIndex = index;
        this.currentTerm = term;
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
    private void initialPositions(long initialPosition) throws IOException {
        int bufferWindowOffset = (int) (initialPosition % buffer.capacity());
        currentEnvelopeStart = bufferWindowOffset;
        lastWrittenPosition = bufferWindowOffset;
        nextSegmentOffset = (bufferWindowOffset / segmentBlockSize + 1) * segmentBlockSize; // Round up to next
        rotateAtSize = logRotation.rotationSize();
        requireMultipleOf("Rotation size", rotateAtSize, "segment size", segmentBlockSize);
        if (rotateAtSize == 0) {
            rotateAtSize = Long.MAX_VALUE; // Rotation disabled
        }
        buffer.clear().position(bufferWindowOffset);

        // Handle the case where we started up on a channel that is already at rotation size.
        // Make sure it is on a segment boundary though, otherwise it needs to wait until it hits the next boundary
        long channelPos = channel.position();
        if (channelPos >= rotateAtSize && currentEnvelopeStart == (nextSegmentOffset - segmentBlockSize)) {
            // Setting nextSegmentOffset to current position to trigger rotation on the first envelope
            nextSegmentOffset = currentEnvelopeStart;
        }
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
    }

    private void writeHeader(EnvelopeType type, int payloadLength) {
        final int payloadEndOffset = buffer.position();

        if (begin && type != EnvelopeType.START_OFFSET) {
            currentIndex++;
            if (nextTerm != UNSPECIFIED_TERM) {
                currentTerm = nextTerm;
                nextTerm = UNSPECIFIED_TERM;
            }
        }

        // Fill in the header
        final int checksumStartOffset = currentEnvelopeStart + Integer.BYTES;
        buffer.position(checksumStartOffset);

        if (type == EnvelopeType.START_OFFSET) {
            buffer.put(type.typeValue)
                    .putInt(payloadLength)
                    // START_OFFSET envelopes do not have an index, as they are skipped automatically when reading
                    .putLong(0)
                    .put(IGNORE_CONTENT_VERSION)
                    // START_OFFSET do not participate in checksum chain.
                    .putInt(0)
                    .putLong(UNSPECIFIED_TERM)
                    .put(UNSPECIFIED_CONTENT_TYPE);

            // Calculate the checksum and insert
            final int thisEnvelopeChecksum = calculateChecksum(payloadEndOffset, checksumStartOffset);
            buffer.putInt(currentEnvelopeStart, thisEnvelopeChecksum);
            // START_OFFSET do not set previousChecksum as they do not participate in checksum chain
        } else {
            assert currentVersion != IGNORE_CONTENT_VERSION;
            assert currentContentType != UNSPECIFIED_CONTENT_TYPE;
            buffer.put(type.typeValue)
                    .putInt(payloadLength)
                    .putLong(currentIndex)
                    .put(currentVersion)
                    .putInt(previousChecksum)
                    .putLong(currentTerm)
                    .put(currentContentType);

            // Calculate the checksum and insert
            final int thisEnvelopeChecksum = calculateChecksum(payloadEndOffset, checksumStartOffset);
            buffer.putInt(currentEnvelopeStart, thisEnvelopeChecksum);
            // Non START_OFFSET envelopes set previous checksum
            previousChecksum = thisEnvelopeChecksum;
        }

        // Now we're ready to position the buffer to start writing the next envelope.
        buffer.position(payloadEndOffset);
        currentEnvelopeStart = payloadEndOffset;
    }

    private int calculateChecksum(int payloadEndOffset, int checksumStartOffset) {
        checksum.reset();
        checksum.update(checksumView.clear().limit(payloadEndOffset).position(checksumStartOffset));
        return (int) checksum.getValue();
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

    private void rotateIfLimitReached() throws IOException {
        if (channel.position() >= rotateAtSize) {
            rotateLogFile();
            // NOTE! 'channel' will be updated by 'setChannel'.
            // 'setChannel' will also update buffer and positions.
        }
    }

    private void beginNewEnvelope() {
        currentEnvelopeStart = buffer.position();
        buffer.position(currentEnvelopeStart + LogEnvelopeHeader.HEADER_SIZE);
        appendedBytes += LogEnvelopeHeader.HEADER_SIZE;
    }

    private void padSegmentAndGoToNext() throws IOException {
        padSegmentAndGoToNext(true);
    }

    private void padSegmentAndGoToNext(boolean allowRotation) throws IOException {
        int position = buffer.position();
        if (position < nextSegmentOffset) {
            buffer.put(PADDING_ZEROES, 0, nextSegmentOffset - position);
            appendedBytes += nextSegmentOffset - position;
        }
        assert buffer.position() == nextSegmentOffset;

        currentEnvelopeStart = nextSegmentOffset;

        if (currentEnvelopeStart == buffer.capacity()
                || (channel.position() + currentEnvelopeStart - lastWrittenPosition) >= rotateAtSize) {
            prepareForFlush();
        }
        nextSegmentOffset += segmentBlockSize;
        if (allowRotation) {
            rotateIfLimitReached();
        }
    }

    private void rotateLogFile() throws IOException {
        try (var logAppendEvent = logTracers.logAppend()) {
            // use our definition of appendIndex as appendIndexProvider may be pre-incremented
            logRotation.locklessRotateLogFile(logAppendEvent, currentIndex, previousChecksum, currentTerm);
            // and notify the event tracer
            logAppendEvent.setLogRotated(true);
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
        checkArgument(size > HEADER_SIZE, ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_SMALL, size, HEADER_SIZE);
        checkArgument(
                size < segmentBlockSize - HEADER_SIZE,
                ERROR_MSG_TEMPLATE_OFFSET_SIZE_TOO_LARGE,
                size,
                segmentBlockSize);
        checkState(
                (currentEnvelopeStart == 0 || currentEnvelopeStart == segmentBlockSize)
                        && channel.position() == segmentBlockSize,
                ERROR_MSG_TEMPLATE_OFFSET_MUST_BE_FIRST_IN_THE_FIRST_SEGMENT);
        checkState(
                currentEnvelopeStart == buffer.position(),
                ERROR_MSG_TEMPLATE_OFFSET_MUST_NOT_BE_INSIDE_ANOTHER_ENVELOPE);

        final int payloadLength = size - HEADER_SIZE;
        buffer.position(currentEnvelopeStart + HEADER_SIZE);
        // put will update appendedBytes counter
        put(new byte[payloadLength], payloadLength);
        writeHeader(EnvelopeType.START_OFFSET, payloadLength);
    }

    public long currentIndex() {
        return currentIndex;
    }

    public long currentTerm() {
        return currentTerm;
    }
}
