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

import static java.lang.Math.min;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.ReadableChannel.UNSPECIFIED_CONTENT_TYPE;
import static org.neo4j.storageengine.api.LogVersionRepository.UNKNOWN_LOG_OFFSET;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireMultipleOf;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;
import static org.neo4j.wal.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.IGNORE_CONTENT_VERSION;
import static org.neo4j.wal.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_APPEND_INDEX;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_CONTENT_TYPE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_ENVELOPE_TYPE;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_KERNEL_VERSION;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PAYLOAD_LENGTH;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_PREVIOUS_CHECKSUM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.OFFSET_TERM;
import static org.neo4j.wal.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.zip.Checksum;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.PhysicalLogChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.wal.LogTracers;
import org.neo4j.wal.entry.LogEnvelopeHeader;
import org.neo4j.wal.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.wal.rotation.LogRotation;

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
        checkState(begin, "Cannot start a new envelope: an entry with index %s is still open.", currentIndex);
        checkNoBufferedEnvelope("Cannot start a new envelope");
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
     * Appends one whole raft entry, shipped verbatim from another member as the envelopes its log already holds:
     * {@code chunks} must together carry every envelope of exactly one entry (empty chunks are ignored). Only entry
     * envelopes ship: a START_OFFSET filler belongs to the local file it was rotated or truncated into, not to the
     * envelope chain the members share, so it is never valid input here. Unlike
     * {@link #directPutAll(ByteBuffer, long)}, which leaves rotation to the caller, this rotates on the size limit
     * like the local append path.
     * <p>
     * Every envelope is verified before its bytes can reach the log, and the channel's tracked state is updated
     * from it, so the channel stays calibrated across raw appends without re-reading the tail from disk.
     * Verification costs one CRC32C pass over the envelope — the same order of work as the copy it accompanies. A corrupt envelope would otherwise be
     * caught only when later read for apply: by then it is durable in the log and may have been shipped onward, so
     * rejecting here keeps the durable log clean.
     * <p>
     * Padding is not always part of the shipped bytes — the padding after a range's last entry, for instance, is
     * never shipped — so the zero bytes the sender's log holds before the next entry may be missing here. Whether
     * they are due cannot be known from the write position alone (a small enough envelope still fits in the tail),
     * so the inbound envelope's header is peeked before each entry starts, and any missing padding is re-created;
     * padding that does arrive in-stream is copied verbatim. Kernel-version consistency within a file is not
     * checked per envelope; the upgrade flow forces a rotation on version changes.
     *
     * @throws ChecksumMismatchException when an envelope's checksum does not match its bytes, or its
     * previous-checksum field does not continue the tracked chain — corrupt shipping or a diverged log.
     * @throws IllegalStateException when the entry is torn (an envelope cut mid-chunk, or a last envelope leaving
     * the entry open), ships no envelopes, disagrees with {@code index}/{@code term}, or is misaligned to this
     * log's segment grid. Only verified bytes are ever flushed, but an entry crossing a segment boundary can
     * leave earlier envelopes durable before a later one is rejected; recovery truncates that torn tail.
     */
    public PhysicalLogChannel appendRaw(ByteBuffer[] chunks, long index, long term) throws IOException {
        checkNoBufferedEnvelope("Cannot append raw envelopes");
        boolean sawEnvelope = false;
        boolean stateSeeded = false;
        for (ByteBuffer src : chunks) {
            if (!src.hasRemaining()) {
                // with nothing to peek, the padding logic below would mistake a short tail for a stripped
                // padding run and pad (possibly rotate) on thin air
                continue;
            }
            // Shipped buffers arrive big-endian (ByteBuffer.wrap default), but the envelope header ints we peek were
            // written in the log's byte order; read them as such. The bulk byte copies below are order-independent.
            src.order(buffer.order());
            padAndRotateBeforeRawEnvelope(src, index);
            // parse watermark: raw bytes below this buffer offset have been parsed and mirrored into channel state
            int rawParsedOffset = buffer.position();
            if (!stateSeeded) {
                // assigned after the first chunk's pre-write rotation check, so a rotation at the entry start
                // carries the previous entry's index and term
                currentIndex = index;
                currentTerm = term;
                nextTerm = term;
                stateSeeded = true;
            }
            final int length = src.remaining();
            final int srcEnd = src.position() + length;
            int srcIndex = src.position();
            while (srcIndex < srcEnd) {
                int payloadChunk = min(srcEnd - srcIndex, nextSegmentOffset - buffer.position());
                buffer.put(buffer.position(), src, srcIndex, payloadChunk);
                buffer.position(buffer.position() + payloadChunk);
                srcIndex += payloadChunk;
                if (srcIndex != srcEnd) {
                    // the buffer window may wrap below, so this window's envelopes must be parsed while still in it
                    var parsed = parseRawEnvelopes(rawParsedOffset);
                    sawEnvelope |= parsed.sawEnvelope();
                    // the payload-length bounds keep every envelope inside its own segment, so a walk that got
                    // this far cannot stall short of the boundary it reached
                    assert parsed.watermark() == buffer.position();
                    padSegmentAndGoToNext();
                    rawParsedOffset = buffer.position();
                }
            }
            var parsed = parseRawEnvelopes(rawParsedOffset);
            rawParsedOffset = parsed.watermark();
            sawEnvelope |= parsed.sawEnvelope();
            if (rawParsedOffset != buffer.position()) {
                throw new IllegalStateException(
                        ("Raw appended entry with index %s is torn: a chunk ended with %s bytes of an incomplete "
                                        + "envelope at file position %s. Every envelope must be appended whole within "
                                        + "one chunk.")
                                .formatted(index, buffer.position() - rawParsedOffset, filePosition(rawParsedOffset)));
            }
            appendedBytes += length;
        }
        // the caller has already counted this entry as appended; accepting an envelope-less entry would surface
        // the divergence one entry later
        checkState(
                sawEnvelope,
                "Raw appended entry with index %s and term %s contains no envelopes: an entry must ship at least "
                        + "one envelope.",
                index,
                term);
        checkState(
                begin,
                "Raw appended entry with index %s is torn: its last envelope leaves the entry open. "
                        + "All envelopes of an entry must be appended in one call.",
                index);
        currentEnvelopeStart = buffer.position();
        return this;
    }

    /**
     * Parses the raw bytes appended between {@code rawParsedOffset} and the buffer position, mirroring channel
     * state from every completed envelope and skipping segment-tail padding.
     *
     * @return the new watermark — the buffer offset up to which the appended bytes have been parsed — and
     * whether this walk parsed any envelope.
     */
    private RawParse parseRawEnvelopes(int rawParsedOffset) throws IOException {
        final int parseEnd = buffer.position();
        boolean sawEnvelope = false;
        while (rawParsedOffset < parseEnd) {
            final int segmentEnd = (rawParsedOffset / segmentBlockSize + 1) * segmentBlockSize;
            final int available = parseEnd - rawParsedOffset;
            if (segmentEnd - rawParsedOffset <= HEADER_SIZE) {
                // too close to the boundary to fit any envelope: must be a padding run
                rawParsedOffset = skipRawPadding(rawParsedOffset, min(segmentEnd, parseEnd));
                continue;
            }
            if (available < OFFSET_ENVELOPE_TYPE + Byte.BYTES) {
                return new RawParse(rawParsedOffset, sawEnvelope); // cannot tell an envelope from a padding run yet
            }
            final byte type = buffer.get(rawParsedOffset + OFFSET_ENVELOPE_TYPE);
            if (type == EnvelopeType.ZERO.typeValue) {
                if (segmentEnd - rawParsedOffset > MAX_ZERO_PADDING_SIZE) {
                    throw new IllegalStateException(
                            ("Raw append of entry with index %s found a zero envelope type byte at file position %s, "
                                            + "%s bytes before the segment boundary: zero padding is only valid "
                                            + "within %s bytes of it.")
                                    .formatted(
                                            currentIndex,
                                            filePosition(rawParsedOffset),
                                            segmentEnd - rawParsedOffset,
                                            MAX_ZERO_PADDING_SIZE));
                }
                rawParsedOffset = skipRawPadding(rawParsedOffset, min(segmentEnd, parseEnd));
                continue;
            }
            if (!isRawEnvelopeType(type)) {
                throw new IllegalStateException(
                        ("Raw append of entry with index %s expected an envelope at file position %s but found envelope "
                                        + "type %s: only FULL, BEGIN, MIDDLE and END envelopes can be raw appended.")
                                .formatted(currentIndex, filePosition(rawParsedOffset), type));
            }
            if (available < OFFSET_PAYLOAD_LENGTH + Integer.BYTES) {
                return new RawParse(rawParsedOffset, sawEnvelope); // payload length not appended yet
            }
            final int payloadLength = buffer.getInt(rawParsedOffset + OFFSET_PAYLOAD_LENGTH);
            final int spaceLeftInSegment = segmentEnd - rawParsedOffset - HEADER_SIZE;
            if (payloadLength <= 0 || payloadLength > spaceLeftInSegment) {
                throw new IllegalStateException(
                        ("Raw appended envelope at file position %s (index %s) has payload length %s, "
                                        + "outside (0, %s], the payload space left in its segment.")
                                .formatted(
                                        filePosition(rawParsedOffset),
                                        currentIndex,
                                        payloadLength,
                                        spaceLeftInSegment));
            }
            final int envelopeEnd = rawParsedOffset + HEADER_SIZE + payloadLength;
            if (envelopeEnd > parseEnd) {
                return new RawParse(rawParsedOffset, sawEnvelope); // incomplete envelope
            }
            parseRawEnvelope(rawParsedOffset, envelopeEnd, type);
            sawEnvelope = true;
            rawParsedOffset = envelopeEnd;
        }
        return new RawParse(rawParsedOffset, sawEnvelope);
    }

    private void parseRawEnvelope(int envelopeStart, int envelopeEnd, byte type) throws IOException {
        final int computedChecksum = calculateChecksum(envelopeEnd, envelopeStart + OFFSET_ENVELOPE_TYPE);
        final int envelopeChecksum = buffer.getInt(envelopeStart + OFFSET_CHECKSUM);
        if (envelopeChecksum != computedChecksum) {
            throw new ChecksumMismatchException(
                    "Raw appended envelope at file position " + filePosition(envelopeStart) + " (index " + currentIndex
                            + ", term " + currentTerm + "): the stored checksum '%d' does not match the calculated "
                            + "checksum of '%d'.",
                    envelopeChecksum,
                    computedChecksum);
        }
        final int envelopePreviousChecksum = buffer.getInt(envelopeStart + OFFSET_PREVIOUS_CHECKSUM);
        if (envelopePreviousChecksum != previousChecksum) {
            throw new ChecksumMismatchException(
                    "Raw appended envelope at file position " + filePosition(envelopeStart) + " (index " + currentIndex
                            + ", term " + currentTerm
                            + "): Envelope checksum chain is broken. Previous checksum field: %d, "
                            + "expected: %d.",
                    envelopePreviousChecksum,
                    previousChecksum);
        }
        final long envelopeIndex = buffer.getLong(envelopeStart + OFFSET_APPEND_INDEX);
        if (envelopeIndex != currentIndex) {
            throw new IllegalStateException(
                    "Raw appended envelope at file position %s has index %s but the append was called with index %s"
                            .formatted(filePosition(envelopeStart), envelopeIndex, currentIndex));
        }
        final long envelopeTerm = buffer.getLong(envelopeStart + OFFSET_TERM);
        if (envelopeTerm != currentTerm) {
            throw new IllegalStateException(
                    ("Raw appended envelope at file position %s (index %s) has term %s but the append was called "
                                    + "with term %s")
                            .formatted(filePosition(envelopeStart), currentIndex, envelopeTerm, currentTerm));
        }

        previousChecksum = envelopeChecksum;
        currentVersion = buffer.get(envelopeStart + OFFSET_KERNEL_VERSION);
        currentContentType = buffer.get(envelopeStart + OFFSET_CONTENT_TYPE);
        begin = type == EnvelopeType.FULL.typeValue || type == EnvelopeType.END.typeValue;
    }

    private record RawParse(int watermark, boolean sawEnvelope) {}

    private int skipRawPadding(int paddingStart, int paddingEnd) throws IOException {
        for (int i = paddingStart; i < paddingEnd; i++) {
            final byte padByte = buffer.get(i);
            if (padByte != 0) {
                throw new IllegalStateException(
                        ("Raw append of entry with index %s expected zero padding up to the segment boundary but "
                                        + "found non-zero byte %s at file position %s.")
                                .formatted(currentIndex, padByte, filePosition(i)));
            }
        }
        return paddingEnd;
    }

    private void checkNoBufferedEnvelope(String action) throws IOException {
        if (buffer.position() != currentEnvelopeStart) {
            throw new IllegalStateException(
                    ("%s: an entry is still open with %s bytes pending after the last completed envelope at file "
                                    + "position %s.")
                            .formatted(
                                    action,
                                    buffer.position() - currentEnvelopeStart,
                                    filePosition(currentEnvelopeStart)));
        }
    }

    /**
     * Maps a buffer-window offset to its position in the log file. Only for failure messages: the mapping reads
     * {@code channel.position()}, a syscall too hot for eager checkState arguments, which is why the raw-append
     * rejections use explicit if-throws.
     */
    private long filePosition(int bufferOffset) throws IOException {
        return channel.position() - lastWrittenPosition + bufferOffset;
    }

    /**
     * Pads the current segment (and rotates on the size limit) before a shipped envelope that would not fit in the bytes left to the boundary, folding in the
     * plain "no room for a header" check.
     * <p>
     * It is not possible to know if padding is needed without peeking, this is because padding depends on what content was written after the header and
     * therefor peeking is necessary.
     */
    private void padAndRotateBeforeRawEnvelope(ByteBuffer src, long index) throws IOException {
        int envelopeStart = src.position();
        int tail = nextSegmentOffset - buffer.position();
        if (tail >= segmentBlockSize) {
            return; // already on a fresh segment boundary
        }
        boolean fits;
        if (src.remaining() >= HEADER_SIZE && isRawEntryStart(src.get(envelopeStart + OFFSET_ENVELOPE_TYPE))) {
            final int payloadLength = src.getInt(envelopeStart + OFFSET_PAYLOAD_LENGTH);
            final int segmentPayloadSpace = segmentBlockSize - HEADER_SIZE;
            // the parser's per-position bound only sees this length after the copy, by which point a garbage
            // length has already decided the padding below, and padding can rotate
            checkState(
                    payloadLength > 0 && payloadLength <= segmentPayloadSpace,
                    "Raw append of entry with index %d peeked payload length %d in the shipped envelope header, "
                            + "outside (0, %d], the payload space of a segment.",
                    index,
                    payloadLength,
                    segmentPayloadSpace);
            final int envelopeLength = HEADER_SIZE + payloadLength;
            checkState(
                    envelopeLength <= tail || tail <= MAX_ZERO_PADDING_SIZE,
                    "Raw append of entry with index %d would pad a %d-byte tail before a %d-byte envelope, but a "
                            + "tail larger than the max zero-padding size (%d) cannot be padding; the shipped bytes "
                            + "are misaligned to the segment grid.",
                    index,
                    tail,
                    envelopeLength,
                    MAX_ZERO_PADDING_SIZE);
            fits = envelopeLength <= tail;
        } else {
            // MIDDLE/END continuation, or a sub-header chunk: only advance the grid when on a boundary.
            fits = tail > HEADER_SIZE;
        }
        if (!fits) {
            padSegmentAndGoToNext();
        }
    }

    private static boolean isRawEntryStart(byte type) {
        return type == EnvelopeType.FULL.typeValue || type == EnvelopeType.BEGIN.typeValue;
    }

    private static boolean isRawEnvelopeType(byte type) {
        return type == EnvelopeType.FULL.typeValue
                || type == EnvelopeType.BEGIN.typeValue
                || type == EnvelopeType.MIDDLE.typeValue
                || type == EnvelopeType.END.typeValue;
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

    @Override
    public EnvelopeWriteChannel putTerm(long term) {
        checkState(
                currentTerm <= term,
                "Failed to write entry to replication log, the entry's term was less than the "
                        + "last appended term, terms must be monotonically increasing. [lastTermAppended=%d, entryTerm=%d]",
                currentTerm,
                term);
        this.nextTerm = term;
        return this;
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
        checkState(
                lastWrittenPosition == currentEnvelopeStart,
                "Buffered envelopes must be flushed before truncating to %d. The rotation below flushes, which would "
                        + "write buffered data [%d,%d) back into the file being shortened, re-extending it past the "
                        + "truncation point and overwriting bytes concurrent readers may already be shipping.",
                position,
                lastWrittenPosition,
                currentEnvelopeStart);
        this.previousChecksum = previousChecksum;
        this.currentIndex = previousIndex;
        this.currentTerm = previousTerm;
        // the reseed state describes a complete entry: any half-appended raw entry is truncated away
        this.begin = true;
        channel.truncate(position);
        // After truncation, it's critical that subsequent writes go to a completely different file rather than
        // overwriting the sections of an existing file. This is because there might be readers in other threads
        // reading the existing files, for example, Raft binary log shipping reads asynchronously. Overwriting
        // data after a truncation could silently ship a mix of old and new data, whereas shortening a file
        // will just ship less data.
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
        // the reseed state describes a complete entry: no entry can be open across a recovery
        this.begin = true;
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
        final int checksumStartOffset = currentEnvelopeStart + OFFSET_ENVELOPE_TYPE;
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

    @VisibleForTesting
    byte currentVersion() {
        return currentVersion;
    }

    @VisibleForTesting
    byte currentContentType() {
        return currentContentType;
    }

    public long currentTerm() {
        return currentTerm;
    }
}
