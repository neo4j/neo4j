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
import static java.nio.ByteOrder.LITTLE_ENDIAN;
import static java.util.Objects.requireNonNull;
import static org.neo4j.io.fs.ChecksumWriter.CHECKSUM_FACTORY;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.HEADER_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.IGNORE_CONTENT_VERSION;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.KERNEL_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.MAX_ZERO_PADDING_SIZE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.REPLICATED_TX_CONTENT_TYPE;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_INDEX;
import static org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.UNSPECIFIED_TERM;
import static org.neo4j.kernel.impl.transaction.log.entry.TailUtils.checkTail;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePowerOfTwo;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.util.Arrays;
import java.util.zip.Checksum;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.LogPosition;
import org.neo4j.kernel.impl.transaction.log.LogPositionMarker;
import org.neo4j.kernel.impl.transaction.log.LogVersionBridge;
import org.neo4j.kernel.impl.transaction.log.LogVersionedStoreChannel;
import org.neo4j.kernel.impl.transaction.log.ReadableLogChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader;
import org.neo4j.kernel.impl.transaction.log.entry.LogEnvelopeHeader.EnvelopeType;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.entry.LogHeader;
import org.neo4j.memory.MemoryTracker;

/**
 * A channel for reading segmented data from a file. All reads are buffer, one segment at a time.
 * Each segment can contain one or more envelopes, with optional padding in the end. The padding
 * is used when there is not enough room left in the segment to fit an envelope with payload size 1.
 * <p>
 * Envelops can be described by the following struct:
 * <pre>
 * envelope {
 *     int checksum;
 *     byte envelopeType;
 *     int payloadLength;
 *     byte version;
 *     int previousChecksum;
 *     byte payload[payloadLength];
 * }
 * </pre>
 *
 * The {@code buffer} position denotes the current read position.
 * The bounds of the current payload is tracked by {@code payloadStartOffset} and {@code payloadEndOffset}.
 * <pre>
 *
 * ... ---> | <---      segmentBlockSize      ---> | <---      segmentBlockSize      ---> | <--- ...
 *          | <---           buffer           ---> |
 *
 *         buffer.position(0)  buffer.position()  buffer.capacity()
 *          ↓                      ↓               ↓
 *          | [envelope] [HHH   payload     ][000] | [           envelope               ] |
 *                           ↑              ↑
 *               payloadStartOffset     payloadEndOffset
 * </pre>
 *
 * @see LogEnvelopeHeader
 * @see EnvelopeType
 * @see EnvelopeWriteChannel
 */
public class EnvelopeReadChannel implements ReadableLogChannel {
    private static final long UNSPECIFIED_SEGMENT = -1;
    private static final byte CHECKSUM_SIZE = Integer.BYTES;
    private static final byte PAYLOAD_CHECKSUM_OFFSET_FROM_START = HEADER_SIZE - CHECKSUM_SIZE;

    private final Checksum checksum = CHECKSUM_FACTORY.get();
    private final LogVersionBridge bridge;
    private final ScopedBuffer scopedBuffer;
    private final boolean raw;
    protected final ByteBuffer buffer;
    private final int segmentBlockSize;
    private final ByteBuffer checksumView;
    private final int segmentShift;
    private final int segmentMask;
    private LogVersionedStoreChannel channel;
    // The log file header of the current file.
    private LogHeader logHeader;
    // In some situations we're not able to enforce the checksum chain, like when we reposition
    // the channel position as we don't know the checksum of the envelope before it.
    private boolean enforceChecksumChain;
    protected int currentChecksum;
    protected int previousChecksum;
    protected long currentSegment;
    protected EnvelopeType payloadType;
    protected long currentIndex = UNSPECIFIED_INDEX;
    protected long currentTerm = UNSPECIFIED_TERM;
    protected byte currentContentType = UNSPECIFIED_CONTENT_TYPE;
    protected byte payloadVersion;
    protected int payloadStartOffset;
    protected int payloadEndOffset;
    private volatile boolean closed;

    public EnvelopeReadChannel(
            LogVersionedStoreChannel startingChannel,
            int segmentBlockSize,
            LogVersionBridge bridge,
            MemoryTracker memoryTracker,
            boolean raw)
            throws IOException {
        this(
                startingChannel,
                segmentBlockSize,
                bridge,
                raw,
                new NativeScopedBuffer(segmentBlockSize, LITTLE_ENDIAN, memoryTracker));
    }

    public EnvelopeReadChannel(
            LogVersionedStoreChannel startingChannel,
            int segmentBlockSize,
            LogVersionBridge bridge,
            boolean raw,
            ScopedBuffer scopedBuffer)
            throws IOException {
        this.channel = requireNonNull(startingChannel);
        requirePowerOfTwo(segmentBlockSize);
        this.segmentBlockSize = segmentBlockSize;
        this.segmentShift = 31 - Integer.numberOfLeadingZeros(segmentBlockSize);
        this.segmentMask = segmentBlockSize - 1;
        this.bridge = requireNonNull(bridge);
        this.raw = raw;

        boolean successfulInitialization = false;
        this.scopedBuffer = scopedBuffer;
        try {
            this.buffer = scopedBuffer.getBuffer();
            this.checksumView = buffer.duplicate().order(buffer.order());

            long startPosition = channel.position();
            readAndValidateFileHeader();
            if (startPosition < segmentBlockSize) {
                startPosition = segmentBlockSize;
            }

            LogPositionMarker positionMarker = new LogPositionMarker();
            positionMarker.mark(channel.getLogVersion(), startPosition);
            setLogPosition(positionMarker);
            successfulInitialization = true;
        } finally {
            if (!successfulInitialization) {
                scopedBuffer.close();
            }
        }
    }

    /**
     * Positions the channel either at the beginning of the first entry found (this means not moving at all if this
     * is satisfied already) or at the end of the entry stream if there are no new entries to be found.
     *
     * @return starting position of the start entry, or end of the last entry if there are no start entries to be found
     */
    @Override
    public long alignWithStartEntry() throws IOException {
        try {
            if (payloadType == null) {
                readEnvelopeHeader();
            } else {
                buffer.position(payloadStartOffset);
            }
            if (payloadType != EnvelopeType.FULL && payloadType != EnvelopeType.BEGIN) {
                goToNextEntry();
            }
            return position() - HEADER_SIZE;
        } catch (ReadPastEndException e) {
            buffer.position(payloadEndOffset);
            return position();
        }
    }

    public long entryIndex() {
        return currentIndex;
    }

    @Override
    public LogPosition firstEntryPosition() throws IOException {
        long currentPosition = position();
        try {
            position(logHeader().getStartPosition().getByteOffset());
            var pos = alignWithStartEntry();
            return new LogPosition(getLogVersion(), pos);
        } finally {
            position(currentPosition);
        }
    }

    public long currentTerm() {
        return currentTerm;
    }

    @Override
    public long getLogVersion() {
        return channel.getLogVersion();
    }

    @Override
    public LogFormat getLogFormatVersion() {
        return channel.getLogFormatVersion();
    }

    /**
     *
     * @return test start of the current envelope
     */
    @Override
    public long position() {
        return (currentSegment * segmentBlockSize) + buffer.position();
    }

    @Override
    public void resetToPosition(long byteOffset) throws IOException {
        currentSegment = UNSPECIFIED_SEGMENT;
        position(byteOffset);
    }

    @Override
    public void position(long byteOffset) throws IOException {
        requireNonNegative(byteOffset);

        LogPositionMarker positionMarker = new LogPositionMarker();
        positionMarker.mark(channel.getLogVersion(), byteOffset);
        setLogPosition(positionMarker);
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) throws IOException {
        positionMarker.mark(channel.getLogVersion(), position());
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentLogPosition() throws IOException {
        return new LogPosition(channel.getLogVersion(), position());
    }

    /**
     * Position the channel to a given position. If the position is within the envelope header it will
     * be moved to the start of the next payload.
     */
    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        if (positionMarker.getLogVersion() != channel.getLogVersion()) {
            throw new IllegalArgumentException("Trying to set position with version: %d, while channel has version: %d."
                    .formatted(positionMarker.getLogVersion(), channel.getLogVersion()));
        }

        long byteOffset = positionMarker.getByteOffset();
        long newSegment = byteOffset >> segmentShift;
        int newBufferOffset = getSegmentOffset(byteOffset);

        if (newSegment == 0) {
            throw new IOException("Invalid position: " + positionMarker);
        }

        if (newSegment == currentSegment) {
            if (newBufferOffset < payloadStartOffset || newBufferOffset > payloadEndOffset) {
                readAllEnvelopesUpToIncluding(newBufferOffset, false);
            }
        } else {
            loadSegmentIntoBuffer(newSegment);
            if (newBufferOffset != 0 || newSegment == 1) {
                // Read into envelopes, possibly skipping START_OFFSET entry
                // also setup checksum values
                readAllEnvelopesUpToIncluding(newBufferOffset, false);
            } else {
                // disable checking as we don't want to load previous segment
                payloadType = null;
                enforceChecksumChain = false;
                if (buffer.remaining() > HEADER_SIZE) {
                    try {
                        // peek at envelope header
                        readEnvelopeHeader();
                        // rollback state ready for client to read into the envelope, but with previousChecksum
                        // and currentIndex initialised
                        currentChecksum = previousChecksum;
                        if (payloadType == EnvelopeType.BEGIN || payloadType == EnvelopeType.FULL) {
                            --currentIndex;
                        }
                    } catch (Exception e) {
                        // retain old behaviour where the channel will throw on next read
                        // have to reload segment as checking for preallocation, partial headers, etc may change the
                        // buffer
                        loadSegmentIntoBuffer(newSegment);
                        currentIndex = UNSPECIFIED_INDEX;
                        previousChecksum = logHeader.getPreviousLogFileChecksum();
                        currentChecksum = previousChecksum;
                        enforceChecksumChain = false;
                    }
                    // rollback remaining state
                    payloadType = null;
                    payloadStartOffset = 0;
                    payloadEndOffset = 0;
                    currentTerm = UNSPECIFIED_TERM;
                    currentContentType = UNSPECIFIED_CONTENT_TYPE;
                }
            }
        }
        checkState(
                newBufferOffset == 0 || newBufferOffset <= payloadEndOffset,
                "Invalid end of payload offset: %d. Expected payloadEndOffset: %d.",
                newBufferOffset,
                payloadEndOffset);

        buffer.position(Math.max(newBufferOffset, payloadStartOffset));
    }

    /**
     * Temporary because we are planning to get the checksum from the latest checkpoint instead of having to
     * read it from the channel. Might need something like this for the checkpoint log or tests though. But this
     * is a warning to not depend on it because it might change soon.
     *
     * @return checksum
     */
    public int temporaryFindPreviousChecksumBeforePosition(long byteOffset) throws IOException {
        long newSegment = byteOffset >> segmentShift;
        int newBufferOffset = getSegmentOffset(byteOffset);

        if (newSegment == 0) {
            throw new IOException("Invalid position: " + byteOffset);
        }

        // Read previous if at boundary
        if (newBufferOffset == 0 && newSegment != 1) {
            newSegment = newSegment - 1;
            newBufferOffset = segmentBlockSize;
        }

        if (newSegment != currentSegment) {
            loadSegmentIntoBuffer(newSegment);
        }

        readAllEnvelopesUpToIncluding(newBufferOffset, true);
        checkState(
                newBufferOffset == 0 || newBufferOffset <= payloadEndOffset,
                "Invalid end of payload offset: %d. Expected payloadEndOffset: %d.",
                newBufferOffset,
                payloadEndOffset);

        buffer.position(Math.max(newBufferOffset, payloadStartOffset));
        return currentChecksum;
    }

    @Override
    public void beginChecksum() {}

    public void setPositionUnsafe(long byteOffset) throws IOException {
        long newSegment = byteOffset >> segmentShift;
        int newBufferOffset = getSegmentOffset(byteOffset);
        if (newSegment != currentSegment) {
            loadSegmentIntoBuffer(newSegment);
        }
        buffer.position(newBufferOffset);
        payloadType = null;
        enforceChecksumChain = false;
        readEnvelopeHeader();
    }

    /**
     * Move the channel to a specific entryIndex.
     *
     * @return position of the next entry
     * @throws IOException          I/O error from channel.
     * @throws ReadPastEndException if the end is reached.
     */
    public long goToEntry(long entryIndex) throws IOException {
        if (entryIndex < logHeader.getLastAppendIndex()) {
            throw new IllegalArgumentException("Invalid entry index: " + entryIndex + " is in a previous log file");
        }
        if (channel.size() <= segmentBlockSize) {
            // nothing except the header in this file
            throw ReadPastEndException.INSTANCE;
        }

        int totalSegments = Math.toIntExact(Math.ceilDiv(channel.size(), segmentBlockSize));

        var segmentBinarySearch = new SegmentBinarySearch(this, totalSegments, segmentBlockSize);
        var segmentPosition = LogBinarySearch.binarySearch(segmentBinarySearch, entryIndex);

        if (segmentPosition != -1 && segmentPosition != position()) {
            position(segmentPosition);
        }

        while (currentIndex < entryIndex) {
            goToNextEntry();
        }
        return position() - HEADER_SIZE;
    }

    @Override
    public int endChecksumAndValidate() {
        return currentChecksum;
    }

    @Override
    public int getChecksum() {
        return currentChecksum;
    }

    public int getPreviousChecksum() {
        return previousChecksum;
    }

    @Override
    public byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        // Mark before reading since getVersion can throw ReadPastEndException
        marker.mark(channel.getLogVersion(), position());

        byte versionByte = getVersion();
        checkState(versionByte != IGNORE_CONTENT_VERSION, "Could not find a valid envelope header.");

        // Mark position to start of envelope
        long position = position();
        if (buffer.position() == payloadStartOffset) {
            position -= HEADER_SIZE;
        }
        marker.mark(channel.getLogVersion(), position);

        return versionByte;
    }

    @Override
    public boolean rewindAfterMarkAndGetVersion() {
        return false;
    }

    @Override
    public byte get() throws IOException {
        ensureDataExists(Byte.BYTES);
        return buffer.get();
    }

    @Override
    public short getShort() throws IOException {
        ensureDataExists(Short.BYTES);
        return buffer.getShort();
    }

    @Override
    public int getInt() throws IOException {
        ensureDataExists(Integer.BYTES);
        return buffer.getInt();
    }

    @Override
    public long getLong() throws IOException {
        ensureDataExists(Long.BYTES);
        return buffer.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        ensureDataExists(Float.BYTES);
        return buffer.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        ensureDataExists(Double.BYTES);
        return buffer.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        assert length <= bytes.length;

        try {
            var bytesRead = 0;
            while (bytesRead < length) {
                if (checkForEndOfEnvelope()) {
                    readEnvelopeHeader();
                }

                final var chunkSize = min(payloadEndOffset - buffer.position(), length - bytesRead);
                ensureDataExists(chunkSize);
                buffer.get(bytes, bytesRead, chunkSize);
                bytesRead += chunkSize;
            }
        } catch (ClosedChannelException e) {
            handleClosedChannelException(e);
        }
    }

    @Override
    public byte getVersion() throws IOException {
        if (checkForEndOfEnvelope()) {
            readEnvelopeHeader();
        }
        return payloadVersion;
    }

    @Override
    public byte getContentType() throws IOException {
        if (checkForEndOfEnvelope()) {
            readEnvelopeHeader();
        }
        return currentContentType;
    }

    @Override
    public long getAppendIndex() throws IOException {
        return entryIndex();
    }

    /**
     * Move the channel to the start of the next transaction entry (either kernel or replicated from Raft), skipping
     * over any non-tx entries along the way.
     *
     * @return position of the next transaction entry
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if the end is reached.
     */
    public long goToNextTransactionEntry() throws IOException {
        byte contentType;
        do {
            goToNextEntry();
            contentType = getContentType();
        } while (contentType != KERNEL_CONTENT_TYPE
                && contentType != UNSPECIFIED_CONTENT_TYPE
                && contentType != REPLICATED_TX_CONTENT_TYPE);
        return position() - HEADER_SIZE;
    }

    /**
     * Move the channel to the next start of the next entry.
     *
     * @return position of the next entry
     * @throws IOException          I/O error from channel.
     * @throws ReadPastEndException if the end is reached.
     */
    public long goToNextEntry() throws IOException {
        do {
            goToNextEnvelope();
        } while (payloadType != EnvelopeType.FULL && payloadType != EnvelopeType.BEGIN);
        return position() - HEADER_SIZE;
    }

    /**
     * Move the channel to the next envelope. This method should be used carefully since it may set the
     * position in the middle of an entry.
     *
     * @return position of the next entry
     * @throws IOException          I/O error from channel.
     * @throws ReadPastEndException if the end is reached.
     */
    public long goToNextEnvelope() throws IOException {
        skipToNextEnvelope();
        readEnvelopeHeader();
        return position() - HEADER_SIZE;
    }

    public boolean isStartEnvelope() {
        return payloadType == EnvelopeType.FULL || payloadType == EnvelopeType.BEGIN;
    }

    /**
     * If not already at the end of an entry then moves to the end of that entry
     * otherwise seeks to the end of the following entry
     * @return LogPosition at the end of the possibly multi-envelope entry
     * @throws IOException          I/O error from channel.
     * @throws ReadPastEndException if the end is reached.
     */
    public LogPosition goToEndOfEntry() throws IOException {
        if (buffer.position() == payloadEndOffset
                || (payloadType != EnvelopeType.FULL && payloadType != EnvelopeType.END)) {
            do {
                skipToNextEnvelope();
                readEnvelopeHeader();
            } while (payloadType != EnvelopeType.FULL && payloadType != EnvelopeType.END);
        }
        skipToNextEnvelope();
        return getCurrentLogPosition();
    }

    @Override
    public boolean isOpen() {
        return !closed;
    }

    @Override
    public void close() throws IOException {
        if (!closed) {
            channel.close();
            scopedBuffer.close();
            channel = null;
            closed = true;
        }
    }

    public LogHeader logHeader() {
        return logHeader;
    }

    LogVersionedStoreChannel channel() {
        return channel;
    }

    private void readAllEnvelopesUpToIncluding(int bufferOffset, boolean forceReadingEvenIfAtEnd) throws IOException {
        assert currentSegment != 0;
        payloadType = null;
        // We need to skip the first checksum chain check as we don't know the previous checksum.
        enforceChecksumChain = false;
        payloadVersion = IGNORE_CONTENT_VERSION;
        buffer.position(0);
        payloadStartOffset = 0;
        payloadEndOffset = 0;

        if (bufferOffset == buffer.limit() && !forceReadingEvenIfAtEnd) {
            // Positioning at the end of the file
            buffer.position(bufferOffset);
            payloadStartOffset = bufferOffset;
            payloadEndOffset = bufferOffset;
            return;
        }

        if (currentSegment == 1) {
            consumeStartOffsetEnvelopeIfPresent();

            // Since we're in the first segment, we might be able to restore the previous checksum from the file
            // header.
            if (logHeader != null) {
                previousChecksum = logHeader.getPreviousLogFileChecksum();
                currentChecksum = previousChecksum;
                enforceChecksumChain = true;
            }

            if (bufferOffset <= buffer.position()) {
                // One of the two situation:
                // (a) didn't had an START_OFFSET envelope, which means buffer.position() is still 0 and we
                //     asked for bufferOffset to be 0. So we can just return and don't have to read anything
                //     else
                // (b) we had an START_OFFSET envelope, which means buffer.position() is now pointing to after
                //     it and we asked for an offset inside the START_OFFSET envelope. So we don't have to
                //     read anything else since we already skipped the START_OFFSET and positioned the buffer
                //     at the start of the next envelope.
                return;
            }
        }

        while (payloadEndOffset < bufferOffset) {
            readEnvelopeHeader();
            skipToNextEnvelope();
        }
    }

    private void consumeStartOffsetEnvelopeIfPresent() throws IOException {
        assert buffer.position() == 0 : "buffer was not positioned at 0 when started checking for START_OFFSET";

        buffer.getInt(); // envelope checksum
        var type = EnvelopeType.of(buffer.get());
        if (type == EnvelopeType.START_OFFSET) {
            int offsetLength = buffer.getInt();
            assert offsetLength > 0 : "START_OFFSET payload length should be bigger than 0";
            buffer.position(HEADER_SIZE); // Skip the whole header.
            enforceZeros(offsetLength);
            assert buffer.position() == HEADER_SIZE + offsetLength
                    : "buffer should have been positioned after START_OFFSET envelope";
            payloadStartOffset = buffer.position();
            payloadEndOffset = buffer.position();
        } else {
            // Not a START_OFFSET, so we rewind to read the envelope and process it as usual.
            buffer.position(0);
        }
    }

    private void skipToNextEnvelope() throws IOException {
        try {
            buffer.position(payloadEndOffset);
        } catch (IllegalArgumentException e) {
            // Either an incomplete envelope, or a larger corruption if there is things after this segment.
            // Would only get here in the unlikely case that the checksums did not mismatch even though the
            // envelope wasn't complete.
            buffer.position(buffer.limit());
            checkTail(
                    this,
                    new LogPosition(channel.getLogVersion(), currentSegment * segmentBlockSize + payloadStartOffset),
                    e);
            throw new IncompleteEnvelopeReadException("Could not go to end of envelope", e);
        }
    }

    private void ensureDataExists(int requestedNumberOfBytes) throws IOException {
        try {
            if (checkForEndOfEnvelope()) {
                readEnvelopeHeader();
            }

            bufferCheck(requestedNumberOfBytes);
        } catch (ClosedChannelException e) {
            handleClosedChannelException(e);
        }
    }

    private void handleClosedChannelException(ClosedChannelException e) throws ClosedChannelException {
        // We don't want to check the closed flag every time we read, instead we can avoid unnecessary the
        // read and catch ClosedChannelException where we see if the channel being closed was
        // deliberate or not. If it was deliberately closed then throw IllegalStateException instead so
        // that callers won't treat this as a kernel panic.
        if (channel == null || !channel.isOpen()) {
            throw new IllegalStateException("This log channel has been closed", e);
        }

        // OK, this channel was closed without us really knowing about it, throw exception as is.
        throw e;
    }

    private void bufferCheck(int requestedNumberOfBytes) throws IOException {
        if (buffer.remaining() < requestedNumberOfBytes) {
            throw new IncompleteEnvelopeReadException(
                    "Entry underflow. %d bytes was requested, but only %d are available."
                            .formatted(requestedNumberOfBytes, buffer.remaining()));
        }
    }

    protected boolean checkForEndOfEnvelope() throws InvalidLogEnvelopeReadException {
        if (buffer.position() > payloadEndOffset) {
            throw new InvalidLogEnvelopeReadException(
                    "Read has gone past an envelope boundary at position: %d of segment: %d. payloadEndOffset: %d, Envelope type: %s."
                            .formatted(buffer.position(), currentSegment, payloadEndOffset, payloadType));
        }
        return buffer.position() == payloadEndOffset;
    }

    private void enforceTerminalZeros() throws IOException {
        enforceZeros(buffer.remaining());
    }

    private void enforceZeros(int length) throws IOException {
        checkState(
                length <= buffer.remaining(),
                "Tried to enforce more zeros (%d) than the buffer's remaining size (%d).",
                length,
                buffer.remaining());

        while (length >= Long.BYTES) {
            long value = buffer.getLong();
            if (value != 0) {
                buffer.position(buffer.position() - Long.BYTES);
                // We break here, so it will continue on the loop below and point out exactly
                // the position where the non-zero was found.
                break;
            }
            length -= Long.BYTES;
        }
        while (length > 0) {
            final var value = buffer.get();
            if (value != 0) {
                buffer.position(buffer.position() - Byte.BYTES);
                printExcessData();
            }
            length -= Byte.BYTES;
        }
    }

    private void printExcessData() throws InvalidLogEnvelopeReadException {
        // don't need to fill up from the entire segment block size
        long position = position();
        final int remaining = Math.min(buffer.remaining(), 1024);
        final var excess = new byte[remaining];
        buffer.get(excess);
        throw new InvalidLogEnvelopeReadException("Unexpected data found at end of buffer at position: " + position
                + ". Expecting only zeros at this point. Found: " + Arrays.toString(excess));
    }

    protected void readEnvelopeHeader() throws IOException {
        int nextEnvelopeChecksum;
        EnvelopeType nextEnvelopeType;

        // Loop until we find the next header, or throws read past end exception
        while (true) {

            // Must be padding
            if (buffer.remaining() <= HEADER_SIZE) {
                // Or could be broken last entry if we technically had room for the header
                if (buffer.capacity() - buffer.position() > HEADER_SIZE) {
                    checkIfIncompleteEnvelopeHeader();
                } else {
                    enforceTerminalZeros();
                }
                nextSegment(true);
            }

            // Optimistically read the beginning of the header
            nextEnvelopeChecksum = buffer.getInt();
            nextEnvelopeType = EnvelopeType.of(buffer.get());

            if (nextEnvelopeType == EnvelopeType.START_OFFSET) {
                consumeStartOffsetEnvelopeIfValid();
                continue;
            }
            if (nextEnvelopeType != EnvelopeType.ZERO) {
                break;
            }

            if (nextEnvelopeChecksum != 0) {
                LogPosition currentLogPosition = getCurrentLogPosition();
                checkTail(this, currentLogPosition, null);
                // If tail was fine it is still an incomplete envelope to be handled
                throw new IncompleteEnvelopeReadException("Unexpected data found at "
                        + new LogPosition(
                                currentLogPosition.getLogVersion(),
                                currentLogPosition.getByteOffset() - Integer.BYTES - Byte.BYTES)
                        + " Found: " + nextEnvelopeChecksum);
            }

            // Found zeroes, figure out if we are in padding or end of pre-allocated file
            final var remaining = buffer.remaining();
            enforceTerminalZeros();
            if (remaining >= MAX_ZERO_PADDING_SIZE) {
                // Must be the end of actual content in a longer pre-allocated file
                // So we throw, to avoid the loop to keep going and just read a lot of zeroes
                // until the end of the file.

                // Position should be reset so we know where the actual content ended
                buffer.position(buffer.position() - remaining - 5 /* checksum + type */);
                throw ReadPastEndException.INSTANCE;
            }
        }

        int nextPayloadLength = buffer.getInt();
        long nextPayloadIndex = buffer.getLong();
        byte nextPayloadVersion = buffer.get();
        int previousEnvelopeChecksumFromHeader = buffer.getInt();
        long nextTerm = buffer.getLong();
        byte nextContentType = buffer.get();

        payloadType = nextEnvelopeType;
        payloadVersion = nextPayloadVersion;
        currentIndex = nextPayloadIndex;
        currentTerm = nextTerm;
        currentContentType = nextContentType;
        payloadStartOffset = buffer.position();
        payloadEndOffset = payloadStartOffset + nextPayloadLength;
        if (payloadEndOffset > segmentBlockSize) {
            throw new InvalidLogEnvelopeReadException(
                    "Envelope span segment boundary - start: %d, length: %d, segmentBlockSize: %d."
                            .formatted(payloadStartOffset, nextPayloadLength, segmentBlockSize));
        }

        if (enforceChecksumChain) {
            if (currentChecksum != previousEnvelopeChecksumFromHeader) {
                if (nextTerm == 0 && nextContentType == 0) { // Fields after checksum
                    // Possibly this is just an incomplete tx that was cut off during writing the checksum
                    LogPosition currentLogPosition = getCurrentLogPosition();
                    try {
                        checkTail(this, currentLogPosition, null);
                        throw new IncompleteEnvelopeReadException(
                                "Found incomplete envelope header, incomplete at previousChecksum "
                                        + new LogPosition(
                                                currentLogPosition.getLogVersion(),
                                                currentLogPosition.getByteOffset() - Long.BYTES - Byte.BYTES)
                                        + " Found: " + previousEnvelopeChecksumFromHeader);
                    } catch (IllegalStateException e) {
                        // Nope it's broken
                    }
                }

                throw new ChecksumMismatchException(
                        "Envelope checksum chain is broken. Previous checksum: %d, expected: %d.",
                        currentChecksum, previousEnvelopeChecksumFromHeader);
            }
        } else {
            // If we skipped the checksum chain check because we're missing the previous checksum then
            // we can enable it again now that we'll have it again.
            enforceChecksumChain = true;
        }

        currentChecksum = nextEnvelopeChecksum;
        previousChecksum = previousEnvelopeChecksumFromHeader;

        checksumView.limit(payloadEndOffset).position(payloadStartOffset - PAYLOAD_CHECKSUM_OFFSET_FROM_START);
        checksum.reset();
        checksum.update(checksumView);
        int readChecksum = (int) checksum.getValue();
        if (readChecksum != nextEnvelopeChecksum) {
            throwOnMismatchingChecksum(nextEnvelopeChecksum, readChecksum);
        }
    }

    private void consumeStartOffsetEnvelopeIfValid() throws IOException {
        int alreadyRead = Integer.BYTES + Byte.BYTES;
        if (currentSegment != 1 || (buffer.position() != alreadyRead)) {
            // Any START_OFFSET envelope we encounter that is not first in the file is an error/malformed log
            // file.
            throw new InvalidLogEnvelopeReadException(
                    EnvelopeType.START_OFFSET, currentSegment, buffer.position() - alreadyRead);
        }
        // START_OFFSET envelopes can be found in the beginning of any log file if the files have been
        // put together by aggregating backups. Allow as first envelope in any file.
        buffer.position(0);
        consumeStartOffsetEnvelopeIfPresent();
    }

    private void throwOnMismatchingChecksum(int nextEnvelopeChecksum, int readChecksum) throws IOException {
        // Oh no it didn't match! It could be because of an incomplete envelope..
        // Let's assume it is as long as there is no more data after the current envelope.
        ChecksumMismatchException e = new ChecksumMismatchException(nextEnvelopeChecksum, readChecksum);
        if (buffer.limit() >= payloadEndOffset) {
            buffer.position(payloadEndOffset);
            try {
                checkTail(this, getCurrentLogPosition(), null);
            } catch (IllegalStateException exception) {
                // It's a bigger corruption, let's throw our original checksum error
                // since that was the first problem.
                throw e;
            }
        }
        throw new IncompleteEnvelopeReadException(e);
    }

    private void checkIfIncompleteEnvelopeHeader() throws IOException {
        assert buffer.remaining() <= HEADER_SIZE;
        try {
            enforceTerminalZeros();
        } catch (InvalidLogEnvelopeReadException e) {
            // We saw some data - this means either broken last entry or corruption
            checkTail(this, getCurrentLogPosition(), e);
            // Either check tail threw an exception or we just had zeroes. If just zeroes it should be
            // considered as a last broken entry
            throw new IncompleteEnvelopeReadException("Found incomplete envelope header", e);
        }
    }

    private void nextSegment(boolean enforceChannelIntegrityForce) throws IOException {
        int read;
        if (channel.size() == channel.position()) {
            // We are at the end, don't try to load in the next segment because that will change the position and
            // it should be correct on calling getPosition if ReadPastEndException is thrown.
            do {
                goToNextFileOrThrow(enforceChannelIntegrityForce);
            } while (buffer.position() == buffer.limit()
                    && channel.size() == channel.position()); // Handle file with only header
            read = loadSegmentIntoBuffer(1);
        } else {
            read = loadSegmentIntoBuffer(currentSegment + 1);
            if (read == -1) {
                // Correct the file position for getPosition by backing the segment again.
                // Necessary if the below go-to-next throws a ReadPastEndException.
                currentSegment--;
                goToNextFileOrThrow(enforceChannelIntegrityForce);
                // Read the first data segment
                read = loadSegmentIntoBuffer(1);
            }
        }

        if (read < HEADER_SIZE) {
            if (read < 1) {
                throw ReadPastEndException.INSTANCE;
            }
            byte[] excess = new byte[read];
            buffer.get(excess);
            throw new IncompleteEnvelopeReadException(
                    "Unexpected data found at start of buffer - expecting a valid header. Found: "
                            + Arrays.toString(excess));
        }
    }

    private void goToNextFileOrThrow(boolean enforceChannelIntegrityChecks) throws IOException {
        final var nextChannel = bridge.next(channel, raw);
        assert nextChannel != null;
        if (nextChannel == channel) {
            // no more channels - we cannot satisfy the requested number of bytes
            if (payloadType == EnvelopeType.BEGIN || payloadType == EnvelopeType.MIDDLE) {
                throw new InvalidEndOfFileReadException(
                        "Log file with version %d ended with an incomplete record type (%s) and no following log file could be found."
                                .formatted(channel.getLogVersion(), payloadType.name()));
            }
            throw ReadPastEndException.INSTANCE;
        }
        setNextChannel(nextChannel, enforceChannelIntegrityChecks);
    }

    public void setNextChannel(LogVersionedStoreChannel nextChannel, boolean enforceChannelIntegrityChecks)
            throws IOException {
        channel = nextChannel;
        if (!enforceChannelIntegrityChecks) {
            enforceChecksumChain = false;
            payloadType = null;
        }
        readAndValidateFileHeader();
    }

    private void readAndValidateFileHeader() throws IOException {
        // First segment contains a header and zeros
        int read = loadSegmentIntoBuffer(0);
        if (read < segmentBlockSize) {
            // Too small file to contain data, just return and let other methods return ReadPastEndException
            return;
        }
        logHeader = LogFormat.parseHeader(buffer, true, null);
        if (logHeader == null) {
            // Pre-allocated file, just return and let other methods return ReadPastEndException
            return;
        }

        if (!enforceChecksumChain) {
            checkState(payloadType == null, "Can not override checksum in the middle of a payload");
            previousChecksum = logHeader.getPreviousLogFileChecksum();
            currentChecksum = previousChecksum;
        }
        enforceChecksumChain = true;

        checkState(
                segmentBlockSize == logHeader.getSegmentBlockSize(),
                "Changing segmentBlockSize not supported. Initialized segmentBlockSize: %d. Actual header: %s.",
                segmentBlockSize,
                logHeader);
        checkState(
                LogFormat.V10.getVersionByte()
                        <= logHeader.getLogFormatVersion().getVersionByte(),
                "Envelopes are not supported in LogFormat versions older than V10, but found: %s.",
                logHeader.getLogFormatVersion());
        checkState(
                currentChecksum == logHeader.getPreviousLogFileChecksum(),
                "Checksum chain broken on file change to logFileVersion: %d currentChecksum: %d header checksum: %d"
                        .formatted(channel.getLogVersion(), currentChecksum, logHeader.getPreviousLogFileChecksum()));

        enforceZeros(segmentBlockSize - buffer.position());
    }

    private int loadSegmentIntoBuffer(long newSegment) throws IOException {
        buffer.clear();
        channel.position(newSegment * segmentBlockSize);
        int totalRead = 0;
        while (buffer.hasRemaining()) {
            int read = channel.read(buffer);
            if (read == -1) {
                // We reached the end
                if (totalRead == 0) {
                    totalRead = -1; // Failed to load any data
                }
                break;
            }
            totalRead += read;
        }
        buffer.flip();

        // Update state
        currentSegment = newSegment;
        payloadStartOffset = 0;
        payloadEndOffset = 0;

        return totalRead;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        int length = dst.remaining();
        try {
            var bytesRead = 0;
            while (bytesRead < length) {
                if (checkForEndOfEnvelope()) {
                    readEnvelopeHeader();
                }

                final var chunkSize = min(payloadEndOffset - buffer.position(), length - bytesRead);
                dst.put(dst.position(), buffer, buffer.position(), chunkSize);
                dst.position(dst.position() + chunkSize);
                buffer.position(buffer.position() + chunkSize);
                bytesRead += chunkSize;
            }
        } catch (ClosedChannelException e) {
            handleClosedChannelException(e);
        }
        return length;
    }

    @Override
    public int directRead(ByteBuffer dst) throws IOException {
        // Skip all the envelope handling
        int length = dst.remaining();
        try {
            var bytesRead = 0;
            // We don't know what envelopes we are reading over
            // and we need to avoid throwing
            // InvalidEndOfFileReadException in goToNextFileOrThrow
            // when we hit the log end,
            // so we clear the payloadType to Zero
            payloadType = EnvelopeType.ZERO;
            while (bytesRead < length) {
                if (buffer.position() == buffer.limit()) {
                    nextSegment(true);
                }
                final var chunkSize = min(buffer.remaining(), length - bytesRead);
                dst.put(dst.position(), buffer, buffer.position(), chunkSize);
                dst.position(dst.position() + chunkSize);
                buffer.position(buffer.position() + chunkSize);
                bytesRead += chunkSize;
            }
        } catch (ClosedChannelException e) {
            handleClosedChannelException(e);
        }
        return length;
    }

    public void reReadSegment() throws IOException {
        var logPositionMarker = new LogPositionMarker();
        var currentLogPosition = getCurrentLogPosition(logPositionMarker);
        currentSegment = -1;
        setLogPosition(currentLogPosition);
    }

    public void goToNextSegment() throws IOException {
        enforceChecksumChain = false;
        nextSegment(false);
        var position = goToNextEnvelope();
        enforceChecksumChain = true;
        buffer.position(getSegmentOffset(position));
    }

    public ByteBuffer getBuffer() {
        return buffer;
    }

    public int getSegmentOffset(long position) {
        return (int) position & segmentMask;
    }

    @Override
    public String toString() {
        return "EnvelopeReadChannel{" + ", previousChecksum="
                + previousChecksum + ", checksumView="
                + checksumView + ", currentSegment="
                + currentSegment + ", payloadType="
                + payloadType + ", currentIndex="
                + currentIndex + ", currentTerm="
                + currentTerm + ", currentContentType="
                + currentContentType + ", payloadVersion="
                + payloadVersion + ", payloadStartOffset="
                + payloadStartOffset + ", payloadEndOffset="
                + payloadEndOffset + ", logHeader="
                + logHeader + ", closed="
                + closed + '}';
    }
}
