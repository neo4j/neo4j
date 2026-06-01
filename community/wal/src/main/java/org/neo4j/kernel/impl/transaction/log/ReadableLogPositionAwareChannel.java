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

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.ReadPastEndException;
import org.neo4j.io.fs.ReadableChannel;

public interface ReadableLogPositionAwareChannel extends ReadableChannel, LogPositionAwareChannel {
    /**
     * Logically, this method is the same as calling
     * {@link LogPositionAwareChannel#getCurrentLogPosition(LogPositionMarker)} followed by a call to
     * {@link ReadableChannel#getVersion()}. However, in some circumstances the call to get can cause the channel to
     * rollover into the next version when the marker has been positioned in the PREVIOUS channel, giving an
     * inconsistent reading. Implementations should ensure that the positioned marker is correct for the location of
     * the returned byte value.
     * @param marker the marker used to track the position of the underlying channel BEFORE getting the byte value
     * @return the next byte value in the channel
     * @throws IOException if unable to read the channel for data
     */
    default byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        getCurrentLogPosition(marker);
        return getVersion();
    }

    default boolean rewindAfterMarkAndGetVersion() {
        return true;
    }

    /**
     * Reads raw byte data from the channel into the provided buffer
     * including any headers, or other elided data that would not
     * normally be visible using read
     *
     * @param dst buffer to copy data into
     * @return The number of bytes read, possibly zero, or -1 if the channel has reached end-of-stream
     * @throws IOException if unable to read the channel for data
     */
    int directRead(ByteBuffer dst) throws IOException;

    /**
     * Move chanel position forward to the next full entry start. This is a no op on
     * non-enveloped channels, but on enveloped files may even require bridging across several files
     *
     * @return byte offset within current log file that should be used to seek to the entry in the future.
     * N.B. This may differ from the value returned by position() as that may include headers skipped over.
     * @throws IOException if unable to alter channel position
     */
    long alignWithStartEntry() throws IOException;

    /**
     * Allows delegates of enveloped channels to signal the ability to skip entries
     * without parsing the payload bytes.
     *
     * @return true if entry skip methods can be used.
     * If it returns false then the skip methods
     * {@link ReadableLogPositionAwareChannel#isAtStartOfFullEntry()},
     * {@link ReadableLogPositionAwareChannel#goToNextEntry()}
     * and {@link ReadableLogPositionAwareChannel#goToEndOfEntry()} must not be called and will throw
     */
    boolean supportsEntrySkipping();

    /**
     * Determine if read position is on first full entry payload byte, or equivalently just
     * before the envelope header which will be read as part of checking this
     * @return true if byte is at start of entry data, else false
     * @throws IOException if error in underlying channel
     * @throws org.neo4j.io.fs.ReadPastEndException if positioned on last readable byte of the channel
     * and unable to satisfy the envelope header read
     * @throws IllegalStateException if called on a channel that returns false
     * from {@link ReadableLogPositionAwareChannel#supportsEntrySkipping()}
     */
    boolean isAtStartOfFullEntry() throws IOException;

    /**
     * Jump to start of next full entry, skipping any intermediate envelopes and
     * possibly entering into new files if on a bridged channel
     * @return the byte offset of the envelope header beginning the new entry.
     * Note this differs from the position after this call which will be  aligned on the first payload byte.
     * @throws IOException if error in underlying channel
     * @throws org.neo4j.io.fs.ReadPastEndException if the skip goes past the end of readable content
     * @throws IllegalStateException if called on a channel that returns false
     * from {@link ReadableLogPositionAwareChannel#supportsEntrySkipping()}
     */
    long goToNextEntry() throws IOException;

    /**
     * If the current position is located within an entry's payload jump to after the last payload byte of the entry as
     * if the full entry data has been consumed by reads. This may involve skipping intermediate envelopes to reach the
     * final envelope. If the current position is already after an entry's payload e.g. from a previous call to the
     * method then the immediately following envelope header is read and the channel skips to the end of that succeeding
     * entry.
     * @return the full{@link LogPosition} of the channel just after the entry.
     * @throws IOException if error in underlying channel
     * @throws org.neo4j.io.fs.ReadPastEndException if the skip goes past the end of readable content
     * @throws IllegalStateException if called on a channel that returns false
     * from {@link ReadableLogPositionAwareChannel#supportsEntrySkipping()}
     */
    LogPosition goToEndOfEntry() throws IOException;

    /**
     * Skips the next {@code length} bytes from this channel{@code bytes} as if they had been read.
     * If the channel maintains a checksum this will include the skipped bytes and thus the validation
     * should be updated accordingly, but the channel may otherwise optimize the skipping.
     *
     * @param length number of bytes to read from the channel.
     * @throws IOException I/O error from channel.
     * @throws ReadPastEndException if not enough data was available.
     * @throws IllegalStateException if called on a channel that returns false from {@link ReadableLogPositionAwareChannel#supportsEntrySkipping()}
     */
    void skip(int length) throws IOException;
}
