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
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.kernel.impl.transaction.log.enveloped.EnvelopeReadChannel;
import org.neo4j.memory.MemoryTracker;

/**
 * Basically a sequence of {@link StoreChannel channels} seamlessly seen as one, even over switch to envelopes.
 * Should be used if the startingChannel is not using envelopes and more than one file might be read.
 */
public class FormatSwitchingReadAheadLogChannel implements ReadableLogChannel {
    ReadableLogChannel delegate;

    public FormatSwitchingReadAheadLogChannel(
            LogVersionedStoreChannel startingChannel,
            LogVersionBridge bridge,
            MemoryTracker memoryTracker,
            boolean raw) {
        this.delegate = new ReadAheadLogChannel(startingChannel, memoryTracker) {
            @Override
            protected LogVersionedStoreChannel next(LogVersionedStoreChannel channel) throws IOException {
                LogVersionedStoreChannel next = bridge.next(channel, raw);
                if (!next.getLogFormatVersion().usesSegments()) {
                    return next;
                }
                delegate.close();
                delegate = getEnvelopeReadChannel(next, bridge, memoryTracker, raw);
                throw FormatSwitchingReadAheadLogChannelException.INSTANCE;
            }
        };
    }

    protected EnvelopeReadChannel getEnvelopeReadChannel(
            LogVersionedStoreChannel next, LogVersionBridge bridge, MemoryTracker memoryTracker, boolean raw)
            throws IOException {
        return new EnvelopeReadChannel(
                next,
                /* Not entirely correct to use the default size from the format since tests can
                have other segment sizes. If ever a problem, switch to reading the size from header. */
                next.getLogFormatVersion().getDefaultSegmentBlockSize(),
                bridge,
                memoryTracker,
                raw);
    }

    @Override
    public long getLogVersion() {
        return delegate.getLogVersion();
    }

    @Override
    public LogFormat getLogFormatVersion() {
        return delegate.getLogFormatVersion();
    }

    @Override
    public byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        try {
            return delegate.markAndGetVersion(marker);
        } catch (FormatSwitchingReadAheadLogChannelException e) {
            return delegate.markAndGetVersion(marker);
        }
    }

    @Override
    public boolean rewindAfterMarkAndGetVersion() {
        return delegate.rewindAfterMarkAndGetVersion();
    }

    @Override
    public int directRead(ByteBuffer dst) throws IOException {
        return delegate.directRead(dst);
    }

    @Override
    public long alignWithStartEntry() throws IOException {
        return delegate.alignWithStartEntry();
    }

    @Override
    public boolean supportsEntrySkipping() {
        return delegate.supportsEntrySkipping();
    }

    @Override
    public boolean isAtStartOfFullEntry() throws IOException {
        return delegate.isAtStartOfFullEntry();
    }

    @Override
    public long goToNextEntry() throws IOException {
        return delegate.goToNextEntry();
    }

    @Override
    public LogPosition goToEndOfEntry() throws IOException {
        return delegate.goToEndOfEntry();
    }

    @Override
    public void skip(int length) throws IOException {
        delegate.skip(length);
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) throws IOException {
        return delegate.getCurrentLogPosition(positionMarker);
    }

    @Override
    public LogPosition getCurrentLogPosition() throws IOException {
        return delegate.getCurrentLogPosition();
    }

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        delegate.setLogPosition(positionMarker);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public void resetToPosition(long byteOffset) throws IOException {
        delegate.resetToPosition(byteOffset);
    }

    @Override
    public LogPosition firstEntryPosition() throws IOException {
        return delegate.firstEntryPosition();
    }

    @Override
    public void beginChecksum() {
        delegate.beginChecksum();
    }

    @Override
    public int getChecksum() {
        return delegate.getChecksum();
    }

    @Override
    public int endChecksumAndValidate() throws IOException {
        return delegate.endChecksumAndValidate();
    }

    @Override
    public byte get() throws IOException {
        return delegate.get();
    }

    @Override
    public short getShort() throws IOException {
        return delegate.getShort();
    }

    @Override
    public int getInt() throws IOException {
        return delegate.getInt();
    }

    @Override
    public long getLong() throws IOException {
        return delegate.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        return delegate.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        delegate.get(bytes, length);
    }

    @Override
    public byte getVersion() throws IOException {
        return delegate.getVersion();
    }

    @Override
    public long getAppendIndex() throws IOException {
        return delegate.getAppendIndex();
    }

    @Override
    public byte getContentType() throws IOException {
        return delegate.getContentType();
    }

    @Override
    public void position(long byteOffset) throws IOException {
        delegate.position(byteOffset);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate.read(dst);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }
}
