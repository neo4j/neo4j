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

import static org.neo4j.util.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.ReadableChannel;

public class DelegateReadableChannel implements ReadableLogPositionAwareChannel {
    private ReadableChannel delegate;

    public DelegateReadableChannel() {}

    public DelegateReadableChannel(ReadableChannel delegate) {
        this.delegate = delegate;
    }

    public void delegateTo(ReadableChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte get() throws IOException {
        assertAssigned();
        return delegate.get();
    }

    @Override
    public short getShort() throws IOException {
        assertAssigned();
        return delegate.getShort();
    }

    @Override
    public int getInt() throws IOException {
        assertAssigned();
        return delegate.getInt();
    }

    @Override
    public long getLong() throws IOException {
        assertAssigned();
        return delegate.getLong();
    }

    @Override
    public float getFloat() throws IOException {
        assertAssigned();
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        assertAssigned();
        return delegate.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        assertAssigned();
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
    public byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel posChannel) {
            return posChannel.markAndGetVersion(marker);
        }
        return ReadableLogPositionAwareChannel.super.markAndGetVersion(marker);
    }

    @Override
    public boolean rewindAfterMarkAndGetVersion() {
        // Rewinding is done to get checksum calculations correct.
        // This channel doesn't care about checksums, so there is no point in rewinding.
        return false;
    }

    @Override
    public int directRead(ByteBuffer dst) throws IOException {
        assertAssigned();
        return delegate.read(dst);
    }

    @Override
    public long alignWithStartEntry() throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel ch) {
            return ch.alignWithStartEntry();
        }
        return position();
    }

    @Override
    public boolean supportsEntrySkipping() {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.supportsEntrySkipping();
        }
        return false;
    }

    @Override
    public boolean isAtStartOfFullEntry() throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.isAtStartOfFullEntry();
        }
        throw new IllegalStateException("Skipping log entries not supported");
    }

    @Override
    public long goToNextEntry() throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.goToNextEntry();
        }
        throw new IllegalStateException("Skipping log entries not supported");
    }

    @Override
    public LogPosition goToEndOfEntry() throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.goToEndOfEntry();
        }
        throw new IllegalStateException("Skipping log entries not supported");
    }

    @Override
    public void skip(int length) throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            positionAwareChannel.skip(length);
        }
        throw new IllegalStateException("Skipping bytes not supported");
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.getCurrentLogPosition(positionMarker);
        }
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentLogPosition() throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            return positionAwareChannel.getCurrentLogPosition();
        }
        return LogPosition.UNSPECIFIED;
    }

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        assertAssigned();
        if (delegate instanceof ReadableLogPositionAwareChannel positionAwareChannel) {
            positionAwareChannel.setLogPosition(positionMarker);
        }
    }

    @Override
    public boolean isOpen() {
        assertAssigned();
        return delegate.isOpen();
    }

    @Override
    public void close() {
        // no op
    }

    @Override
    public void beginChecksum() {
        // no op
    }

    @Override
    public int getChecksum() {
        // no op
        return 0;
    }

    @Override
    public int endChecksumAndValidate() {
        // no op
        return 0;
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        assertAssigned();
        return delegate.read(dst);
    }

    @Override
    public long position() throws IOException {
        assertAssigned();
        return delegate.position();
    }

    @Override
    public void position(long byteOffset) throws IOException {
        assertAssigned();
        delegate.position(byteOffset);
    }

    private void assertAssigned() {
        checkArgument(delegate != null, "No assigned channel to delegate reads");
    }
}
