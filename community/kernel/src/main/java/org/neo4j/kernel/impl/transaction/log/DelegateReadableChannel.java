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
    public byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        if (delegate instanceof ReadableLogPositionAwareChannel posChannel) {
            return posChannel.markAndGetVersion(marker);
        }
        return ReadableLogPositionAwareChannel.super.markAndGetVersion(marker);
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) {
        positionMarker.unspecified();
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentLogPosition() {
        return LogPosition.UNSPECIFIED;
    }

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) {
        assertAssigned();
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
