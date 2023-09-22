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

import java.io.Flushable;
import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.FlushableChannel;
import org.neo4j.io.fs.PhysicalFlushableChannel;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.io.memory.ScopedBuffer;

/**
 * Decorator around a {@link LogVersionedStoreChannel} making it expose {@link FlushableLogPositionAwareChannel}. This
 * implementation uses a {@link PhysicalFlushableChannel}, which provides buffering for write operations over the
 * decorated channel.
 */
public class PhysicalFlushableLogPositionAwareChannel implements FlushableLogPositionAwareChannel {
    private final PhysicalFlushableLogChannel channel;
    private LogVersionedStoreChannel logVersionedStoreChannel;

    public PhysicalFlushableLogPositionAwareChannel(
            LogVersionedStoreChannel logVersionedStoreChannel, ScopedBuffer buffer) {
        this.logVersionedStoreChannel = logVersionedStoreChannel;
        this.channel = new PhysicalFlushableLogChannel(logVersionedStoreChannel, buffer);
    }

    @Override
    public LogPositionMarker getCurrentLogPosition(LogPositionMarker positionMarker) throws IOException {
        positionMarker.mark(logVersionedStoreChannel.getLogVersion(), channel.position());
        return positionMarker;
    }

    @Override
    public LogPosition getCurrentLogPosition() throws IOException {
        return new LogPosition(logVersionedStoreChannel.getLogVersion(), channel.position());
    }

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        if (positionMarker.getLogVersion() != logVersionedStoreChannel.getLogVersion()) {
            throw new IllegalArgumentException("Log position points log version %d but the current one is %d"
                    .formatted(positionMarker.getLogVersion(), logVersionedStoreChannel.getLogVersion()));
        }
        logVersionedStoreChannel.position(positionMarker.getByteOffset());
    }

    @Override
    public Flushable prepareForFlush() throws IOException {
        return channel.prepareForFlush();
    }

    @Override
    public int putChecksum() throws IOException {
        return channel.putChecksum();
    }

    @Override
    public void beginChecksumForWriting() {
        channel.beginChecksumForWriting();
    }

    @Override
    public FlushableChannel put(byte value) throws IOException {
        return channel.put(value);
    }

    @Override
    public FlushableChannel putShort(short value) throws IOException {
        return channel.putShort(value);
    }

    @Override
    public FlushableChannel putInt(int value) throws IOException {
        return channel.putInt(value);
    }

    @Override
    public FlushableChannel putLong(long value) throws IOException {
        return channel.putLong(value);
    }

    @Override
    public FlushableChannel putFloat(float value) throws IOException {
        return channel.putFloat(value);
    }

    @Override
    public FlushableChannel putDouble(double value) throws IOException {
        return channel.putDouble(value);
    }

    @Override
    public FlushableChannel put(byte[] value, int offset, int length) throws IOException {
        return channel.put(value, offset, length);
    }

    @Override
    public FlushableChannel putAll(ByteBuffer src) throws IOException {
        return channel.putAll(src);
    }

    @Override
    public WritableChannel putVersion(byte version) throws IOException {
        return channel.putVersion(version);
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public int write(ByteBuffer buffer) throws IOException {
        int remaining = buffer.remaining();
        logVersionedStoreChannel.writeAll(buffer);
        return remaining;
    }

    public void setChannel(LogVersionedStoreChannel channel) {
        this.logVersionedStoreChannel = channel;
        this.channel.setChannel(channel);
    }
}
