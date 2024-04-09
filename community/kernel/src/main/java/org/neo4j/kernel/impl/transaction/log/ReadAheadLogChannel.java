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
import java.nio.ByteOrder;
import org.neo4j.io.fs.ReadAheadChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.io.memory.ScopedBuffer;
import org.neo4j.kernel.impl.transaction.log.entry.LogFormat;
import org.neo4j.memory.MemoryTracker;

/**
 * Basically a sequence of {@link StoreChannel channels} seamlessly seen as one.
 */
public class ReadAheadLogChannel extends ReadAheadChannel<LogVersionedStoreChannel> implements ReadableLogChannel {
    private final LogVersionBridge bridge;
    private final boolean raw;

    public ReadAheadLogChannel(LogVersionedStoreChannel startingChannel, MemoryTracker memoryTracker) {
        this(
                startingChannel,
                LogVersionBridge.NO_MORE_CHANNELS,
                new NativeScopedBuffer(DEFAULT_READ_AHEAD_SIZE, ByteOrder.LITTLE_ENDIAN, memoryTracker),
                false);
    }

    public ReadAheadLogChannel(
            LogVersionedStoreChannel startingChannel, LogVersionBridge bridge, MemoryTracker memoryTracker) {
        this(
                startingChannel,
                bridge,
                new NativeScopedBuffer(DEFAULT_READ_AHEAD_SIZE, ByteOrder.LITTLE_ENDIAN, memoryTracker),
                false);
    }

    public ReadAheadLogChannel(
            LogVersionedStoreChannel startingChannel,
            LogVersionBridge bridge,
            MemoryTracker memoryTracker,
            boolean raw) {
        this(
                startingChannel,
                bridge,
                new NativeScopedBuffer(DEFAULT_READ_AHEAD_SIZE, ByteOrder.LITTLE_ENDIAN, memoryTracker),
                raw);
    }

    protected ReadAheadLogChannel(LogVersionedStoreChannel startingChannel, ScopedBuffer scopedBuffer, boolean raw) {
        this(startingChannel, LogVersionBridge.NO_MORE_CHANNELS, scopedBuffer, raw);
    }

    /**
     * This constructor is private to ensure that the given buffer always comes form one of our own constructors.
     */
    private ReadAheadLogChannel(
            LogVersionedStoreChannel startingChannel, LogVersionBridge bridge, ScopedBuffer scopedBuffer, boolean raw) {
        super(startingChannel, scopedBuffer);
        this.bridge = bridge;
        this.raw = raw;
    }

    @Override
    public long getLogVersion() {
        return channel.getLogVersion();
    }

    @Override
    public LogFormat getLogFormatVersion() {
        return channel.getLogFormatVersion();
    }

    @Override
    public byte markAndGetVersion(LogPositionMarker marker) throws IOException {
        final var currentMarker = getCurrentLogPosition(marker);
        final var data = getVersion();
        if (!currentMarker.isMarkerInLog(channel.getLogVersion())) {
            // reading the byte forced the channel to move to the next log - let's re-mark at the correct location
            marker.mark(channel.getLogVersion(), position() - Byte.BYTES);
        }
        return data;
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

    @Override
    public void setLogPosition(LogPositionMarker positionMarker) throws IOException {
        if (positionMarker.getLogVersion() != channel.getLogVersion()) {
            throw new IllegalArgumentException("Log position points log version %d but the current one is %d"
                    .formatted(positionMarker.getLogVersion(), channel.getLogVersion()));
        }
        channel.position(positionMarker.getByteOffset());
    }

    @Override
    protected LogVersionedStoreChannel next(LogVersionedStoreChannel channel) throws IOException {
        return bridge.next(channel, raw);
    }

    @Override
    public void setCurrentPosition(long byteOffset) throws IOException {
        channel.position(byteOffset);
    }
}
