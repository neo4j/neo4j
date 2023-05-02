/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.storageengine.api.enrichment;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.BufferOverflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import org.neo4j.collection.trackable.HeapTrackingArrayList;
import org.neo4j.collection.trackable.HeapTrackingCollections;
import org.neo4j.io.fs.WritableChannel;
import org.neo4j.memory.MemoryTracker;

public class WriteEnrichmentChannel implements WritableChannel {

    public static final int CHUNK_SIZE = 32768;

    private final HeapTrackingArrayList<ByteBuffer> chunks;
    private final MemoryTracker memoryTracker;

    private ByteBuffer currentChunk;
    private boolean isClosed;

    public WriteEnrichmentChannel(MemoryTracker memoryTracker) {
        this.memoryTracker = requireNonNull(memoryTracker);
        this.chunks = HeapTrackingCollections.newArrayList(1, memoryTracker);
    }

    /**
     * @param channel the channel to write the data to
     * @throws IOException if unable to write the data
     */
    public void serialize(WritableChannel channel) throws IOException {
        for (var chunk : chunks) {
            channel.putAll(chunk.flip());
        }
    }

    /**
     * @return <code>true</code> if this channel has any data in it
     */
    public boolean isEmpty() {
        return chunks.isEmpty();
    }

    /**
     * @return the current position at the end of the channel
     */
    public int position() {
        var pos = 0;
        for (final ByteBuffer chunk : chunks) {
            pos += chunk.position();
        }
        return pos;
    }

    public byte peek(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.get(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public char peekChar(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getChar(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public short peekShort(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getShort(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public int peekInt(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getInt(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public long peekLong(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getLong(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public float peekFloat(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getFloat(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public double peekDouble(int position) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                return chunk.getDouble(position);
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel put(int position, byte value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.put(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putShort(int position, short value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.putShort(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putInt(int position, int value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.putInt(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putLong(int position, long value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.putLong(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putFloat(int position, float value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.putFloat(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putDouble(int position, double value) {
        for (var chunk : chunks) {
            final var endOfChunk = chunk.position();
            if (position < endOfChunk) {
                chunk.putDouble(position, value);
                return this;
            }

            position -= endOfChunk;
        }

        throw new BufferOverflowException();
    }

    public WriteEnrichmentChannel putChar(char value) {
        ensureCapacityForWrite(Character.BYTES).putChar(value);
        return this;
    }

    public WriteEnrichmentChannel put(byte[] value) {
        return put(value, value.length);
    }

    @Override
    public WriteEnrichmentChannel put(byte value) {
        ensureCapacityForWrite(Byte.BYTES).put(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel putShort(short value) {
        ensureCapacityForWrite(Short.BYTES).putShort(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel putInt(int value) {
        ensureCapacityForWrite(Integer.BYTES).putInt(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel putLong(long value) {
        ensureCapacityForWrite(Long.BYTES).putLong(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel putFloat(float value) {
        ensureCapacityForWrite(Float.BYTES).putFloat(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel putDouble(double value) {
        ensureCapacityForWrite(Double.BYTES).putDouble(value);
        return this;
    }

    @Override
    public WriteEnrichmentChannel put(byte[] value, int length) {
        return put(value, 0, length);
    }

    @Override
    public WriteEnrichmentChannel put(byte[] value, int offset, int length) {
        var pos = offset;
        while (pos < length) {
            // get the tail buffer and add as much of the bytes as we can
            final var buffer = ensureCapacityForWrite(1);
            final var available = Math.min(length - pos, buffer.remaining());
            buffer.put(value, pos, available);
            pos += available;
        }
        return this;
    }

    @Override
    public WriteEnrichmentChannel putAll(ByteBuffer src) {
        write(src);
        return this;
    }

    @Override
    public int write(ByteBuffer src) {
        final var allToWrite = src.remaining();
        var remaining = allToWrite;
        while (remaining > 0) {
            // get the tail buffer and add as much of the bytes as we can
            final var buffer = ensureCapacityForWrite(1);
            final var toWrite = Math.min(buffer.remaining(), remaining);

            final var destPos = buffer.position();
            final var srcPos = src.position();
            buffer.put(destPos, src, srcPos, toWrite).position(destPos + toWrite);
            buffer.position(destPos + toWrite);
            src.position(srcPos + toWrite);
            remaining -= toWrite;
        }
        return allToWrite;
    }

    @Override
    public int putChecksum() throws IOException {
        // no-op
        return 0;
    }

    @Override
    public void beginChecksum() {
        // no-op
    }

    @Override
    public boolean isOpen() {
        return !isClosed;
    }

    @Override
    public void close() {
        if (!isClosed) {
            isClosed = true;
            memoryTracker.releaseHeap((long) chunks.size() * CHUNK_SIZE);
            chunks.close();
        }
    }

    private ByteBuffer ensureCapacityForWrite(int size) {
        if (chunks.isEmpty()) {
            return newChunk();
        }

        if (currentChunk.remaining() < size) {
            return newChunk();
        }

        return currentChunk;
    }

    private ByteBuffer newChunk() {
        memoryTracker.allocateHeap(CHUNK_SIZE);
        currentChunk = ByteBuffer.allocate(CHUNK_SIZE).order(ByteOrder.LITTLE_ENDIAN);
        chunks.add(currentChunk);
        return currentChunk;
    }
}
