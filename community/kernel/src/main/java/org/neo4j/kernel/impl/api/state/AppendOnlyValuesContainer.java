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
package org.neo4j.kernel.impl.api.state;

import static java.lang.Math.max;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkArgument;
import static org.neo4j.util.Preconditions.checkState;
import static org.neo4j.values.storable.ValueByteBufferCodec.VALUE_TYPES;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.neo4j.io.ByteUnit;
import org.neo4j.kernel.impl.util.collection.Memory;
import org.neo4j.kernel.impl.util.collection.MemoryAllocator;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.VisibleForTesting;
import org.neo4j.values.storable.Value;
import org.neo4j.values.storable.ValueByteBufferCodec;
import org.neo4j.values.storable.ValueByteBufferCodec.Writer;

public class AppendOnlyValuesContainer implements ValuesContainer {
    private static final int CHUNK_SIZE = (int) ByteUnit.kibiBytes(512);
    private static final int REMOVED = 0xFF;

    private final int chunkSize;
    private final List<ByteBuffer> chunks = new ArrayList<>();
    private final List<Memory> allocated = new ArrayList<>();
    private final Writer writer;
    private final MemoryAllocator allocator;
    private final MemoryTracker memoryTracker;
    private ByteBuffer currentChunk;
    private boolean closed;

    public AppendOnlyValuesContainer(MemoryAllocator allocator, MemoryTracker memoryTracker) {
        this(CHUNK_SIZE, allocator, memoryTracker);
    }

    @VisibleForTesting
    AppendOnlyValuesContainer(int chunkSize, MemoryAllocator allocator, MemoryTracker memoryTracker) {
        this.chunkSize = chunkSize;
        this.allocator = allocator;
        this.memoryTracker = memoryTracker;
        this.writer = new Writer(chunkSize, new OffHeapByteBufferAllocator(allocator, memoryTracker));
    }

    @Override
    public long add(Value value) {
        assertNotClosed();
        requireNonNull(value, "value cannot be null");
        final ByteBuffer buf = writer.write(value);
        if (currentChunk == null || buf.remaining() > currentChunk.remaining()) {
            currentChunk = addNewChunk(max(chunkSize, buf.remaining()));
        }

        final long ref = ((chunks.size() - 1L) << 32) | currentChunk.position();
        currentChunk.put(buf);
        return ref;
    }

    @Override
    public Value get(long ref) {
        assertNotClosed();
        final int chunkIdx = (int) (ref >>> 32);
        int offset = (int) ref;

        checkArgument(
                chunkIdx >= 0 && chunkIdx < chunks.size(),
                "invalid chunk idx %d (total #%d chunks), ref: 0x%X",
                chunkIdx,
                chunks.size(),
                ref);
        final ByteBuffer chunk = chunks.get(chunkIdx);
        checkArgument(offset >= 0 && offset < chunk.position(), "invalid chunk offset (%d), ref: 0x%X", offset, ref);
        final int typeId = chunk.get(offset) & 0xFF;
        checkArgument(typeId != REMOVED, "element is already removed, ref: 0x%X", ref);
        checkArgument(typeId < VALUE_TYPES.length, "invaling typeId (%d) for ref 0x%X", typeId, ref);
        offset++;

        final ValueByteBufferCodec.ValueType type = VALUE_TYPES[typeId];
        return type.getReader().read(chunk, offset);
    }

    @Override
    public Value remove(long ref) {
        assertNotClosed();
        final Value removed = get(ref);
        final int chunkIdx = (int) (ref >>> 32);
        final int chunkOffset = (int) ref;
        final ByteBuffer chunk = chunks.get(chunkIdx);
        chunk.put(chunkOffset, (byte) REMOVED);
        return removed;
    }

    @Override
    public void close() {
        assertNotClosed();
        closed = true;
        allocated.forEach(m -> m.free(memoryTracker));
        allocated.clear();
        chunks.clear();
        writer.close();
        currentChunk = null;
    }

    private void assertNotClosed() {
        checkState(!closed, "Container is closed");
    }

    private ByteBuffer addNewChunk(int size) {
        final Memory memory = allocator.allocate(size, false, memoryTracker);
        final ByteBuffer chunk = memory.asByteBuffer();
        allocated.add(memory);
        chunks.add(chunk);
        return chunk;
    }

    private static class OffHeapByteBufferAllocator implements ValueByteBufferCodec.ByteBufferAllocator {
        private final MemoryAllocator allocator;
        private final MemoryTracker memoryTracker;
        private Memory bufMemory;

        public OffHeapByteBufferAllocator(MemoryAllocator allocator, MemoryTracker memoryTracker) {
            this.allocator = allocator;
            this.memoryTracker = memoryTracker;
        }

        @Override
        public ByteBuffer allocate(long capacity) {
            this.bufMemory = allocator.allocate(capacity, false, memoryTracker);
            return bufMemory.asByteBuffer();
        }

        @Override
        public void free() {
            bufMemory.free(memoryTracker);
            bufMemory = null;
        }
    }
}
