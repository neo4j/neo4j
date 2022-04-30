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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import java.nio.ByteOrder;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.memory.HeapScopedBuffer;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;
import org.neo4j.memory.MemoryTracker;

/**
 * BlockSwapper to use when reflection access to java.nio.DirectByteBuffer isn't available. Uses temporaty buffer to read from/write to StoreChannel
 */
final class FallbackBlockSwapper implements BlockSwapper {

    private final MemoryTracker memoryTracker;

    FallbackBlockSwapper(MemoryTracker memoryTracker) {
        this.memoryTracker = memoryTracker;
    }

    @Override
    public int swapIn(StoreChannel channel, long bufferAddress, long fileOffset, int bufferSize) throws IOException {
        int readTotal = 0;
        try (var scopedBuffer = new HeapScopedBuffer(bufferSize, ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();
            int read;
            do {
                read = channel.read(buffer, fileOffset + readTotal);
            } while (read != -1 && (readTotal += read) < bufferSize);

            buffer.flip();
            for (int i = 0; i < readTotal; i++) {
                byte b = buffer.get();
                UnsafeUtil.putByte(bufferAddress + i, b);
            }

            // Zero-fill the rest.
            int rest = bufferSize - readTotal;
            if (rest > 0) {
                UnsafeUtil.setMemory(bufferAddress + readTotal, rest, MuninnPageCache.ZERO_BYTE);
            }
            return readTotal;
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(formatSwapInErrorMessage(fileOffset, bufferSize, readTotal), e);
        }
    }

    private static String formatSwapInErrorMessage(long fileOffset, int size, int readTotal) {
        return "Read failed after " + readTotal + " of " + size + " bytes from fileOffset " + fileOffset + ".";
    }

    @Override
    public void swapOut(StoreChannel channel, long bufferAddress, long fileOffset, int bufferLength)
            throws IOException {
        try (var scopedBuffer = new HeapScopedBuffer(bufferLength, ByteOrder.LITTLE_ENDIAN, memoryTracker)) {
            var buffer = scopedBuffer.getBuffer();

            for (int i = 0; i < bufferLength; i++) {
                byte b = UnsafeUtil.getByte(bufferAddress + i);
                buffer.put(b);
            }
            buffer.flip();
            channel.writeAll(buffer, fileOffset);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }
}
