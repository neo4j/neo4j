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
package org.neo4j.io.pagecache.impl;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.internal.unsafe.UnsafeUtil;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.pagecache.impl.muninn.MuninnPageCache;

/**
 * BlockSwapper that uses reflection to wrap buffer address into ByteBuffer proxy to directly read from/write to StoreChannel
 */
final class UnsafeBlockSwapper implements BlockSwapper {
    private static final ThreadLocal<ByteBuffer> PROXY_CACHE = new ThreadLocal<>();

    private static ByteBuffer proxy(long buffer, int bufferLength) throws IOException {
        ByteBuffer buf = PROXY_CACHE.get();
        if (buf != null) {
            if (buf.capacity() != bufferLength) {
                return createAndGetNewBuffer(buffer, bufferLength);
            }
            UnsafeUtil.initDirectByteBuffer(buf, buffer, bufferLength);
            return buf;
        }
        return createAndGetNewBuffer(buffer, bufferLength);
    }

    private static ByteBuffer createAndGetNewBuffer(long buffer, int bufferLength) throws IOException {
        ByteBuffer buf;
        try {
            buf = UnsafeUtil.newDirectByteBuffer(buffer, bufferLength);
        } catch (Throwable e) {
            throw new IOException(e);
        }
        PROXY_CACHE.set(buf);
        return buf;
    }

    @Override
    public int swapIn(StoreChannel channel, long bufferAddress, long fileOffset, int bufferSize) throws IOException {
        int readTotal = 0;
        try {
            ByteBuffer bufferProxy;
            bufferProxy = proxy(bufferAddress, bufferSize);
            int read;
            do {
                read = channel.read(bufferProxy, fileOffset + readTotal);
            } while (read != -1 && (readTotal += read) < bufferSize);

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
        try {
            // direct write from memory to channel using proxy
            var bufferProxy = proxy(bufferAddress, bufferLength);
            channel.writeAll(bufferProxy, fileOffset);
        } catch (IOException e) {
            throw e;
        } catch (Throwable e) {
            throw new IOException(e);
        }
    }
}
