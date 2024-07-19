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
package org.neo4j.io.pagecache;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import java.util.Arrays;
import org.eclipse.collections.api.map.primitive.MutableLongObjectMap;
import org.eclipse.collections.impl.factory.primitive.LongObjectMaps;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.monitoring.PageFileCounters;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.io.pagecache.tracing.PageFileSwapperTracer;
import org.neo4j.io.pagecache.tracing.version.FileTruncateEvent;

/**
 * Wraps a byte array and present it as a PageCursor.
 * <p>
 * All the accessor methods (getXXX, putXXX) are implemented and delegates calls to its internal {@link ByteBuffer}.
 * {@link #setOffset(int)} and {@link #getOffset()} positions the internal {@link ByteBuffer}.
 * {@link #shouldRetry()} always returns {@code false}.
 */
public class ByteArrayPageCursor extends PageCursor {
    private static final long DEFAULT_PAGE_ID = 0;
    private final MutableLongObjectMap<ByteBuffer> buffers;
    private long pageId;
    // If this is false then the next call to next() will just set it to true, this to adhere to the general PageCursor
    // interaction contract
    private boolean initialized;
    private ByteBuffer buffer;
    private CursorException cursorException;

    public static PageCursor wrap(byte[] array, int offset, int length, long currentPageId) {
        return new ByteArrayPageCursor(
                currentPageId, ByteBuffer.wrap(array, offset, length).order(ByteOrder.LITTLE_ENDIAN));
    }

    public static PageCursor wrap(byte[] array, int offset, int length) {
        return wrap(array, offset, length, DEFAULT_PAGE_ID);
    }

    public static PageCursor wrap(byte[] array) {
        return wrap(array, 0, array.length);
    }

    public static PageCursor wrap(int length) {
        return wrap(new byte[length]);
    }

    public ByteArrayPageCursor(ByteBuffer buffer) {
        this(DEFAULT_PAGE_ID, buffer);
    }

    public ByteArrayPageCursor(long pageId, ByteBuffer buffer) {
        buffers = LongObjectMaps.mutable.empty();
        buffers.put(pageId, buffer);
        this.pageId = pageId;
        this.buffer = buffer;
        this.initialized = true;
    }

    public ByteArrayPageCursor(MutableLongObjectMap<ByteBuffer> buffers, long pageId) {
        this.buffers = buffers;
        this.pageId = pageId;
        this.initialized = false;
        this.buffer = buffers.get(pageId);
    }

    @Override
    public byte getByte() {
        return buffer.get();
    }

    @Override
    public byte getByte(int offset) {
        return buffer.get(offset);
    }

    @Override
    public void putByte(byte value) {
        buffer.put(value);
    }

    @Override
    public void putByte(int offset, byte value) {
        buffer.put(offset, value);
    }

    @Override
    public long getLong() {
        return buffer.getLong();
    }

    @Override
    public long getLong(int offset) {
        return buffer.getLong(offset);
    }

    @Override
    public void putLong(long value) {
        buffer.putLong(value);
    }

    @Override
    public void putLong(int offset, long value) {
        buffer.putLong(offset, value);
    }

    @Override
    public int getInt() {
        return buffer.getInt();
    }

    @Override
    public int getInt(int offset) {
        return buffer.getInt(offset);
    }

    @Override
    public void putInt(int value) {
        buffer.putInt(value);
    }

    @Override
    public void putInt(int offset, int value) {
        buffer.putInt(offset, value);
    }

    @Override
    public void getBytes(byte[] data) {
        buffer.get(data);
    }

    @Override
    public void getBytes(byte[] data, int arrayOffset, int length) {
        buffer.get(data, arrayOffset, length);
    }

    @Override
    public void putBytes(byte[] data) {
        buffer.put(data);
    }

    @Override
    public void putBytes(byte[] data, int arrayOffset, int length) {
        buffer.put(data, arrayOffset, length);
    }

    @Override
    public void putBytes(int bytes, byte value) {
        byte[] byteArray = new byte[bytes];
        Arrays.fill(byteArray, value);
        buffer.put(byteArray);
    }

    @Override
    public short getShort() {
        return buffer.getShort();
    }

    @Override
    public short getShort(int offset) {
        return buffer.getShort(offset);
    }

    @Override
    public void putShort(short value) {
        buffer.putShort(value);
    }

    @Override
    public void putShort(int offset, short value) {
        buffer.putShort(offset, value);
    }

    @Override
    public void setOffset(int offset) {
        buffer.position(offset);
    }

    @Override
    public int getOffset() {
        return buffer.position();
    }

    @Override
    public void mark() {
        buffer.mark();
    }

    @Override
    public void setOffsetToMark() {
        buffer.reset();
    }

    @Override
    public long getCurrentPageId() {
        return pageId;
    }

    @Override
    public Path getCurrentFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public PagedFile getPagedFile() {
        return new ByteArrayPagedFile(buffer.capacity());
    }

    @Override
    public Path getRawCurrentFile() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean next() {
        if (!initialized) {
            initialized = true;
            return true;
        }
        return next(pageId + 1);
    }

    @Override
    public boolean next(long pageId) {
        this.initialized = true;
        this.pageId = pageId;
        if (buffers.containsKey(pageId)) {
            buffer = buffers.get(pageId);
        } else {
            buffer = ByteBuffer.allocate(buffer.capacity());
            buffers.put(pageId, buffer);
        }
        return true;
    }

    @Override
    public void close() { // Nothing to close
    }

    @Override
    public boolean shouldRetry() {
        return false;
    }

    @Override
    public void copyPage(PageCursor target) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int copyTo(int sourceOffset, PageCursor targetCursor, int targetOffset, int lengthInBytes) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int copyTo(int sourceOffset, ByteBuffer targetBuffer) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int copyFrom(ByteBuffer sourceBuffer, int targetOffset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void shiftBytes(int sourceOffset, int length, int shift) {
        int currentOffset = getOffset();
        setOffset(sourceOffset);
        byte[] bytes = new byte[length];
        getBytes(bytes);
        setOffset(sourceOffset + shift);
        putBytes(bytes);
        setOffset(currentOffset);
    }

    @Override
    public boolean checkAndClearBoundsFlag() {
        return false;
    }

    @Override
    public void checkAndClearCursorException() throws CursorException {
        if (cursorException != null) {
            try {
                throw cursorException;
            } finally {
                cursorException = null;
            }
        }
    }

    @Override
    public void setCursorException(String message) {
        cursorException = Exceptions.chain(cursorException, new CursorException(message));
    }

    @Override
    public void clearCursorException() { // Don't check
    }

    @Override
    public PageCursor openLinkedCursor(long pageId) {
        if (!buffers.containsKey(pageId)) {
            buffers.put(pageId, ByteBuffer.allocate(buffer.capacity()));
        }
        return new ByteArrayPageCursor(buffers, pageId);
    }

    @Override
    public void zapPage() {
        Arrays.fill(buffer.array(), (byte) 0);
    }

    @Override
    public boolean isWriteLocked() {
        // Because we allow writes; they can't possibly conflict because this class is meant to be used by only one
        // thread at a time anyway.
        return true;
    }

    @Override
    public void setPageHorizon(long horizon) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unpin() {}

    @Override
    public ByteOrder getByteOrder() {
        return buffer.order();
    }

    private record ByteArrayPagedFile(int pageSize) implements PagedFile {
        @Override
        public PageCursor io(long pageId, int pf_flags, CursorContext context) {
            throw new UnsupportedOperationException();
        }

        @Override
        public int pageSize() {
            return pageSize;
        }

        @Override
        public int payloadSize() {
            return pageSize;
        }

        @Override
        public int pageReservedBytes() {
            return 0;
        }

        @Override
        public long fileSize() {
            return pageSize;
        }

        @Override
        public Path path() {
            return null;
        }

        @Override
        public void flushAndForce(FileFlushEvent flushEvent) {}

        @Override
        public long getLastPageId() {
            return 0;
        }

        @Override
        public void increaseLastPageIdTo(long newLastPageId) {}

        @Override
        public void close() {}

        @Override
        public void setDeleteOnClose(boolean deleteOnClose) {}

        @Override
        public boolean isDeleteOnClose() {
            return false;
        }

        @Override
        public String getDatabaseName() {
            return null;
        }

        @Override
        public PageFileCounters pageFileCounters() {
            return PageFileSwapperTracer.NULL;
        }

        @Override
        public boolean isMultiVersioned() {
            return false;
        }

        @Override
        public void truncate(long pagesToKeep, FileTruncateEvent truncateEvent) {}

        @Override
        public int touch(long pageId, int count, CursorContext cursorContext) {
            return 0;
        }

        @Override
        public boolean preAllocateSupported() {
            return false;
        }

        @Override
        public void preAllocate(long newFileSizeInPages) {}
    }
}
