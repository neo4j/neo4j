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
package org.neo4j.kernel.impl.index.schema;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Path;
import org.neo4j.io.fs.ReadableChannel;
import org.neo4j.io.pagecache.CursorException;
import org.neo4j.io.pagecache.PageCursor;
import org.neo4j.io.pagecache.PagedFile;

class ReadableChannelPageCursor extends PageCursor {
    private final ReadableChannel channel;
    private CursorException cursorException;

    ReadableChannelPageCursor(ReadableChannel channel) {
        this.channel = channel;
    }

    @Override
    public byte getByte() {
        try {
            return channel.get();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public byte getByte(int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putByte(byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putByte(int offset, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getLong() {
        try {
            return channel.getLong();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public long getLong(int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong(long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putLong(int offset, long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getInt() {
        try {
            return channel.getInt();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public int getInt(int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt(int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putInt(int offset, int value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void getBytes(byte[] data) {
        getBytes(data, 0, data.length);
    }

    @Override
    public void getBytes(byte[] data, int arrayOffset, int length) {
        if (arrayOffset != 0) {
            throw new UnsupportedOperationException();
        }

        try {
            channel.get(data, length);
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public void putBytes(byte[] data) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBytes(byte[] data, int arrayOffset, int length) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putBytes(int bytes, byte value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public short getShort() {
        try {
            return channel.getShort();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public short getShort(int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putShort(short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void putShort(int offset, short value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOffset(int offset) {
        throw new UnsupportedOperationException();
    }

    @Override
    public int getOffset() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void mark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public void setOffsetToMark() {
        throw new UnsupportedOperationException();
    }

    @Override
    public long getCurrentPageId() {
        return 0;
    }

    @Override
    public Path getCurrentFile() {
        return null;
    }

    @Override
    public PagedFile getPagedFile() {
        return null;
    }

    @Override
    public Path getRawCurrentFile() {
        return null;
    }

    @Override
    public boolean next() throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean next(long pageId) throws IOException {
        throw new UnsupportedOperationException();
    }

    @Override
    public void close() {
        try {
            channel.close();
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    @Override
    public boolean shouldRetry() {
        return false;
    }

    @Override
    public void copyPage(PageCursor targetCursor) {
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
        throw new UnsupportedOperationException();
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
                clearCursorException();
            }
        }
    }

    @Override
    public void setCursorException(String message) {
        this.cursorException = new CursorException(message);
    }

    @Override
    public void clearCursorException() {
        cursorException = null;
    }

    @Override
    public PageCursor openLinkedCursor(long pageId) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void zapPage() {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isWriteLocked() {
        return false;
    }

    @Override
    public void setPageHorizon(long horizon) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void unpin() {}

    @Override
    public ByteOrder getByteOrder() {
        return ByteOrder.LITTLE_ENDIAN;
    }
}
