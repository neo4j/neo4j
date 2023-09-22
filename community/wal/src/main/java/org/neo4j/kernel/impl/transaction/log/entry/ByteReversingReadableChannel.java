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
package org.neo4j.kernel.impl.transaction.log.entry;

import java.io.IOException;
import java.nio.ByteBuffer;
import org.neo4j.io.fs.ChecksumMismatchException;
import org.neo4j.io.fs.ReadableChannel;

/**
 * ReadableChecksumChannel that reverses bytes of short, int, and long types read from delegate
 */
public class ByteReversingReadableChannel implements ReadableChannel {
    private final ReadableChannel delegate;

    public ByteReversingReadableChannel(ReadableChannel delegate) {
        this.delegate = delegate;
    }

    @Override
    public byte get() throws IOException {
        return delegate.get();
    }

    @Override
    public short getShort() throws IOException {
        return Short.reverseBytes(delegate.getShort());
    }

    @Override
    public int getInt() throws IOException {
        return Integer.reverseBytes(delegate.getInt());
    }

    @Override
    public long getLong() throws IOException {
        return Long.reverseBytes(delegate.getLong());
    }

    @Override
    public float getFloat() throws IOException {
        return delegate.getFloat();
    }

    @Override
    public double getDouble() throws IOException {
        return delegate.getDouble();
    }

    @Override
    public void get(byte[] bytes, int length) throws IOException {
        delegate.get(bytes, length);
    }

    @Override
    public byte getVersion() throws IOException {
        return delegate.getVersion();
    }

    @Override
    public void beginChecksum() {
        delegate.beginChecksum();
    }

    @Override
    public int getChecksum() {
        return delegate.getChecksum();
    }

    @Override
    public int endChecksumAndValidate() throws IOException {
        // Validate checksum
        int calculatedChecksum = getChecksum();
        int checksum = getInt();
        if (calculatedChecksum != checksum) {
            throw new ChecksumMismatchException(checksum, calculatedChecksum);
        }
        beginChecksum();

        return calculatedChecksum;
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate.read(dst);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public void position(long byteOffset) throws IOException {
        delegate.position(byteOffset);
    }
}
