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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

public class DelegatingStoreChannel<T extends StoreChannel> implements StoreChannel {
    protected final T delegate;

    public DelegatingStoreChannel(T delegate) {
        this.delegate = delegate;
    }

    @Override
    public FileLock tryLock() throws IOException {
        return delegate.tryLock();
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return delegate.write(srcs, offset, length);
    }

    @Override
    public void close() throws IOException {
        delegate.close();
    }

    @Override
    public void writeAll(ByteBuffer src, long position) throws IOException {
        delegate.writeAll(src, position);
    }

    @Override
    public StoreChannel truncate(long size) throws IOException {
        delegate.truncate(size);
        return this;
    }

    @Override
    public int getFileDescriptor() {
        return delegate.getFileDescriptor();
    }

    @Override
    public void writeAll(ByteBuffer src) throws IOException {
        delegate.writeAll(src);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return delegate.write(src);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return delegate.read(dsts, offset, length);
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return delegate.write(srcs);
    }

    @Override
    public boolean isOpen() {
        return delegate.isOpen();
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return delegate.read(dst);
    }

    @Override
    public void force(boolean metaData) throws IOException {
        delegate.force(metaData);
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return delegate.read(dsts);
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return delegate.read(dst, position);
    }

    @Override
    public void readAll(ByteBuffer dst) throws IOException {
        delegate.readAll(dst);
    }

    @Override
    public void readAll(ByteBuffer dst, long position) throws IOException {
        delegate.readAll(dst, position);
    }

    @Override
    public long position() throws IOException {
        return delegate.position();
    }

    @Override
    public long size() throws IOException {
        return delegate.size();
    }

    @Override
    public StoreChannel position(long newPosition) throws IOException {
        delegate.position(newPosition);
        return this;
    }

    @Override
    public void flush() throws IOException {
        delegate.flush();
    }

    @Override
    public boolean hasPositionLock() {
        return delegate.hasPositionLock();
    }

    @Override
    public Object getPositionLock() {
        return delegate.getPositionLock();
    }

    @Override
    public void tryMakeUninterruptible() {
        delegate.tryMakeUninterruptible();
    }
}
