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

import static org.apache.commons.lang3.reflect.FieldUtils.getDeclaredField;
import static org.neo4j.io.fs.FileSystemAbstraction.INVALID_FILE_DESCRIPTOR;
import static org.neo4j.util.FeatureToggles.flag;

import java.io.FileDescriptor;
import java.io.IOException;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandles;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.io.pagecache.impl.SingleFilePageSwapper;

public class StoreFileChannel implements StoreChannel {
    private static final boolean PRINT_REFLECTION_EXCEPTIONS =
            flag(SingleFilePageSwapper.class, "printReflectionExceptions", false);
    private static final Class<?> CLS_FILE_CHANNEL_IMPL = getInternalFileChannelClass();
    private static final MethodHandle POSITION_LOCK_GETTER = getPositionLockGetter();
    private static final MethodHandle MAKE_CHANNEL_UNINTERRUPTIBLE = getUninterruptibleSetter();
    private static final MethodHandle CHANNEL_GET_FD = getChannelFileDescriptorGetter();
    private static final MethodHandle DESCRIPTOR_GET_FD = getFileDescriptorGetter();

    private static Class<?> getInternalFileChannelClass() {
        Class<?> cls = null;
        try {
            cls = Class.forName("sun.nio.ch.FileChannelImpl");
        } catch (Throwable throwable) {
            if (PRINT_REFLECTION_EXCEPTIONS) {
                throwable.printStackTrace();
            }
        }
        return cls;
    }

    private static MethodHandle unreflect(ThrowingFunction<MethodHandles.Lookup, MethodHandle, Exception> unreflector) {
        try {
            if (CLS_FILE_CHANNEL_IMPL != null) {
                MethodHandles.Lookup lookup = MethodHandles.lookup();
                return unreflector.apply(lookup);
            } else {
                return null;
            }
        } catch (Throwable e) {
            if (PRINT_REFLECTION_EXCEPTIONS) {
                e.printStackTrace();
            }
            return null;
        }
    }

    private static MethodHandle getUninterruptibleSetter() {
        return unreflect(lookup -> {
            Method uninterruptibleSetter = CLS_FILE_CHANNEL_IMPL.getMethod("setUninterruptible");
            return lookup.unreflect(uninterruptibleSetter);
        });
    }

    private static MethodHandle getPositionLockGetter() {
        return unreflect(lookup -> {
            Field positionLock = getDeclaredField(CLS_FILE_CHANNEL_IMPL, "positionLock", true);
            return lookup.unreflectGetter(positionLock);
        });
    }

    private static MethodHandle getChannelFileDescriptorGetter() {
        return unreflect(lookup -> {
            Field fd = getDeclaredField(CLS_FILE_CHANNEL_IMPL, "fd", true);
            return lookup.unreflectGetter(fd);
        });
    }

    private static MethodHandle getFileDescriptorGetter() {
        return unreflect(lookup -> {
            Field fd = getDeclaredField(FileDescriptor.class, "fd", true);
            return lookup.unreflectGetter(fd);
        });
    }

    private final FileChannel channel;

    public StoreFileChannel(FileChannel channel) {
        this.channel = channel;
    }

    public StoreFileChannel(StoreFileChannel channel) {
        this.channel = channel.channel;
    }

    @Override
    public long write(ByteBuffer[] srcs) throws IOException {
        return channel.write(srcs);
    }

    @Override
    public long write(ByteBuffer[] srcs, int offset, int length) throws IOException {
        return channel.write(srcs, offset, length);
    }

    @Override
    public void writeAll(ByteBuffer src, long position) throws IOException {
        long filePosition = position;
        long expectedEndPosition = filePosition + src.limit() - src.position();
        int bytesWritten;
        while ((filePosition += bytesWritten = channel.write(src, filePosition)) < expectedEndPosition) {
            if (bytesWritten < 0) {
                throw new IOException("Unable to write to disk, reported bytes written was " + bytesWritten);
            }
        }
    }

    @Override
    public void writeAll(ByteBuffer src) throws IOException {
        int bytesToWrite = src.remaining();
        int bytesWritten;
        while ((bytesToWrite -= bytesWritten = write(src)) > 0) {
            if (bytesWritten < 0) {
                throw new IOException("Unable to write to disk, reported bytes written was " + bytesWritten);
            }
        }
    }

    @Override
    public StoreFileChannel truncate(long size) throws IOException {
        channel.truncate(size);
        return this;
    }

    @Override
    public int getFileDescriptor() {
        if (channel.getClass() != CLS_FILE_CHANNEL_IMPL || CHANNEL_GET_FD == null || DESCRIPTOR_GET_FD == null) {
            return INVALID_FILE_DESCRIPTOR;
        }
        try {
            FileDescriptor fd = (FileDescriptor) CHANNEL_GET_FD.invoke(channel);
            return (int) DESCRIPTOR_GET_FD.invoke(fd);
        } catch (Throwable throwable) {
            if (PRINT_REFLECTION_EXCEPTIONS) {
                throwable.printStackTrace();
            }
        }
        return INVALID_FILE_DESCRIPTOR;
    }

    @Override
    public boolean hasPositionLock() {
        return POSITION_LOCK_GETTER != null && channel.getClass() == CLS_FILE_CHANNEL_IMPL;
    }

    @Override
    public Object getPositionLock() {
        if (POSITION_LOCK_GETTER == null) {
            return null;
        }
        try {
            return POSITION_LOCK_GETTER.invoke(channel);
        } catch (Throwable th) {
            throw new LinkageError("Cannot get FileChannel.positionLock", th);
        }
    }

    @Override
    public void tryMakeUninterruptible() {
        if (MAKE_CHANNEL_UNINTERRUPTIBLE != null && channel.getClass() == CLS_FILE_CHANNEL_IMPL) {
            try {
                MAKE_CHANNEL_UNINTERRUPTIBLE.invoke(channel);
            } catch (Throwable t) {
                throw new LinkageError("No setter for uninterruptible flag", t);
            }
        }
    }

    @Override
    public StoreFileChannel position(long newPosition) throws IOException {
        channel.position(newPosition);
        return this;
    }

    @Override
    public int read(ByteBuffer dst, long position) throws IOException {
        return channel.read(dst, position);
    }

    @Override
    public void readAll(ByteBuffer dst) throws IOException {
        while (dst.hasRemaining()) {
            int bytesRead = channel.read(dst);
            if (bytesRead < 0) {
                throw new IllegalStateException("Channel has reached end-of-stream.");
            }
        }
    }

    @Override
    public void readAll(ByteBuffer dst, long position) throws IOException {
        long filePosition = position;
        while (dst.hasRemaining()) {
            int bytesRead = channel.read(dst, filePosition);
            if (bytesRead < 0) {
                throw new IllegalStateException("Channel has reached end-of-stream.");
            }
            filePosition += bytesRead;
        }
    }

    @Override
    public void force(boolean metaData) throws IOException {
        channel.force(metaData);
    }

    @Override
    public int read(ByteBuffer dst) throws IOException {
        return channel.read(dst);
    }

    @Override
    public long read(ByteBuffer[] dsts, int offset, int length) throws IOException {
        return channel.read(dsts, offset, length);
    }

    @Override
    public long position() throws IOException {
        return channel.position();
    }

    @Override
    public FileLock tryLock() throws IOException {
        return channel.tryLock();
    }

    @Override
    public boolean isOpen() {
        return channel.isOpen();
    }

    @Override
    public long read(ByteBuffer[] dsts) throws IOException {
        return channel.read(dsts);
    }

    @Override
    public int write(ByteBuffer src) throws IOException {
        return channel.write(src);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    @Override
    public long size() throws IOException {
        return channel.size();
    }

    @Override
    public void flush() throws IOException {
        force(false);
    }
}
