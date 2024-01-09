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

import static java.lang.String.format;
import static java.nio.file.StandardOpenOption.APPEND;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.READ;
import static java.nio.file.StandardOpenOption.WRITE;
import static org.neo4j.io.ByteUnit.kibiBytes;
import static org.neo4j.io.fs.FileUtils.toBufferedStream;

import java.io.BufferedInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.WatchService;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.neo4j.io.fs.watcher.DefaultFileSystemWatcher;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.memory.NativeScopedBuffer;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.util.VisibleForTesting;

/**
 * Default file system abstraction that creates files using the underlying file system.
 */
public class DefaultFileSystemAbstraction implements FileSystemAbstraction {
    static final String UNABLE_TO_CREATE_DIRECTORY_FORMAT = "Unable to write directory path [%s] for Neo4j store.";
    public static final Set<OpenOption> WRITE_OPTIONS = Set.of(READ, WRITE, CREATE);
    private static final Set<OpenOption> READ_OPTIONS = Set.of(READ);
    private static final Set<OpenOption> APPEND_OPTIONS = Set.of(CREATE, APPEND);

    @Override
    public FileWatcher fileWatcher() throws IOException {
        WatchService watchService = FileSystems.getDefault().newWatchService();
        return new DefaultFileSystemWatcher(watchService);
    }

    @Override
    public StoreFileChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        FileChannel channel = FileChannel.open(fileName, options);
        return getStoreFileChannel(channel);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        return toBufferedStream(fileName, this::getStoreFileChannel, append ? APPEND_OPTIONS : WRITE_OPTIONS);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        return new BufferedInputStream(openFileInputStream(fileName), (int) kibiBytes(8));
    }

    @Override
    public StoreFileChannel write(Path fileName) throws IOException {
        return open(fileName, WRITE_OPTIONS);
    }

    @Override
    public StoreFileChannel read(Path fileName) throws IOException {
        return open(fileName, READ_OPTIONS);
    }

    @Override
    public void mkdir(Path fileName) throws IOException {
        Files.createDirectories(fileName);
    }

    @Override
    public void mkdirs(Path file) throws IOException {
        if (Files.exists(file) && Files.isDirectory(file)) {
            return;
        }

        try {
            Files.createDirectories(file);
        } catch (IOException e) {
            throw new IOException(format(UNABLE_TO_CREATE_DIRECTORY_FORMAT, file), e);
        }
    }

    @Override
    public boolean fileExists(Path file) {
        return Files.exists(file);
    }

    @Override
    public long getFileSize(Path file) throws IOException {
        return Files.size(file);
    }

    @Override
    public long getBlockSize(Path file) throws IOException {
        return FileUtils.blockSize(file);
    }

    @Override
    public void deleteFile(Path fileName) throws IOException {
        FileUtils.deleteFile(fileName);
    }

    @Override
    public void deleteRecursively(Path directory) throws IOException {
        FileUtils.deleteDirectory(directory);
    }

    @Override
    public void deleteRecursively(Path directory, Predicate<Path> removeFilePredicate) throws IOException {
        FileUtils.deleteDirectory(directory, removeFilePredicate);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        Files.move(from, to, copyOptions);
    }

    @Override
    public Path[] listFiles(Path directory) throws IOException {
        try (Stream<Path> list = Files.list(directory)) {
            return list.toArray(Path[]::new);
        }
    }

    @Override
    public Path[] listFiles(Path directory, DirectoryStream.Filter<Path> filter) throws IOException {
        try (DirectoryStream<Path> paths = Files.newDirectoryStream(directory, filter)) {
            return StreamSupport.stream(paths.spliterator(), false).toArray(Path[]::new);
        }
    }

    @Override
    public boolean isDirectory(Path file) {
        return Files.isDirectory(file);
    }

    @Override
    public void moveToDirectory(Path file, Path toDirectory) throws IOException {
        FileUtils.moveFileToDirectory(file, toDirectory);
    }

    @Override
    public void copyToDirectory(Path file, Path toDirectory) throws IOException {
        FileUtils.copyFileToDirectory(file, toDirectory);
    }

    @Override
    public void copyFile(Path from, Path to) throws IOException {
        FileUtils.copyFile(from, to);
    }

    @Override
    public void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        FileUtils.copyFile(from, to, copyOptions);
    }

    @Override
    public void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException {
        FileUtils.copyDirectory(fromDirectory, toDirectory);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        FileUtils.truncateFile(path, size);
    }

    @Override
    public long lastModifiedTime(Path file) throws IOException {
        return Files.getLastModifiedTime(file).toMillis();
    }

    @Override
    public void deleteFileOrThrow(Path file) throws IOException {
        Files.delete(file);
    }

    @Override
    public int getFileDescriptor(StoreChannel channel) {
        return channel.getFileDescriptor();
    }

    @Override
    public boolean isPersistent() {
        return true;
    }

    @Override
    public Path createTempFile(String prefix, String suffix) throws IOException {
        return Files.createTempFile(prefix, suffix);
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        return Files.createTempFile(dir, prefix, suffix);
    }

    @Override
    public Path createTempDirectory(String prefix) throws IOException {
        return Files.createTempDirectory(prefix);
    }

    @Override
    public Path createTempDirectory(Path dir, String prefix) throws IOException {
        return Files.createTempDirectory(dir, prefix);
    }

    @Override
    public void close() {
        // nothing
    }

    protected StoreFileChannel getStoreFileChannel(FileChannel channel) {
        return new StoreFileChannel(channel);
    }

    private InputStream openFileInputStream(Path path) throws IOException {
        FileChannel channel = FileChannel.open(path, READ_OPTIONS);
        StoreFileChannel fileChannel = getStoreFileChannel(channel);
        fileChannel.tryMakeUninterruptible();
        return new NativeByteBufferInputStream(fileChannel);
    }

    @VisibleForTesting
    static class NativeByteBufferOutputStream extends OutputStream {

        private final StoreFileChannel fileChannel;
        private final ByteBuffer buffer;
        private final NativeScopedBuffer scopedBuffer;

        public NativeByteBufferOutputStream(StoreFileChannel fileChannel) {
            this.fileChannel = fileChannel;
            this.scopedBuffer =
                    new NativeScopedBuffer((int) kibiBytes(8), ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);
            this.buffer = scopedBuffer.getBuffer();
        }

        @Override
        public void write(int b) throws IOException {
            throw new UnsupportedOperationException("All stream operations should be buffer based.");
        }

        @Override
        public void write(byte[] b, int off, int len) throws IOException {
            int length;
            for (int offset = off; offset < off + len; offset += length) {
                length = Math.min(len - offset, buffer.capacity());
                buffer.clear();
                buffer.put(b, offset, length);
                buffer.flip();
                fileChannel.writeAll(buffer);
            }
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
            scopedBuffer.close();
            super.close();
        }
    }

    private static class NativeByteBufferInputStream extends InputStream {

        private final StoreFileChannel fileChannel;
        private final ByteBuffer buffer;
        private final NativeScopedBuffer scopedBuffer;

        public NativeByteBufferInputStream(StoreFileChannel fileChannel) {
            this.fileChannel = fileChannel;
            this.scopedBuffer =
                    new NativeScopedBuffer((int) kibiBytes(8), ByteOrder.LITTLE_ENDIAN, EmptyMemoryTracker.INSTANCE);
            this.buffer = scopedBuffer.getBuffer();
        }

        @Override
        public int read(byte[] b, int off, int len) throws IOException {
            buffer.clear();
            if (len < buffer.capacity()) {
                buffer.limit(len);
            }
            int readData = fileChannel.read(buffer);
            if (readData == -1) {
                return -1;
            }
            buffer.flip();
            buffer.get(b, off, readData);
            return readData;
        }

        @Override
        public int read() throws IOException {
            throw new UnsupportedOperationException("All stream operations should be buffer based.");
        }

        @Override
        public void close() throws IOException {
            fileChannel.close();
            scopedBuffer.close();
            super.close();
        }
    }
}
