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

import static java.lang.Math.min;
import static java.lang.Math.toIntExact;
import static java.lang.String.format;
import static java.lang.System.getProperty;
import static java.nio.file.StandardCopyOption.REPLACE_EXISTING;
import static java.util.Arrays.asList;
import static org.neo4j.io.fs.DefaultFileSystemAbstraction.UNABLE_TO_CREATE_DIRECTORY_FORMAT;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.time.Clock;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.commons.lang3.ArrayUtils;
import org.neo4j.graphdb.Resource;
import org.neo4j.internal.helpers.collection.CombiningIterator;
import org.neo4j.io.ByteUnit;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.io.memory.ByteBuffers;
import org.neo4j.test.impl.ChannelInputStream;
import org.neo4j.test.impl.ChannelOutputStream;

public class EphemeralFileSystemAbstraction implements FileSystemAbstraction {
    private static final AtomicLong UNIQUE_TEMP_FILE = new AtomicLong();

    private final Clock clock;
    private final AtomicInteger keepFiles = new AtomicInteger();
    private final Set<Path> directories = ConcurrentHashMap.newKeySet();
    private final Map<Path, EphemeralFileData> files;
    private final String tempDirectory = getProperty("java.io.tmpdir");
    private volatile boolean closed;

    public EphemeralFileSystemAbstraction() {
        this(Clock.systemUTC());
    }

    public EphemeralFileSystemAbstraction(Clock clock) {
        this.clock = clock;
        this.files = new ConcurrentHashMap<>();
        initCurrentWorkingDirectory();
    }

    private void initCurrentWorkingDirectory() {
        try {
            mkdirs(Path.of(".").toAbsolutePath().normalize());
        } catch (IOException e) {
            throw new UncheckedIOException(
                    "EphemeralFileSystemAbstraction could not initialise current working directory", e);
        }
    }

    private EphemeralFileSystemAbstraction(Set<Path> directories, Map<Path, EphemeralFileData> files, Clock clock) {
        this.clock = clock;
        this.files = new ConcurrentHashMap<>(files);
        this.directories.addAll(directories);
        initCurrentWorkingDirectory();
    }

    public void clear() {
        closeFiles();
    }

    /**
     * Simulate a filesystem crash, in which any changes that have not been {@link StoreChannel#force}d
     * will be lost. Practically, all files revert to the state when they are last {@link StoreChannel#force}d.
     */
    public void crash() {
        files.values().forEach(EphemeralFileData::crash);
    }

    public Resource keepFiles() {
        keepFiles.getAndIncrement();
        return keepFiles::decrementAndGet;
    }

    @Override
    public synchronized void close() throws IOException {
        if (keepFiles.get() > 0) {
            return;
        }
        closeFiles();
        closed = true;
    }

    public boolean isClosed() {
        return closed;
    }

    private void closeFiles() {
        for (EphemeralFileData file : files.values()) {
            file.free();
        }
        files.clear();
    }

    public void assertNoOpenFiles() throws Exception {
        Exception exception = null;
        for (EphemeralFileData file : files.values()) {
            Iterator<EphemeralFileChannel> channels = file.getOpenChannels();
            while (channels.hasNext()) {
                EphemeralFileChannel channel = channels.next();
                if (exception == null) {
                    exception = new IOException("Expected no open files. "
                            + "The stack traces of the currently open files are attached as suppressed exceptions.");
                }
                exception.addSuppressed(channel.openedAt);
            }
        }
        if (exception != null) {
            throw exception;
        }
    }

    @Override
    public FileWatcher fileWatcher() {
        return FileWatcher.SILENT_WATCHER;
    }

    @Override
    public synchronized StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        return getStoreChannel(fileName);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        return new ChannelOutputStream(write(fileName), append, INSTANCE);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        return new ChannelInputStream(read(fileName), INSTANCE);
    }

    @Override
    public synchronized StoreChannel write(Path fileName) throws IOException {
        Path parentFile = fileName.getParent();
        if (parentFile != null /*means that this is the 'default location'*/ && !fileExists(parentFile)) {
            throw new NoSuchFileException("'" + fileName + "' (The system cannot find the path specified)");
        }

        EphemeralFileData data =
                files.computeIfAbsent(canonicalFile(fileName), key -> new EphemeralFileData(key, clock));
        return new StoreFileChannel(
                new EphemeralFileChannel(data, () -> new EphemeralFileStillOpenException(fileName.toString())));
    }

    @Override
    public synchronized StoreChannel read(Path fileName) throws IOException {
        return getStoreChannel(fileName);
    }

    @Override
    public long getFileSize(Path fileName) {
        EphemeralFileData file = files.get(canonicalFile(fileName));
        return file == null ? 0 : file.size();
    }

    @Override
    public long getBlockSize(Path file) {
        return 512;
    }

    @Override
    public boolean fileExists(Path file) {
        file = canonicalFile(file);
        return directories.contains(file) || files.containsKey(file);
    }

    private static Path canonicalFile(Path path) {
        return path.toAbsolutePath().normalize();
    }

    @Override
    public boolean isDirectory(Path file) {
        return directories.contains(canonicalFile(file));
    }

    @Override
    public void mkdir(Path directory) {
        directories.add(canonicalFile(directory));
    }

    @Override
    public void mkdirs(Path directory) throws IOException {
        Path currentDirectory = canonicalFile(directory);

        while (currentDirectory != null) {
            if (files.containsKey(currentDirectory)) {
                throw new IOException(format(UNABLE_TO_CREATE_DIRECTORY_FORMAT, currentDirectory));
            } else {
                mkdir(currentDirectory);
            }
            currentDirectory = currentDirectory.getParent();
        }
    }

    @Override
    public void deleteFile(Path fileName) throws IOException {
        fileName = canonicalFile(fileName);
        EphemeralFileData removed = files.remove(fileName);
        if (removed != null) {
            removed.free();
        } else {
            if (!fileExists(fileName)) {
                return;
            }
            Path[] fileList = listFiles(fileName);
            if (fileList.length > 0) {
                throw new DirectoryNotEmptyException(fileName.toString());
            }
            if (!directories.remove(fileName)) {
                throw new NoSuchFileException(fileName.toString());
            }
        }
    }

    @Override
    public void deleteRecursively(Path directory) throws IOException {
        if (isDirectory(directory)) {
            // Delete all files in directory and sub-directory
            directory = canonicalFile(directory);
            for (Map.Entry<Path, EphemeralFileData> file : files.entrySet()) {
                Path fileName = file.getKey();
                if (fileName.startsWith(directory) && !fileName.equals(directory)) {
                    deleteFile(fileName);
                }
            }

            // Delete all sub-directories
            Path finalDirectory = directory;
            List<Path> subDirectories = directories.stream()
                    .filter(p -> p.startsWith(finalDirectory) && !p.equals(finalDirectory))
                    .sorted(Comparator.reverseOrder())
                    .toList();
            for (Path subDirectory : subDirectories) {
                deleteFile(subDirectory);
            }
        }
        deleteFile(directory);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        from = canonicalFile(from);
        to = canonicalFile(to);

        if (directories.contains(from)) {
            if (!isDirectory(to.getParent())) {
                throw new NoSuchFileException("Target directory[" + to.getParent() + "] does not exists");
            }
            directories.add(to);
            // Rename the directory, meaning all its files instead
            for (var child : listFiles(from)) {
                if (isDirectory(child)) {
                    internalRenameDirectory(to, child);
                } else {
                    internalRenameFile(child, to.resolve(child.getFileName().toString()), copyOptions);
                }
            }
        } else {
            internalRenameFile(from, to, copyOptions);
        }
    }

    private void internalRenameDirectory(Path to, Path child) {
        directories.remove(child);
        directories.add(to.resolve(child.getFileName().toString()));
    }

    private void internalRenameFile(Path from, Path to, CopyOption[] copyOptions)
            throws NoSuchFileException, FileAlreadyExistsException {
        if (!files.containsKey(from)) {
            throw new NoSuchFileException("'" + from + "' doesn't exist");
        }

        boolean replaceExisting = false;
        for (CopyOption copyOption : copyOptions) {
            replaceExisting |= copyOption == REPLACE_EXISTING;
        }
        if (files.containsKey(to) && !replaceExisting) {
            throw new FileAlreadyExistsException("'" + to + "' already exists");
        }
        if (!isDirectory(to.getParent())) {
            throw new NoSuchFileException("Target directory[" + to.getParent() + "] does not exists");
        }
        files.put(to, files.remove(from));
    }

    @Override
    public Path[] listFiles(Path directory) throws IOException {
        directory = canonicalFile(directory);
        if (files.containsKey(directory)) {
            throw new NotDirectoryException(directory.toString());
        }
        if (!directories.contains(directory)) {
            throw new NoSuchFileException(directory.toString());
        }

        Set<Path> found = new HashSet<>();
        Iterator<Path> filesAndFolders =
                new CombiningIterator<>(asList(this.files.keySet().iterator(), directories.iterator()));
        while (filesAndFolders.hasNext()) {
            Path file = filesAndFolders.next();
            if (directory.equals(file.getParent())) {
                found.add(file);
            }
        }

        return found.toArray(new Path[0]);
    }

    @Override
    public Path[] listFiles(Path directory, DirectoryStream.Filter<Path> filter) {
        directory = canonicalFile(directory);
        if (files.containsKey(directory))
        // This means that you're trying to list files on a file, not a directory.
        {
            return null;
        }

        Set<Path> found = new HashSet<>();
        Iterator<Path> files =
                new CombiningIterator<>(asList(this.files.keySet().iterator(), directories.iterator()));
        while (files.hasNext()) {
            Path path = files.next();
            if (directory.equals(path.getParent())) {
                try {
                    if (filter.accept(path)) {
                        found.add(path);
                    }
                } catch (IOException e) {
                    throw new UncheckedIOException(e);
                }
            }
        }
        return found.toArray(new Path[0]);
    }

    private StoreChannel getStoreChannel(Path fileName) throws IOException {
        EphemeralFileData data = files.get(canonicalFile(fileName));
        if (data != null) {
            return new StoreFileChannel(new EphemeralFileChannel(
                    data,
                    () -> new EphemeralFileStillOpenException(
                            fileName.toAbsolutePath().toString())));
        }
        return write(fileName);
    }

    @Override
    public void moveToDirectory(Path file, Path toDirectory) throws IOException {
        Path destinationFile = toDirectory.resolve(file.getFileName());
        if (isDirectory(file)) {
            mkdir(destinationFile);
            for (Path f : listFiles(file)) {
                moveToDirectory(f, destinationFile);
            }
            deleteFile(file);
        } else {
            EphemeralFileData fileToMove = files.remove(canonicalFile(file));
            if (fileToMove == null) {
                throw new NoSuchFileException(file.toAbsolutePath().toString());
            }
            files.put(canonicalFile(destinationFile), fileToMove);
        }
    }

    @Override
    public void copyToDirectory(Path file, Path toDirectory) throws IOException {
        Path targetFile = toDirectory.resolve(file.getFileName());
        copyFile(file, targetFile);
    }

    @Override
    public void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        EphemeralFileData data = files.get(canonicalFile(from));
        if (data == null) {
            throw new NoSuchFileException("File " + from + " not found");
        }
        if (!ArrayUtils.contains(copyOptions, REPLACE_EXISTING) && files.get(canonicalFile(from)) != null) {
            throw new FileAlreadyExistsException(to.toAbsolutePath().toString());
        }
        copyFile(from, this, to, newCopyBuffer());
    }

    @Override
    public void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException {
        copyRecursivelyFromOtherFs(fromDirectory, this, toDirectory, newCopyBuffer());
    }

    public synchronized EphemeralFileSystemAbstraction snapshot() {
        Map<Path, EphemeralFileData> copiedFiles = new HashMap<>();
        for (Map.Entry<Path, EphemeralFileData> file : files.entrySet()) {
            copiedFiles.put(file.getKey(), file.getValue().copy());
        }
        return new EphemeralFileSystemAbstraction(directories, copiedFiles, clock);
    }

    private void copyRecursivelyFromOtherFs(Path from, FileSystemAbstraction fromFs, Path to) throws IOException {
        copyRecursivelyFromOtherFs(from, fromFs, to, newCopyBuffer());
    }

    private static ByteBuffer newCopyBuffer() {
        return ByteBuffers.allocate(toIntExact(ByteUnit.MebiByte.toBytes(1)), ByteOrder.LITTLE_ENDIAN, INSTANCE);
    }

    private void copyRecursivelyFromOtherFs(Path from, FileSystemAbstraction fromFs, Path to, ByteBuffer buffer)
            throws IOException {
        this.mkdirs(to);
        for (Path fromFile : fromFs.listFiles(from)) {
            Path toFile = to.resolve(fromFile.getFileName());
            if (fromFs.isDirectory(fromFile)) {
                copyRecursivelyFromOtherFs(fromFile, fromFs, toFile);
            } else {
                copyFile(fromFile, fromFs, toFile, buffer);
            }
        }
    }

    private void copyFile(Path from, FileSystemAbstraction fromFs, Path to, ByteBuffer buffer) throws IOException {
        try (StoreChannel source = fromFs.read(from);
                StoreChannel sink = this.write(to)) {
            sink.truncate(0);
            long sourceSize = source.size();
            for (int available; (available = (int) (sourceSize - source.position())) > 0; ) {
                buffer.clear();
                buffer.limit(min(available, buffer.capacity()));
                source.read(buffer);
                buffer.flip();
                sink.write(buffer);
            }
        }
    }

    @Override
    public void truncate(Path file, long size) throws IOException {
        EphemeralFileData data = files.get(canonicalFile(file));
        if (data == null) {
            throw new NoSuchFileException("File " + file + " not found");
        }
        data.truncate(size);
    }

    @Override
    public long lastModifiedTime(Path file) {
        EphemeralFileData data = files.get(canonicalFile(file));
        if (data == null) {
            return 0;
        }
        return data.getLastModified();
    }

    @Override
    public void deleteFileOrThrow(Path file) throws IOException {
        file = canonicalFile(file);
        if (!fileExists(file)) {
            throw new NoSuchFileException(file.toAbsolutePath().toString());
        }
        deleteFile(file);
    }

    @Override
    public int getFileDescriptor(StoreChannel channel) {
        return INVALID_FILE_DESCRIPTOR;
    }

    @Override
    public boolean isPersistent() {
        return false;
    }

    @Override
    public Path createTempFile(String prefix, String suffix) throws IOException {
        return createTempFile(Path.of(tempDirectory), prefix, suffix);
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        Path parent = canonicalFile(dir);
        mkdirs(parent);
        while (true) {
            Path tmp = parent.resolve(prefix + Long.toUnsignedString(UNIQUE_TEMP_FILE.getAndIncrement()) + suffix);
            var prev = files.putIfAbsent(tmp, new EphemeralFileData(tmp, clock));
            if (prev == null) {
                return tmp;
            }
        }
    }

    @Override
    public Path createTempDirectory(String prefix) throws IOException {
        return createTempDirectory(Path.of(tempDirectory), prefix);
    }

    @Override
    public Path createTempDirectory(Path dir, String prefix) throws IOException {
        Path parent = canonicalFile(dir);
        mkdirs(parent);
        Path tmp;
        do {
            tmp = parent.resolve(prefix + Long.toUnsignedString(UNIQUE_TEMP_FILE.getAndIncrement()));
        } while (!directories.add(tmp));
        return tmp;
    }
}
