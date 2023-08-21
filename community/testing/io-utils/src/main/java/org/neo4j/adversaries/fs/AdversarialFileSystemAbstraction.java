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
package org.neo4j.adversaries.fs;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryNotEmptyException;
import java.nio.file.DirectoryStream;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.NoSuchFileException;
import java.nio.file.NotDirectoryException;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import org.neo4j.adversaries.Adversary;
import org.neo4j.adversaries.watcher.AdversarialFileWatcher;
import org.neo4j.io.fs.DefaultFileSystemAbstraction;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.StoreFileChannel;
import org.neo4j.io.fs.watcher.FileWatcher;

/**
 * Used by the robustness suite to check for partial failures.
 */
@SuppressWarnings("unchecked")
public class AdversarialFileSystemAbstraction implements FileSystemAbstraction {
    private final FileSystemAbstraction delegate;
    private final Adversary adversary;

    public AdversarialFileSystemAbstraction(Adversary adversary) {
        this(adversary, new DefaultFileSystemAbstraction());
    }

    public AdversarialFileSystemAbstraction(Adversary adversary, FileSystemAbstraction delegate) {
        this.adversary = adversary;
        this.delegate = delegate;
    }

    @Override
    public FileWatcher fileWatcher() throws IOException {
        adversary.injectFailure(UnsupportedOperationException.class, IOException.class);
        return new AdversarialFileWatcher(delegate.fileWatcher(), adversary);
    }

    @Override
    public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        return AdversarialFileChannel.wrap((StoreFileChannel) delegate.open(fileName, options), adversary);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, SecurityException.class);
        delegate.renameFile(from, to, copyOptions);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, SecurityException.class);
        return new AdversarialOutputStream(delegate.openAsOutputStream(fileName, append), adversary);
    }

    @Override
    public StoreChannel write(Path fileName) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        return AdversarialFileChannel.wrap((StoreFileChannel) delegate.write(fileName), adversary);
    }

    @Override
    public StoreChannel read(Path fileName) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        return AdversarialFileChannel.wrap((StoreFileChannel) delegate.read(fileName), adversary);
    }

    @Override
    public void mkdir(Path fileName) throws IOException {
        adversary.injectFailure(FileAlreadyExistsException.class, SecurityException.class);
        delegate.mkdir(fileName);
    }

    @Override
    public Path[] listFiles(Path directory) throws IOException {
        adversary.injectFailure(NotDirectoryException.class, SecurityException.class);
        return delegate.listFiles(directory);
    }

    @Override
    public Path[] listFiles(Path directory, DirectoryStream.Filter<Path> filter) throws IOException {
        adversary.injectFailure(NotDirectoryException.class, SecurityException.class);
        return delegate.listFiles(directory, filter);
    }

    @Override
    public long getFileSize(Path fileName) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.getFileSize(fileName);
    }

    @Override
    public long getBlockSize(Path file) throws IOException {
        adversary.injectFailure(SecurityException.class);
        return delegate.getBlockSize(file);
    }

    @Override
    public void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        adversary.injectFailure(SecurityException.class, NoSuchFileException.class, IOException.class);
        delegate.copyFile(from, to, copyOptions);
    }

    @Override
    public void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException {
        adversary.injectFailure(SecurityException.class, IOException.class, NullPointerException.class);
        delegate.copyRecursively(fromDirectory, toDirectory);
    }

    @Override
    public void deleteFile(Path fileName) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, DirectoryNotEmptyException.class, SecurityException.class);
        delegate.deleteFile(fileName);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, SecurityException.class);
        return new AdversarialInputStream(delegate.openAsInputStream(fileName), adversary);
    }

    @Override
    public void moveToDirectory(Path file, Path toDirectory) throws IOException {
        adversary.injectFailure(
                SecurityException.class,
                IllegalArgumentException.class,
                NoSuchFileException.class,
                NullPointerException.class,
                IOException.class);
        delegate.moveToDirectory(file, toDirectory);
    }

    @Override
    public void copyToDirectory(Path file, Path toDirectory) throws IOException {
        adversary.injectFailure(
                SecurityException.class,
                IllegalArgumentException.class,
                NoSuchFileException.class,
                NullPointerException.class,
                IOException.class);
        delegate.copyToDirectory(file, toDirectory);
    }

    @Override
    public boolean isDirectory(Path file) {
        adversary.injectFailure(SecurityException.class);
        return delegate.isDirectory(file);
    }

    @Override
    public boolean fileExists(Path file) {
        adversary.injectFailure(SecurityException.class);
        return delegate.fileExists(file);
    }

    @Override
    public void mkdirs(Path fileName) throws IOException {
        adversary.injectFailure(SecurityException.class, IOException.class);
        delegate.mkdirs(fileName);
    }

    @Override
    public void deleteRecursively(Path directory) throws IOException {
        adversary.injectFailure(SecurityException.class, NullPointerException.class, IOException.class);
        delegate.deleteRecursively(directory);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        adversary.injectFailure(
                NoSuchFileException.class,
                IOException.class,
                IllegalArgumentException.class,
                SecurityException.class,
                NullPointerException.class);
        delegate.truncate(path, size);
    }

    @Override
    public long lastModifiedTime(Path file) throws IOException {
        adversary.injectFailure(SecurityException.class, NullPointerException.class, IOException.class);
        return delegate.lastModifiedTime(file);
    }

    @Override
    public void deleteFileOrThrow(Path file) throws IOException {
        adversary.injectFailure(NoSuchFileException.class, IOException.class, SecurityException.class);
        delegate.deleteFileOrThrow(file);
    }

    @Override
    public int getFileDescriptor(StoreChannel channel) {
        return delegate.getFileDescriptor(channel);
    }

    @Override
    public boolean isPersistent() {
        return delegate.isPersistent();
    }

    @Override
    public Path createTempFile(String prefix, String suffix) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.createTempFile(prefix, suffix);
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.createTempFile(dir, prefix, suffix);
    }

    @Override
    public Path createTempDirectory(String prefix) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.createTempDirectory(prefix);
    }

    @Override
    public Path createTempDirectory(Path dir, String prefix) throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        return delegate.createTempDirectory(dir, prefix);
    }

    @Override
    public void close() throws IOException {
        adversary.injectFailure(IOException.class, SecurityException.class);
        delegate.close();
    }
}
