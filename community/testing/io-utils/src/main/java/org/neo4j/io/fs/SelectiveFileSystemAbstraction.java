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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.util.Set;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.watcher.FileWatcher;

/**
 * Allows you to select different file system behaviour for one file and a different file system behaviour for
 * everyone else
 * e.g. Adversarial behaviour for the file under tests and normal behaviour for all other files.
 */
public class SelectiveFileSystemAbstraction implements FileSystemAbstraction {
    private final Path specialFile;
    private final FileSystemAbstraction specialFileSystem;
    private final FileSystemAbstraction defaultFileSystem;

    public SelectiveFileSystemAbstraction(
            Path specialFile, FileSystemAbstraction specialFileSystem, FileSystemAbstraction defaultFileSystem) {
        this.specialFile = specialFile;
        this.specialFileSystem = specialFileSystem;
        this.defaultFileSystem = defaultFileSystem;
    }

    @Override
    public FileWatcher fileWatcher() throws IOException {
        return new SelectiveFileWatcher(specialFile, defaultFileSystem.fileWatcher(), specialFileSystem.fileWatcher());
    }

    @Override
    public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        return chooseFileSystem(fileName).open(fileName, options);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        return chooseFileSystem(fileName).openAsOutputStream(fileName, append);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        return chooseFileSystem(fileName).openAsInputStream(fileName);
    }

    @Override
    public StoreChannel write(Path fileName) throws IOException {
        return chooseFileSystem(fileName).write(fileName);
    }

    @Override
    public StoreChannel read(Path fileName) throws IOException {
        return chooseFileSystem(fileName).read(fileName);
    }

    @Override
    public boolean fileExists(Path file) {
        return chooseFileSystem(file).fileExists(file);
    }

    @Override
    public void mkdir(Path fileName) throws IOException {
        chooseFileSystem(fileName).mkdir(fileName);
    }

    @Override
    public void mkdirs(Path fileName) throws IOException {
        chooseFileSystem(fileName).mkdirs(fileName);
    }

    @Override
    public long getFileSize(Path fileName) throws IOException {
        return chooseFileSystem(fileName).getFileSize(fileName);
    }

    @Override
    public long getBlockSize(Path file) throws IOException {
        return chooseFileSystem(file).getBlockSize(file);
    }

    @Override
    public void deleteFile(Path fileName) throws IOException {
        chooseFileSystem(fileName).deleteFile(fileName);
    }

    @Override
    public void deleteRecursively(Path directory) throws IOException {
        chooseFileSystem(directory).deleteRecursively(directory);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        chooseFileSystem(from).renameFile(from, to, copyOptions);
    }

    @Override
    public Path[] listFiles(Path directory) throws IOException {
        return chooseFileSystem(directory).listFiles(directory);
    }

    @Override
    public Path[] listFiles(Path directory, DirectoryStream.Filter<Path> filter) throws IOException {
        return chooseFileSystem(directory).listFiles(directory, filter);
    }

    @Override
    public boolean isDirectory(Path file) {
        return chooseFileSystem(file).isDirectory(file);
    }

    @Override
    public void moveToDirectory(Path file, Path toDirectory) throws IOException {
        chooseFileSystem(file).moveToDirectory(file, toDirectory);
    }

    @Override
    public void copyToDirectory(Path file, Path toDirectory) throws IOException {
        chooseFileSystem(file).copyToDirectory(file, toDirectory);
    }

    @Override
    public void copyFile(Path from, Path to) throws IOException {
        chooseFileSystem(from).copyFile(from, to);
    }

    @Override
    public void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        chooseFileSystem(from).copyFile(from, to, copyOptions);
    }

    @Override
    public void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException {
        chooseFileSystem(fromDirectory).copyRecursively(fromDirectory, toDirectory);
    }

    @Override
    public void truncate(Path path, long size) throws IOException {
        chooseFileSystem(path).truncate(path, size);
    }

    @Override
    public long lastModifiedTime(Path file) throws IOException {
        return chooseFileSystem(file).lastModifiedTime(file);
    }

    @Override
    public void deleteFileOrThrow(Path file) throws IOException {
        chooseFileSystem(file).deleteFileOrThrow(file);
    }

    @Override
    public int getFileDescriptor(StoreChannel channel) {
        return defaultFileSystem.getFileDescriptor(channel);
    }

    @Override
    public boolean isPersistent() {
        return defaultFileSystem.isPersistent();
    }

    @Override
    public Path createTempFile(String prefix, String suffix) throws IOException {
        return defaultFileSystem.createTempFile(prefix, suffix);
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        return defaultFileSystem.createTempFile(dir, prefix, suffix);
    }

    @Override
    public Path createTempDirectory(String prefix) throws IOException {
        return defaultFileSystem.createTempDirectory(prefix);
    }

    @Override
    public Path createTempDirectory(Path dir, String prefix) throws IOException {
        return defaultFileSystem.createTempDirectory(dir, prefix);
    }

    private FileSystemAbstraction chooseFileSystem(Path file) {
        return file.equals(specialFile) ? specialFileSystem : defaultFileSystem;
    }

    @Override
    public void close() throws IOException {
        IOUtils.closeAll(specialFileSystem, defaultFileSystem);
    }
}
