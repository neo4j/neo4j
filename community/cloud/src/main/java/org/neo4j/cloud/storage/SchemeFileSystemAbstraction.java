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
package org.neo4j.cloud.storage;

import static java.lang.ClassLoader.getSystemClassLoader;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cloud.storage.StorageUtils.APPEND_OPTIONS;
import static org.neo4j.cloud.storage.StorageUtils.READ_OPTIONS;
import static org.neo4j.cloud.storage.StorageUtils.WRITE_OPTIONS;
import static org.neo4j.service.Services.loadAll;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.file.CopyOption;
import java.nio.file.DirectoryStream.Filter;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.util.Collection;
import java.util.Locale;
import java.util.Set;
import java.util.function.Predicate;
import java.util.function.Supplier;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.factory.Sets;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannel;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.io.fs.watcher.FileWatcher;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.logging.NullLogProvider;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;

/**
 * A {@link FileSystemAbstraction} that can also resolve cloud storage {@link URI}s to the appropriate
 * {@link StoragePath} objects.
 */
public class SchemeFileSystemAbstraction implements FileSystemAbstraction, StorageSchemeResolver {

    private final MutableMap<String, StorageSystemProvider> schemesToProvider = Maps.mutable.empty();

    private final FileSystemAbstraction fs;

    private final Collection<StorageSystemProviderFactory> factories;

    private final Config config;

    private final InternalLogProvider logProvider;

    private final MemoryTracker memoryTracker;

    public SchemeFileSystemAbstraction(FileSystemAbstraction fs) {
        this(fs, Config.defaults());
    }

    public SchemeFileSystemAbstraction(FileSystemAbstraction fs, Config config) {
        this(fs, config, NullLogProvider.getInstance(), EmptyMemoryTracker.INSTANCE);
    }

    public SchemeFileSystemAbstraction(FileSystemAbstraction fs, Config config, InternalLogProvider logProvider) {
        this(fs, config, logProvider, EmptyMemoryTracker.INSTANCE);
    }

    public SchemeFileSystemAbstraction(
            FileSystemAbstraction fs, Config config, InternalLogProvider logProvider, MemoryTracker memoryTracker) {
        this(fs, loadAll(StorageSystemProviderFactory.class), config, logProvider, memoryTracker);
    }

    public SchemeFileSystemAbstraction(
            FileSystemAbstraction fs,
            Collection<StorageSystemProviderFactory> providerFactories,
            Config config,
            InternalLogProvider logProvider,
            MemoryTracker memoryTracker) {
        this.fs = requireNonNull(fs);
        this.factories = requireNonNull(providerFactories);
        this.config = requireNonNull(config);
        this.logProvider = requireNonNull(logProvider);
        this.memoryTracker = requireNonNull(memoryTracker);
    }

    @Override
    public Set<String> resolvableSchemes() {
        // will always support the file scheme
        final var schemes = Sets.mutable.of("file");
        // and whatever has been registered in the system
        for (var factory : factories) {
            schemes.add(factory.scheme());
        }
        return schemes.asUnmodifiable();
    }

    @Override
    public boolean canResolve(URI resource) {
        return internalCanResolve(resource.getScheme());
    }

    @Override
    public boolean canResolve(String resource) {
        final var matcher = SCHEME.matcher(resource);
        if (matcher.matches()) {
            return internalCanResolve(matcher.group(1));
        }
        // no scheme: it's a local file path that the fallback can handle
        return true;
    }

    @Override
    public Path resolve(URI resource) throws IOException {
        return internalResolve(resource.getScheme(), () -> resource);
    }

    @Override
    public Path resolve(String resource) throws IOException {
        final var matcher = SCHEME.matcher(resource);
        if (matcher.matches()) {
            return internalResolve(matcher.group(1), () -> URI.create(resource));
        }

        return Path.of(resource);
    }

    @Override
    public StoreChannel open(Path fileName, Set<OpenOption> options) throws IOException {
        if (fileName instanceof StoragePath path) {
            //noinspection
            return internalOpen(path, options);
        }

        return fs.open(fileName, options);
    }

    @Override
    public StoreChannel write(Path fileName) throws IOException {
        if (fileName instanceof StoragePath path) {
            // these are slightly different from fallback options as storage paths can't be both READ/WRITE
            return internalOpen(path, WRITE_OPTIONS);
        }
        return fs.write(fileName);
    }

    @Override
    public StoreChannel read(Path fileName) throws IOException {
        if (fileName instanceof StoragePath path) {
            return internalOpen(path, READ_OPTIONS);
        }
        return fs.read(fileName);
    }

    @Override
    public OutputStream openAsOutputStream(Path fileName, boolean append) throws IOException {
        if (fileName instanceof StoragePath path) {
            final var options = append ? APPEND_OPTIONS : WRITE_OPTIONS;
            //noinspection resource
            return provider(path).newOutputStream(fileName, options.toArray(OpenOption[]::new));
        }

        return fs.openAsOutputStream(fileName, append);
    }

    @Override
    public InputStream openAsInputStream(Path fileName) throws IOException {
        if (fileName instanceof StoragePath path) {
            //noinspection resource
            return provider(path).newInputStream(path);
        }

        return fs.openAsInputStream(fileName);
    }

    @Override
    public void truncate(Path file, long size) throws IOException {
        if (file instanceof StoragePath path) {
            //noinspection resource
            try (var channel = provider(path).newByteChannel(path, WRITE_OPTIONS)) {
                // must go this route as the default Files impl requires a FileChannel which we don't support
                channel.truncate(size);
            }
        } else {
            fs.truncate(file, size);
        }
    }

    @Override
    public boolean fileExists(Path file) {
        return fs.fileExists(file)
                // we pretend the file exists if it is a dir in cloud backends that don't support empty directories
                || StoragePath.isStorageDir(file);
    }

    @Override
    public void mkdir(Path fileName) throws IOException {
        fs.mkdir(fileName);
    }

    @Override
    public void mkdirs(Path fileName) throws IOException {
        fs.mkdirs(fileName);
    }

    @Override
    public long getFileSize(Path fileName) throws IOException {
        return fs.getFileSize(fileName);
    }

    @Override
    public long getBlockSize(Path file) throws IOException {
        if (file instanceof StoragePath path) {
            throw new IOException("StoragePaths do not have access to the remote system's block size: " + path);
        }

        return fs.getBlockSize(file);
    }

    @Override
    public void delete(Path path) throws IOException {
        fs.delete(path);
    }

    @Override
    public void deleteFile(Path fileName) throws IOException {
        fs.deleteFile(fileName);
    }

    @Override
    public void deleteRecursively(Path directory) throws IOException {
        fs.deleteRecursively(directory);
    }

    @Override
    public void deleteRecursively(Path directory, Predicate<Path> removeFilePredicate) throws IOException {
        fs.deleteRecursively(directory, removeFilePredicate);
    }

    @Override
    public void renameFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        fs.renameFile(from, to, copyOptions);
    }

    @Override
    public Path[] listFiles(Path directory) throws IOException {
        return fs.listFiles(directory);
    }

    @Override
    public Path[] listFiles(Path directory, Filter<Path> filter) throws IOException {
        return fs.listFiles(directory, filter);
    }

    @Override
    public boolean isDirectory(Path file) {
        return fs.isDirectory(file);
    }

    @Override
    public void moveToDirectory(Path file, Path toDirectory) throws IOException {
        fs.moveToDirectory(file, toDirectory);
    }

    @Override
    public void copyToDirectory(Path file, Path toDirectory) throws IOException {
        fs.copyToDirectory(file, toDirectory);
    }

    @Override
    public void copyFile(Path from, Path to, CopyOption... copyOptions) throws IOException {
        fs.copyFile(from, to, copyOptions);
    }

    @Override
    public void copyRecursively(Path fromDirectory, Path toDirectory) throws IOException {
        fs.copyRecursively(fromDirectory, toDirectory);
    }

    @Override
    public long lastModifiedTime(Path file) throws IOException {
        return fs.lastModifiedTime(file);
    }

    @Override
    public void deleteFileOrThrow(Path file) throws IOException {
        fs.deleteFileOrThrow(file);
    }

    @Override
    public int getFileDescriptor(StoreChannel channel) {
        return fs.getFileDescriptor(channel);
    }

    @Override
    public Path createTempFile(String prefix, String suffix) throws IOException {
        return fs.createTempFile(prefix, suffix);
    }

    @Override
    public Path createTempFile(Path dir, String prefix, String suffix) throws IOException {
        return fs.createTempFile(dir, prefix, suffix);
    }

    @Override
    public Path createTempDirectory(String prefix) throws IOException {
        return fs.createTempDirectory(prefix);
    }

    @Override
    public Path createTempDirectory(Path dir, String prefix) throws IOException {
        return fs.createTempDirectory(dir, prefix);
    }

    @Override
    public boolean isPersistent() {
        return !factories.isEmpty() || fs.isPersistent();
    }

    @Override
    public FileWatcher fileWatcher() {
        throw new UnsupportedOperationException("fileWatcher not implemented");
    }

    @Override
    public void close() throws IOException {
        try {
            IOUtils.closeAll(schemesToProvider.values());
        } finally {
            schemesToProvider.clear();
        }
    }

    private boolean internalCanResolve(String scheme) {
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            return true;
        }

        for (var factory : factories) {
            if (factory.matches(scheme)) {
                return true;
            }
        }
        return false;
    }

    private Path internalResolve(String scheme, Supplier<URI> resource) throws IOException {
        if (scheme == null || "file".equalsIgnoreCase(scheme)) {
            return Path.of(resource.get());
        }

        final var schemeToResolve = scheme.toLowerCase(Locale.ROOT);
        for (var factory : factories) {
            if (factory.matches(schemeToResolve)) {
                try {
                    final var provider = schemesToProvider.getIfAbsentPut(
                            schemeToResolve,
                            () -> factory.createStorageSystemProvider(
                                    (prefix) -> tempChannel(prefix, schemeToResolve),
                                    config,
                                    logProvider,
                                    memoryTracker,
                                    getSystemClassLoader()));
                    final var uri = resource.get();
                    provider.getStorageSystem(uri);
                    return provider.getPath(uri);
                } catch (UncheckedIOException ex) {
                    throw ex.getCause();
                }
            }
        }

        throw new ProviderMismatchException("No storage system found for scheme: " + scheme);
    }

    private ChunkChannel tempChannel(String prefix, String scheme) throws IOException {
        final var path = fs.createTempFile(prefix, scheme);
        final var channel = fs.write(path);
        return new ChunkChannel() {
            @Override
            public Path path() {
                return path;
            }

            @Override
            public void write(ByteBuffer buffer) throws IOException {
                channel.writeAll(buffer);
            }

            @Override
            public void close() throws IOException {
                try {
                    channel.close();
                } finally {
                    fs.delete(path);
                }
            }
        };
    }

    private StoreChannel internalOpen(StoragePath path, Set<OpenOption> options) throws IOException {
        //noinspection resource
        return new StorageChannel(provider(path).newByteChannel(path, options));
    }

    private static StorageSystemProvider provider(StoragePath path) {
        return path.getFileSystem().provider();
    }
}
