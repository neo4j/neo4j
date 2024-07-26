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

import static java.nio.file.LinkOption.NOFOLLOW_LINKS;
import static java.nio.file.StandardOpenOption.CREATE;
import static java.nio.file.StandardOpenOption.TRUNCATE_EXISTING;
import static java.nio.file.StandardOpenOption.WRITE;
import static java.util.Objects.requireNonNull;
import static org.neo4j.cloud.storage.StorageUtils.normalizeForRead;
import static org.neo4j.cloud.storage.StorageUtils.normalizeForWrite;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URI;
import java.nio.channels.SeekableByteChannel;
import java.nio.file.CopyOption;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystemAlreadyExistsException;
import java.nio.file.FileSystemNotFoundException;
import java.nio.file.LinkOption;
import java.nio.file.OpenOption;
import java.nio.file.Path;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.spi.FileSystemProvider;
import java.util.EnumSet;
import java.util.Map;
import java.util.Set;
import org.eclipse.collections.api.factory.Maps;
import org.eclipse.collections.api.map.MutableMap;
import org.neo4j.cloud.storage.StorageSystemProviderFactory.ChunkChannelSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.io.IOUtils;
import org.neo4j.logging.InternalLogProvider;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.Preconditions;

/**
 * The base class for providing access to a cloud storage system as a local {@link FileSystemProvider}
 */
public abstract class StorageSystemProvider extends FileSystemProvider implements AutoCloseable {

    private final MutableMap<URI, StorageSystem> systems = Maps.mutable.empty();

    protected final String scheme;

    protected final ChunkChannelSupplier tempSupplier;

    protected final Config config;

    protected final InternalLogProvider logProvider;

    protected final MemoryTracker memoryTracker;

    protected StorageSystemProvider(
            String scheme,
            ChunkChannelSupplier tempSupplier,
            Config config,
            InternalLogProvider logProvider,
            MemoryTracker memoryTracker) {
        this.scheme = requireNonNull(scheme);
        this.tempSupplier = requireNonNull(tempSupplier);
        this.config = requireNonNull(config);
        this.logProvider = requireNonNull(logProvider);
        this.memoryTracker = requireNonNull(memoryTracker);
    }

    /**
     * @param path the storage path to access
     * @param options the options to use when opening the channel for read/write operations
     * @return the channel for performing read/write operations
     * @throws IOException if unable to create the channel
     */
    protected abstract SeekableByteChannel openAsByteChannel(StoragePath path, Set<? extends OpenOption> options)
            throws IOException;

    /**
     * @param fileName the storage path to access
     * @param options the options to use when opening the channel for write operations
     * @return the stream for performing write operations
     * @throws IOException if unable to create the channel
     */
    protected abstract OutputStream openAsOutputStream(StoragePath fileName, Set<? extends OpenOption> options)
            throws IOException;

    /**
     * @param fileName the storage path to access
     * @return the stream for performing read operations
     * @throws IOException if unable to create the channel
     */
    protected abstract InputStream openAsInputStream(StoragePath fileName) throws IOException;

    /**
     * Creates a storage system for the remote resource represented by a {@link URI}
     * @param storageUri the resources {@link URI}
     * @return the storage system that can handle access to the provided resource
     */
    protected abstract StorageSystem create(URI storageUri);

    /**
     * Resolve a resource relative to a storage system's location
     * @param uri the resource's full {@link URI}
     * @return the resource's storage location
     */
    protected abstract StorageLocation resolve(URI uri);

    /**
     * Get or create a storage system
     * @param uri the resource's full {@link URI}
     * @return the resource's storage system
     */
    public StorageSystem getStorageSystem(URI uri) {
        return internalCreateFileSystem(resolve(checkScheme(uri)).baseURI);
    }

    /**
     * @param path the storage path to access
     * @param options the options to use when opening the channel for read/write operations
     * @return the channel for performing read/write operations
     * @throws IOException if unable to create the channel
     */
    public SeekableByteChannel newByteChannel(StoragePath path, Set<? extends OpenOption> options) throws IOException {
        final var normalized = options.contains(WRITE) ? normalizeForWrite(options) : normalizeForRead(options);
        return openAsByteChannel(ensureNotDirectory(path), normalized);
    }

    @Override
    public String getScheme() {
        return scheme;
    }

    @Override
    public FileSystem newFileSystem(URI uri, Map<String, ?> env) {
        final var systemUri = resolve(checkScheme(uri)).baseURI;
        if (systems.containsKey(systemUri)) {
            throw new FileSystemAlreadyExistsException(
                    "Storage system already exists for the system URI : " + systemUri);
        }

        return internalCreateFileSystem(systemUri);
    }

    @Override
    public StorageSystem getFileSystem(URI uri) {
        return internalGetFileSystem(resolve(checkScheme(uri)).baseURI);
    }

    @Override
    public OutputStream newOutputStream(Path path, OpenOption... options) throws IOException {
        var normalizedOptions = options.length != 0
                ? normalizeForWrite(options)
                : normalizeForWrite(EnumSet.of(CREATE, TRUNCATE_EXISTING, WRITE));
        return openAsOutputStream(ensureNotDirectory(path), normalizedOptions);
    }

    @Override
    public Path getPath(URI uri) {
        final var resolved = resolve(checkScheme(uri));
        //noinspection resource
        return internalGetFileSystem(resolved.baseURI).getPath(resolved.resourcePath);
    }

    @Override
    public void move(Path source, Path target, CopyOption... options) throws IOException {
        // remote storage is unlikely to have atomic move so copy/delete instead
        copy(source, target, options);
        delete(source);
    }

    @Override
    public boolean isSameFile(Path path, Path path2) throws IOException {
        return path.toRealPath(NOFOLLOW_LINKS).equals(path2.toRealPath(NOFOLLOW_LINKS));
    }

    @Override
    public boolean isHidden(Path path) {
        return false;
    }

    @Override
    public FileStore getFileStore(Path path) {
        // remote storage doesn't have partitions/volumes so nothing to return
        return null;
    }

    @Override
    public void setAttribute(Path path, String attribute, Object value, LinkOption... options) {
        throw new UnsupportedOperationException("setAttribute");
    }

    @Override
    public SeekableByteChannel newByteChannel(Path path, Set<? extends OpenOption> options, FileAttribute<?>... attrs)
            throws IOException {
        final var normalized = options.contains(WRITE) ? normalizeForWrite(options) : normalizeForRead(options);
        return newByteChannel(ensureNotDirectory(path), normalized);
    }

    @Override
    public InputStream newInputStream(Path path, OpenOption... options) throws IOException {
        Preconditions.checkArgument(!normalizeForRead(options).contains(WRITE), "Opening for WRITE is not allowed");
        return openAsInputStream(ensureNotDirectory(path));
    }

    @Override
    public void close() {
        try {
            IOUtils.closeAllUnchecked(systems.values());
        } finally {
            systems.clear();
        }
    }

    protected StoragePath ensureCorrectPath(Path path) {
        Preconditions.checkArgument(path instanceof StoragePath, "Path provided must be a storage path");
        return ensureCorrectPath((StoragePath) path);
    }

    protected StoragePath ensureCorrectPath(StoragePath storagePath) {
        requireNonNull(storagePath, "Path must not be null");
        Preconditions.checkArgument(
                storagePath.scheme().equals(getScheme()),
                "Path provided must have the correct scheme for this storage system");
        return storagePath;
    }

    protected StoragePath ensureNotDirectory(Path path) throws IOException {
        return ensureNotDirectory(ensureCorrectPath(path));
    }

    protected StoragePath ensureNotDirectory(StoragePath path) throws IOException {
        final var storagePath = ensureCorrectPath(path);
        if (storagePath.isDirectory()) {
            throw new IOException("Path provided is a directory but must be a regular file: " + storagePath);
        }
        return storagePath;
    }

    private URI checkScheme(URI uri) {
        final var uriScheme = uri.getScheme();
        Preconditions.checkArgument(
                scheme.equals(uriScheme),
                "Invalid scheme provided: expected '%s' but found '%s'".formatted(scheme, uriScheme));
        return uri;
    }

    private StorageSystem internalGetFileSystem(URI systemUri) {
        final var system = systems.get(systemUri);
        if (system == null) {
            throw new FileSystemNotFoundException("Unable to find the storage system for the system URI: " + systemUri);
        }
        return system;
    }

    private StorageSystem internalCreateFileSystem(URI systemUri) {
        return systems.getIfAbsentPut(systemUri, () -> create(systemUri));
    }

    /**
     * Represents a handle to a remote resource described by its {@link URI} and the relative resource path on that
     * system
     * @param baseURI the storage system's {@link URI}
     * @param resourcePath the relative resource path on its storage system
     */
    public record StorageLocation(URI baseURI, String resourcePath) {}
}
