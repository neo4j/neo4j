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

import static org.neo4j.cloud.storage.PathRepresentation.SEPARATOR;

import java.io.IOException;
import java.nio.file.FileStore;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Path;
import java.nio.file.PathMatcher;
import java.nio.file.WatchService;
import java.nio.file.attribute.UserPrincipalLookupService;
import java.util.Collections;
import java.util.List;
import java.util.Objects;
import java.util.Set;

/**
 * The base class for representing a cloud storage system as a local {@link java.nio.file.FileSystem}
 */
public abstract class StorageSystem extends FileSystem {

    public static final String BASIC_FILE_ATTRIBUTE_VIEW = "basic";

    private final StorageSystemProvider provider;

    private boolean open = true;

    protected StorageSystem(StorageSystemProvider provider) {
        this.provider = Objects.requireNonNull(provider);
    }

    /**
     * @return the {@link java.net.URI} prefix specific for this storage system. For some systems this could be as
     * simple as the scheme-specific part, ex. {@code cloud://}. In others, it might include further information, ex.
     * {@code cloud://some-container}
     */
    protected abstract String uriPrefix();

    protected abstract void internalClose() throws IOException;

    /**
     * @return does the underlying file system support creating empty directories
     */
    protected abstract boolean supportsEmptyDirs();

    /**
     *
     * @param path the path to check
     * @return <code>true</code> if this system can handle access operations for the provided path
     */
    public abstract boolean canResolve(StoragePath path);

    /**
     * @return the specific scheme for the {@link java.net.URI}s that this system can represent
     */
    public String scheme() {
        return provider.getScheme();
    }

    @Override
    public StorageSystemProvider provider() {
        return provider;
    }

    @Override
    public Path getPath(String first, String... more) {
        return new StoragePath(this, PathRepresentation.of(first, more));
    }

    @Override
    public boolean isOpen() {
        return open;
    }

    @Override
    public boolean isReadOnly() {
        return false;
    }

    @Override
    public String getSeparator() {
        return SEPARATOR;
    }

    @Override
    public Iterable<Path> getRootDirectories() {
        return List.of(getPath(SEPARATOR));
    }

    @Override
    public Iterable<FileStore> getFileStores() {
        return Collections.emptyList();
    }

    @Override
    public Set<String> supportedFileAttributeViews() {
        return Set.of(BASIC_FILE_ATTRIBUTE_VIEW);
    }

    @Override
    public PathMatcher getPathMatcher(String syntaxAndPattern) {
        // borrow the JDK's version and assumes it's POSIX like
        return FileSystems.getDefault().getPathMatcher(syntaxAndPattern);
    }

    @Override
    public UserPrincipalLookupService getUserPrincipalLookupService() {
        throw new UnsupportedOperationException("getUserPrincipalLookupService");
    }

    @Override
    public WatchService newWatchService() {
        throw new UnsupportedOperationException("newWatchService");
    }

    @Override
    public void close() throws IOException {
        if (open) {
            try {
                internalClose();
            } finally {
                open = false;
            }
        }
    }
}
