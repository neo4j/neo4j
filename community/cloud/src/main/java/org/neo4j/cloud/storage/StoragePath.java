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
import static org.neo4j.cloud.storage.PathRepresentation.CURRENT;
import static org.neo4j.cloud.storage.PathRepresentation.CURRENT_DIR_CHAR;
import static org.neo4j.cloud.storage.PathRepresentation.EMPTY_PATH;
import static org.neo4j.cloud.storage.PathRepresentation.PARENT;
import static org.neo4j.cloud.storage.PathRepresentation.PATH_SEPARATOR_CHAR;
import static org.neo4j.cloud.storage.PathRepresentation.SEPARATOR;

import java.io.File;
import java.net.URI;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.ProviderMismatchException;
import java.nio.file.WatchEvent.Kind;
import java.nio.file.WatchEvent.Modifier;
import java.nio.file.WatchKey;
import java.nio.file.WatchService;
import java.util.ArrayDeque;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import org.neo4j.internal.helpers.collection.Iterables;
import org.neo4j.util.Preconditions;

public class StoragePath implements Path {

    private final StorageSystem storage;

    private final PathRepresentation path;

    StoragePath(StorageSystem storage, PathRepresentation path) {
        this.storage = Objects.requireNonNull(storage);
        this.path = Objects.requireNonNull(path);
    }

    public static boolean isRoot(StoragePath storagePath) {
        return storagePath.path.isRoot();
    }

    /**
     * Checks if the given path is a storage path for a backend that doesn't support empty directories
     *
     * This can be useful because we don't create empty directories in some cloud providers.
     * If that is the case, checking for the existence of a directory in tools like backup doesn't make sense, but it does make sense for your local file system.
     */
    public static boolean isStorageDir(Path path) {
        if (path instanceof StoragePath storagePath) {
            return storagePath.isDirectory() && !storagePath.storage.supportsEmptyDirs();
        }
        return false;
    }

    public static boolean isEmpty(StoragePath storagePath) {
        return Objects.equals(storagePath.path, EMPTY_PATH);
    }

    public String scheme() {
        return storage.scheme();
    }

    public boolean isDirectory() {
        return path.isDirectory();
    }

    @Override
    public StorageSystem getFileSystem() {
        return storage;
    }

    @Override
    public boolean isAbsolute() {
        return path.isAbsolute();
    }

    @Override
    public StoragePath getRoot() {
        return isAbsolute() ? new StoragePath(storage, PathRepresentation.ROOT) : null;
    }

    @Override
    public StoragePath getFileName() {
        if (path.isRoot() || EMPTY_PATH.equals(path)) {
            return null;
        }

        final var elements = path.elements();
        if (path.hasTrailingSeparator()) {
            return from(last(elements) + SEPARATOR);
        } else {
            return from(last(elements));
        }
    }

    @Override
    public StoragePath getParent() {
        final var parent = path.getParent();
        return parent == null ? null : new StoragePath(storage, parent);
    }

    @Override
    public int getNameCount() {
        return path.elements().size();
    }

    @Override
    public StoragePath getName(int index) {
        return subpath(index, index + 1);
    }

    @Override
    public StoragePath subpath(int beginIndex, int endIndex) {
        return new StoragePath(storage, path.subpath(beginIndex, endIndex));
    }

    @Override
    public boolean startsWith(Path other) {
        if (!storage.equals(other.getFileSystem())) {
            return false;
        }

        if (isAbsolute() != other.isAbsolute()) {
            return false;
        }

        if (other.getNameCount() > getNameCount()) {
            return false;
        }

        if (other instanceof StoragePath sp) {
            return path.equals(sp.path)
                    || (path.length() >= sp.path.length()
                            && checkPrefixedParts(split(path, false), split(sp.path, false)));
        }

        return false;
    }

    @Override
    public boolean startsWith(String other) {
        return startsWith(from(other));
    }

    @Override
    public boolean endsWith(Path other) {
        if (!storage.equals(other.getFileSystem())) {
            return false;
        }

        if (other.isAbsolute() && !isAbsolute()) {
            return false;
        }

        if (other.getNameCount() > getNameCount()) {
            return false;
        }

        if (other instanceof StoragePath sp) {
            return path.equals(sp.path)
                    || (path.length() >= sp.path.length()
                            && path.hasTrailingSeparator() == sp.path.hasTrailingSeparator()
                            && checkPrefixedParts(split(path, true), split(sp.path, true)));
        }

        return false;
    }

    @Override
    public boolean endsWith(String other) {
        return endsWith(from(other));
    }

    @Override
    public StoragePath normalize() {
        if (path.isRoot()) {
            return this;
        }

        final var elements = path.elements();
        final var normalized = new ArrayDeque<String>(elements.size());
        for (var element : elements) {
            if (element.equals(CURRENT)) {
                continue;
            }
            if (element.equals(PARENT)) {
                normalized.pollLast();
                continue;
            }

            normalized.addLast(element);
        }

        final var parts = new StringBuilder(String.join(SEPARATOR, normalized));
        if (path.isAbsolute()) {
            parts.insert(0, SEPARATOR);
        }

        if (path.hasTrailingSeparator() || !Objects.equals(last(elements), normalized.peekLast())) {
            parts.append(SEPARATOR);
        }
        return from(parts.toString());
    }

    @Override
    public StoragePath resolve(Path other) {
        final var storagePath = ensureStoragePath(other);
        if (storagePath.isAbsolute()) {
            return storagePath;
        }
        if (storagePath.path.equals(EMPTY_PATH)) {
            return this;
        }

        if (storage.canResolve(storagePath)) {
            String resolvedPath;
            if (!path.hasTrailingSeparator()) {
                resolvedPath = this + SEPARATOR + storagePath;
            } else {
                resolvedPath = toString() + storagePath;
            }

            return from(resolvedPath);
        }

        throw new ProviderMismatchException(
                "A storage path can only resolve another storage path within the same storage system");
    }

    @Override
    public StoragePath resolve(String other) {
        return resolve(from(other));
    }

    @Override
    public StoragePath resolveSibling(String other) {
        return getParent().resolve(other);
    }

    @Override
    public StoragePath resolveSibling(Path other) {
        return getParent().resolve(other);
    }

    @Override
    public StoragePath relativize(Path other) {
        final var otherPath = ensureStoragePath(other);
        if (equals(otherPath)) {
            return new StoragePath(storage, EMPTY_PATH);
        }

        if (!storage.canResolve(otherPath)) {
            throw new ProviderMismatchException(
                    "A storage path can only relativize another storage path within the same storage system");
        }

        Preconditions.checkArgument(
                isAbsolute() == other.isAbsolute(),
                "to obtain a relative path both must be absolute or both must be relative");

        if (path.equals(EMPTY_PATH)) {
            return otherPath;
        }

        final var nameCount = getNameCount();
        final var otherNameCount = other.getNameCount();

        final var limit = Math.min(nameCount, otherNameCount);
        final var differenceCount = getDifferenceCount(otherPath, limit);

        var parentDirCount = nameCount - differenceCount;
        if (differenceCount < otherNameCount) {
            return getRelativePathFromDifference(otherPath, otherNameCount, differenceCount, parentDirCount);
        }

        final var relativePath = new char[parentDirCount * 3 - 1]; // 3 chars of ../ for each parent
        int index = 0;
        while (parentDirCount > 0) {
            relativePath[index++] = CURRENT_DIR_CHAR;
            relativePath[index++] = CURRENT_DIR_CHAR;
            if (parentDirCount > 1) {
                relativePath[index++] = PATH_SEPARATOR_CHAR;
            }
            parentDirCount--;
        }

        return new StoragePath(storage, PathRepresentation.of(relativePath));
    }

    @Override
    public URI toUri() {
        final var uri = new StringBuilder(getFileSystem().uriPrefix()).append(SEPARATOR);
        for (var step : toAbsolutePath().toRealPath(NOFOLLOW_LINKS)) {
            var name = step.getFileName().toString();
            final var isDir = name.endsWith(SEPARATOR);
            if (isDir) {
                // need to strip it off to prevent it getting encoded
                name = name.substring(0, name.length() - 1);
            }
            uri.append(URLEncoder.encode(name, StandardCharsets.UTF_8));
            if (isDir) {
                // now add it back again
                uri.append(SEPARATOR);
            }
        }

        return URI.create(uri.toString());
    }

    @Override
    public StoragePath toAbsolutePath() {
        if (isAbsolute()) return this;

        return new StoragePath(storage, PathRepresentation.of(SEPARATOR, path.toString()));
    }

    @Override
    public StoragePath toRealPath(LinkOption... options) {
        final var p = isAbsolute() ? this : toAbsolutePath();
        return from(SEPARATOR, p.normalize().toString());
    }

    @Override
    public int compareTo(Path other) {
        final var storagePath = ensureStoragePath(other);
        if (storagePath.storage != this.storage) {
            throw new ClassCastException("compared storage paths must be from the same storage system");
        }

        return toRealPath(NOFOLLOW_LINKS)
                .toString()
                .compareTo(storagePath.toRealPath(NOFOLLOW_LINKS).toString());
    }

    @Override
    public Iterator<Path> iterator() {
        return new StoragePathIterator(path.elements().iterator(), path.isAbsolute(), path.hasTrailingSeparator());
    }

    @Override
    public File toFile() {
        throw new UnsupportedOperationException("Storage paths cannot be converted to File objects");
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>[] events, Modifier... modifiers) {
        throw new UnsupportedOperationException("register");
    }

    @Override
    public WatchKey register(WatchService watcher, Kind<?>... events) {
        throw new UnsupportedOperationException("register");
    }

    @Override
    public String toString() {
        return path.toString();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }

        StoragePath that = (StoragePath) o;
        return storage.equals(that.storage) && path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return Objects.hash(storage, path);
    }

    private StoragePath getRelativePathFromDifference(
            StoragePath otherPath, int otherNameCount, int differenceCount, int parentDirCount) {
        Objects.requireNonNull(otherPath);
        final var remainingSubPath = otherPath.subpath(differenceCount, otherNameCount);

        if (parentDirCount == 0) {
            return remainingSubPath;
        }

        // we need to pop up some directories (i.e. three characters ../) then append the remaining sub-path
        var relativePathSize =
                parentDirCount * 3 + remainingSubPath.path.toString().length();

        if (otherPath.path.equals(EMPTY_PATH)) {
            relativePathSize--;
        }

        final var relativePath = new char[relativePathSize];
        var index = 0;
        while (parentDirCount > 0) {
            relativePath[index++] = CURRENT_DIR_CHAR;
            relativePath[index++] = CURRENT_DIR_CHAR;
            if (otherPath.path.equals(EMPTY_PATH)) {
                if (parentDirCount > 1) relativePath[index++] = PATH_SEPARATOR_CHAR;
            } else {
                relativePath[index++] = PATH_SEPARATOR_CHAR;
            }
            parentDirCount--;
        }
        System.arraycopy(remainingSubPath.path.chars(), 0, relativePath, index, remainingSubPath.path.chars().length);

        return new StoragePath(storage, PathRepresentation.of(relativePath));
    }

    private int getDifferenceCount(StoragePath other, int limit) {
        var i = 0;
        while (i < limit) {
            if (!Objects.equals(getName(i), other.getName(i))) {
                break;
            }
            i++;
        }
        return i;
    }

    private StoragePath from(String first, String... more) {
        return new StoragePath(storage, PathRepresentation.of(first, more));
    }

    private static StoragePath ensureStoragePath(Path other) {
        if (other instanceof StoragePath path) {
            return path;
        }

        throw new ProviderMismatchException("Path provided is not a StoragePath: " + other);
    }

    private static Iterator<String> split(PathRepresentation path, boolean reversed) {
        final var elements = path.elements();
        return reversed ? Iterables.reverse(elements).iterator() : elements.iterator();
    }

    private static boolean checkPrefixedParts(Iterator<String> theMatches, Iterator<String> toMatch) {
        while (toMatch.hasNext()) {
            if (!theMatches.hasNext() || !theMatches.next().equals(toMatch.next())) {
                return false;
            }
        }
        return true;
    }

    private static String last(List<String> items) {
        return items.isEmpty() ? null : items.get(items.size() - 1);
    }

    private class StoragePathIterator implements Iterator<Path> {
        private final Iterator<String> delegate;
        private final boolean isAbsolute;
        private final boolean hasTrailingSeparator;
        private boolean first;

        private StoragePathIterator(Iterator<String> delegate, boolean isAbsolute, boolean hasTrailingSeparator) {
            this.delegate = delegate;
            this.isAbsolute = isAbsolute;
            this.hasTrailingSeparator = hasTrailingSeparator;
            this.first = true;
        }

        @Override
        public boolean hasNext() {
            return delegate.hasNext();
        }

        @Override
        public StoragePath next() {
            String pathString = delegate.next();
            if (isAbsolute && first) {
                first = false;
                pathString = SEPARATOR + pathString;
                if (!hasNext() && hasTrailingSeparator) {
                    pathString = pathString + SEPARATOR;
                }
            }

            if (hasNext() || hasTrailingSeparator) {
                pathString = pathString + SEPARATOR;
            }
            return from(pathString);
        }
    }
}
