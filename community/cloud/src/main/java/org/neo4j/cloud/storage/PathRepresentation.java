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

import static java.util.Objects.requireNonNull;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.eclipse.collections.api.factory.Lists;
import org.eclipse.collections.api.list.MutableList;
import org.neo4j.util.Preconditions;

/**
 * A POSIX-like representation of some path on a storage system
 */
public class PathRepresentation {

    public static final String SEPARATOR = "/";
    public static final String CURRENT = ".";
    public static final String PARENT = "..";

    public static final PathRepresentation ROOT = new PathRepresentation(SEPARATOR);
    public static final PathRepresentation EMPTY_PATH = new PathRepresentation("");
    public static final char PATH_SEPARATOR_CHAR = SEPARATOR.charAt(0);
    public static final char CURRENT_DIR_CHAR = CURRENT.charAt(0);
    public static final char PARENT_DIR_CHAR = PARENT.charAt(0);

    private final String path;

    private PathRepresentation(String path) {
        this.path = requireNonNull(path, "path may not be null");
    }

    private PathRepresentation(char[] chars) {
        this(new String(requireNonNull(chars, "path characters may not be null")));
    }

    /**
     *
     * @param chars the characters that describe the path
     * @return the representation of the path constructed directly from the character arguments, i.e. the characters
     * are assumed to contain no redundant separators.
     */
    static PathRepresentation of(char... chars) {
        return new PathRepresentation(chars);
    }

    /**
     * Construct a POSIX-like path representation from a series of elements.
     * @param first the first element of the path, or the whole path if {@code more} is not defined. Must never be <code>null</code>
     * @param more  zero or more path elements. Itself and it's elements must never be <code>null</code>
     * @return the representation of the path. Note that redundant separators will be removed e.g. {@code "/", "/foo}
     * will become {@code "/foo"}. However, directory aliases {@code ".", ".."} will be retained.
     */
    public static PathRepresentation of(String first, String... more) {
        Preconditions.requireNonNull(first, "The first element of the path may not be null");
        Preconditions.requireNonNull(more, "The more elements of the path may not be null");
        Preconditions.requireNoNullElements(more);

        final var parts = Lists.mutable.of(more);
        parts.add(0, first);
        return new PathRepresentation(partsToPathString(parts));
    }

    /**
     *
     * @param path the base path to be subdivided.
     * @param beginIndex the initial segment index from which to start the new sub-path (inclusive)
     * @param endIndex the terminal segment index at which to end the new sub-path (exclusive)
     * @return the sub path based on the provided index arguments
     */
    public static String subpath(PathRepresentation path, int beginIndex, int endIndex) {
        final var elements = path.elements();
        final int size = elements.size();
        Preconditions.checkArgument(beginIndex >= 0, "begin index may not be < 0");
        Preconditions.checkArgument(beginIndex < size, "begin index may not be >= the number of path elements");
        Preconditions.checkArgument(endIndex <= size, "end index may not be > the number of path elements");
        Preconditions.checkArgument(endIndex >= beginIndex, "end index may not be <= the begin index");

        return String.join(SEPARATOR, elements.subList(beginIndex, endIndex));
    }

    /**
     * @return <code>true</code> if this path is the 'root' path, i.e. is {@link #SEPARATOR}
     */
    public boolean isRoot() {
        return path.equals(SEPARATOR);
    }

    /**
     *
     * @return <code>true</code> if this path is an absolute path, i.e. it's first segment is {@link #SEPARATOR}
     */
    public boolean isAbsolute() {
        return isAbsolutePart(path);
    }

    /**
     * @return <code>true</code> if this path is a directory. For cloud-storage paths this means it terminates in {@link #SEPARATOR}
     * or a directory alias
     */
    public boolean isDirectory() {
        return path.isEmpty()
                || isDirectoryPart(path)
                || path.equals(CURRENT)
                || path.equals(PARENT)
                || path.endsWith(PATH_SEPARATOR_CHAR + CURRENT)
                || path.endsWith(PATH_SEPARATOR_CHAR + PARENT);
    }

    /**
     * @return <code>true</code> if this path terminates in {@link #SEPARATOR}
     */
    public boolean hasTrailingSeparator() {
        return isDirectoryPart(path);
    }

    /**
     * @return the characters that make up this path
     */
    public char[] chars() {
        return path.toCharArray();
    }

    /**
     * @return the path segments that make up this path, i.e. the parts contained within {@link #SEPARATOR} characters
     */
    public List<String> elements() {
        if (isRoot()) {
            return Collections.emptyList();
        }

        return Arrays.stream(path.split(SEPARATOR))
                .filter(s -> !s.trim().isEmpty())
                .toList();
    }

    /**
     * @param beginIndex the initial segment index from which to start the new sub-path (inclusive)
     * @param endIndex the terminal segment index at which to end the new sub-path (exclusive)
     * @return the sub path based on the provided index arguments
     */
    public PathRepresentation subpath(int beginIndex, int endIndex) {
        var pathStr = subpath(this, beginIndex, endIndex);
        if (hasTrailingSeparator() || elements().size() > endIndex) {
            return new PathRepresentation(pathStr + SEPARATOR);
        } else {
            return new PathRepresentation(pathStr);
        }
    }

    /**
     * @return the parent path or <code>NULL</code> if this path is the root path
     */
    public PathRepresentation getParent() {
        final var size = elements().size();

        if (isRoot() || equals(EMPTY_PATH)) {
            return null;
        }
        if (size == 1) {
            return isAbsolute() ? ROOT : null;
        }

        var subPath = subpath(0, size - 1);
        return isAbsolute() ? new PathRepresentation(SEPARATOR + subPath.path) : subPath;
    }

    public int length() {
        return path.length();
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        final var that = (PathRepresentation) o;
        return path.equals(that.path);
    }

    @Override
    public int hashCode() {
        return path.hashCode();
    }

    @Override
    public String toString() {
        return path;
    }

    private static String partsToPathString(MutableList<String> allParts) {
        var path = allParts.stream()
                .flatMap(part -> Arrays.stream(part.split("/+")))
                .filter(p -> !p.isEmpty())
                .collect(Collectors.joining(SEPARATOR));

        if (isDirectoryPart(allParts.getLast()) && !isDirectoryPart(path)) {
            path = path + SEPARATOR;
        }
        if (isAbsolutePart(allParts.getFirst()) && !isAbsolutePart(path)) {
            path = SEPARATOR + path;
        }
        return path;
    }

    private static boolean isAbsolutePart(String path) {
        return !(path == null) && !path.isEmpty() && path.charAt(0) == PATH_SEPARATOR_CHAR;
    }

    private static boolean isDirectoryPart(String path) {
        return !path.isEmpty() && path.charAt(path.length() - 1) == PATH_SEPARATOR_CHAR;
    }
}
