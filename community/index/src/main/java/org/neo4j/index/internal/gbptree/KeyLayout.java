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
package org.neo4j.index.internal.gbptree;

import static java.lang.String.format;

import java.util.Comparator;
import org.neo4j.io.pagecache.PageCursor;

/**
 * Key layout for customizing a {@link GBPTree}, how its keys are represented as bytes and what keys contains.
 * <p>
 * Additionally custom meta data can be supplied, which will be persisted in {@link GBPTree}.
 * <p>
 * Rather extend {@link KeyLayout.Adapter} as to get standard implementation of e.g. {@link KeyLayout.Adapter#toString()}.
 *
 * @param <KEY> type of key
 */
public interface KeyLayout<KEY> extends Comparator<KEY> {
    int FIXED_SIZE_KEY = -1;

    /**
     * Indicate if entries are fixed or dynamic size.
     * @return {@code true} if entries are fixed size, otherwise {@code false}.
     */
    boolean fixedSize();

    /**
     * @return new key instance.
     */
    KEY newKey();

    /**
     * Copies contents of {@code key} to {@code into}.
     *
     * @param key key (left unchanged as part of this call) to copy contents from.
     * @param into key (changed as part of this call) to copy contents into.
     * @return the provided {@code into} instance for convenience.
     */
    KEY copyKey(KEY key, KEY into);

    /**
     * Copies contents of {@code key} into a new key.
     *
     * @param key key (left unchanged as part of this call) to copy contents from.
     * @return the a new copy of the provided {@code into}.
     */
    default KEY copyKey(KEY key) {
        return copyKey(key, newKey());
    }

    /**
     * @param key for which to give size.
     * @return size, in bytes, of given key.
     */
    int keySize(KEY key);

    /**
     * Writes contents of {@code key} into {@code cursor} at its current offset.
     *
     * @param cursor {@link PageCursor} to write into, at current offset.
     * @param key key containing data to write.
     */
    void writeKey(PageCursor cursor, KEY key);

    /**
     * Reads key contents at {@code cursor} at its current offset into {@code key}.
     * @param cursor {@link PageCursor} to read from, at current offset.
     * @param into key instances to read into.
     * @param keySize size of key to read or {@link #FIXED_SIZE_KEY} if key is fixed size.
     */
    void readKey(PageCursor cursor, KEY into, int keySize);

    /**
     * Find the shortest key (best-effort) that separate left from right in sort order
     * and initialize into with result.
     * @param left key that is less than right
     * @param right key that is greater than left.
     * @param into will be initialized with result.
     */
    default void minimalSplitter(KEY left, KEY right, KEY into) {
        copyKey(right, into);
    }

    /**
     * Used as verification when loading an index after creation, to verify that the same layout is used,
     * as the one it was initially created with.
     *
     * @return a long acting as an identifier, written in the header of an index.
     */
    long identifier();

    /**
     * @return major version of layout. Will be compared to version written into meta page when opening index.
     */
    int majorVersion();

    /**
     * @return minor version of layout. Will be compared to version written into meta page when opening index.
     */
    int minorVersion();

    /**
     * Typically, a layout is compatible with given identifier, major and minor version if
     * <ul>
     * <li>{@code layoutIdentifier == this.identifier()}</li>
     * <li>{@code majorVersion == this.majorVersion()}</li>
     * <li>{@code minorVersion == this.minorVersion()}</li>
     * </ul>
     * <p>
     * When opening a {@link GBPTree tree} to 'use' it, read and write to it, providing a layout with the right compatibility is
     * important because it decides how to read and write entries in the tree.
     *
     * @param layoutIdentifier the stored layout identifier we want to check compatibility against.
     * @param majorVersion the stored major version we want to check compatibility against.
     * @param minorVersion the stored minor version we want to check compatibility against.
     * @return true if this layout is compatible with combination of identifier, major and minor version, false otherwise.
     */
    boolean compatibleWith(long layoutIdentifier, int majorVersion, int minorVersion);

    /**
     * Initializes the given key to a state where it's lower than any possible key in the tree.
     * @param key key to initialize.
     */
    void initializeAsLowest(KEY key);

    /**
     * Initializes the given key to a state where it's higher than any possible key in the tree.
     * @param key key to initialize.
     */
    void initializeAsHighest(KEY key);

    /**
     * Adapter for {@link KeyLayout}, which contains convenient standard implementations of some methods.
     *
     * @param <KEY> type of key
     */
    abstract class Adapter<KEY> implements KeyLayout<KEY> {
        private final boolean fixedSize;
        private final long identifier;
        private final int majorVersion;
        private final int minorVersion;

        protected Adapter(boolean fixedSize, long identifier, int majorVersion, int minorVersion) {
            this.fixedSize = fixedSize;
            this.identifier = identifier;
            this.majorVersion = majorVersion;
            this.minorVersion = minorVersion;
        }

        @Override
        public boolean fixedSize() {
            return fixedSize;
        }

        @Override
        public long identifier() {
            return identifier;
        }

        @Override
        public int majorVersion() {
            return majorVersion;
        }

        @Override
        public int minorVersion() {
            return minorVersion;
        }

        @Override
        public String toString() {
            return format(
                    "%s[version:%d.%d, identifier:%d, fixedSize:%b]",
                    getClass().getSimpleName(), majorVersion(), minorVersion(), identifier(), fixedSize);
        }

        @Override
        public boolean compatibleWith(long layoutIdentifier, int majorVersion, int minorVersion) {
            return layoutIdentifier == identifier() && majorVersion == majorVersion() && minorVersion == minorVersion();
        }
    }
}
