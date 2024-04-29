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

import java.io.Closeable;
import java.io.IOException;
import java.util.Collection;
import java.util.List;
import org.neo4j.io.pagecache.context.CursorContext;

/**
 * Seeks and allows read access to data in a {@link GBPTree}. The interaction is cursor-like, where the next item is visited
 * on every call to {@link #next()} and if returning {@code true} the entry data can be accessed using {@link #key()} and {@link #value()}.
 *
 * @param <KEY> type of key in this tree.
 * @param <VALUE> type of value in this value.
 */
public interface Seeker<KEY, VALUE> extends Closeable {
    /**
     * Moves this seeker to the next result in this seek.
     *
     * @return {@code true} if there was a result to go to where the data for this result item can be accessed from {@link #key()} and {@link #value()}.
     * Otherwise {@code false} is returned meaning that the seek is exhausted.
     * @throws IOException on I/O error.
     */
    boolean next() throws IOException;

    /**
     * @return key of the current result item, i.e. from the most recent call to a successful and true-returning {@link #next()}.
     * @throws IllegalStateException if called before first invocation of true-returning {@link #next()}.
     */
    KEY key();

    /**
     * @return value of the current result item, i.e. from the most recent call to a successful and true-returning {@link #next()}.
     * @throws IllegalStateException if called before first invocation of true-returning {@link #next()}.
     */
    VALUE value();

    /**
     * Moves this seeker to specified range by reinitializing it
     *
     * @param fromInclusive lower bound of the range to seek (inclusive).
     * @param toExclusive higher bound of the range to seek (exclusive).
     * @throws UnsupportedOperationException on non-implemented calls
     */
    default void reinitializeToNewRange(KEY fromInclusive, KEY toExclusive) {
        throw new UnsupportedOperationException();
    }

    interface Factory<KEY, VALUE> {
        /**
         * Allocates a {@link Seeker} instance for seeking in the tree. Caller can reuse the returned instance for multiple calls to
         * {@link #seek(Seeker, Object, Object)} and in the end caller is also responsible for closing the seeker.
         *
         * @param cursorContext {@link CursorContext} to use in the {@link Seeker} instance.
         * @return the allocated and reusable {@link Seeker} instance.
         * @throws IOException on I/O error opening a seeker.
         */
        Seeker<KEY, VALUE> allocateSeeker(CursorContext cursorContext) throws IOException;

        /**
         * Seeks hits in this tree, given a key range. Hits are iterated over using the returned {@link Seeker}.
         * There's no guarantee that neither the {@link KEY} nor the {@link VALUE} instances are immutable and so
         * if caller wants to cache the results it's safest to copy the instances, or rather their contents,
         * into its own result cache.
         * <p>
         * Seeks can go either forwards or backwards depending on the values of the key arguments.
         * <ul>
         * <li>
         * A {@code fromInclusive} that is smaller than the {@code toExclusive} results in results in ascending order.
         * </li>
         * <li>
         * A {@code fromInclusive} that is bigger than the {@code toExclusive} results in results in descending order.
         * </li>
         * </ul>
         *
         * @param fromInclusive lower bound of the range to seek (inclusive).
         * @param toExclusive higher bound of the range to seek (exclusive).
         * @param cursorContext underlying page cursor context
         * @return a {@link Seeker} used to iterate over the hits within the specified key range.
         * @throws IOException on error reading from index.
         */
        default Seeker<KEY, VALUE> seek(KEY fromInclusive, KEY toExclusive, CursorContext cursorContext)
                throws IOException {
            return seek(allocateSeeker(cursorContext), fromInclusive, toExclusive);
        }

        /**
         * Seeks with an existing {@link Seeker} instance, for reduced overhead.
         *
         * @see #seek(Object, Object, CursorContext)
         * @param seeker {@link Seeker} instance to use to do this seek. This instance is also returned for convenience.
         * @param fromInclusive lower bound of the range to seek (inclusive).
         * @param toExclusive higher bound of the range to seek (exclusive).
         * @return a {@link Seeker} used to iterate over the hits within the specified key range.
         * @throws IOException on error reading from index.
         */
        Seeker<KEY, VALUE> seek(Seeker<KEY, VALUE> seeker, KEY fromInclusive, KEY toExclusive) throws IOException;

        /**
         * Partitions the provided key range into {@code numberOfPartitions} partitions and instantiates a {@link Seeker} for each.
         * Caller can seek through the partitions in parallel. Caller is responsible for closing the returned {@link Seeker seekers}.
         *
         * @param fromInclusive lower bound of the target range to seek (inclusive).
         * @param toExclusive higher bound of the target range to seek (exclusive).
         * @param numberOfPartitions number of partitions desired by the caller. If the tree is small a lower number of partitions may be returned.
         * The number of partitions will never be higher than the provided {@code numberOfPartitions}.
         * @return a {@link Collection} of {@link Seeker seekers}, each having their own distinct partition to seek. Collectively they
         * seek across the whole provided range.
         * @throws IOException on error reading from index.
         */
        List<KEY> partitionedSeek(
                KEY fromInclusive, KEY toExclusive, int numberOfPartitions, CursorContext cursorContext)
                throws IOException;
    }
}
