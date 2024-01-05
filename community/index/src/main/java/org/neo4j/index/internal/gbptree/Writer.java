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
import java.io.UncheckedIOException;

/**
 * Able to {@link #merge(Object, Object, ValueMerger)} and {@link #remove(Object)} key/value pairs
 * into a {@link GBPTree}. After all modifications have taken place the writer must be {@link #close() closed},
 * typically using try-with-resource clause.
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public interface Writer<KEY, VALUE> extends Closeable {
    /**
     * Associate given {@code key} with given {@code value}.
     * Any existing {@code value} associated with {@code key} will be overwritten.
     *
     * @param key key to associate with value
     * @param value value to associate with key
     * @throws UncheckedIOException on index access error.
     */
    void put(KEY key, VALUE value);

    /**
     * A {@link Writer} can preserve state and internal latches between calls to e.g.
     * {@link #put(Object, Object)} or {@link #merge(Object, Object, ValueMerger)} for better performance.
     * If the user of a writer knows that the next write call will not be immediate,
     * it may be beneficial to release such latches and state so that other parallel {@link Writer writers}
     * can progress unhindered. This method will release any such state and latches.
     */
    void yield();

    /**
     * If the {@code key} doesn't already exist in the index the {@code key} will be added and the {@code value}
     * associated with it.
     * If the {@code key} already exists then its existing {@code value} will be merged with the given {@code value}, using the {@link ValueMerger}.
     *
     * @param key key for which to merge values.
     * @param value value to merge with currently associated value for the {@code key}.
     * @param valueMerger {@link ValueMerger} to consult if key already exists.
     * @throws UncheckedIOException on index access error.
     * @see ValueMerger#merge(Object, Object, Object, Object)
     */
    void merge(KEY key, VALUE value, ValueMerger<KEY, VALUE> valueMerger);

    /**
     * If the {@code key} already exists then its existing {@code value} will be merged with the given {@code value}, using the {@link ValueMerger}.
     * If the {@code key} doesn't exist then no changes will be made and {@code false} will be returned.
     *
     * @param key key for which to merge values.
     * @param value value to merge with currently associated value for the {@code key}.
     * @param valueMerger {@link ValueMerger} to consult if key already exists.
     * @throws UncheckedIOException on index access error.
     * @see ValueMerger#merge(Object, Object, Object, Object)
     */
    void mergeIfExists(KEY key, VALUE value, ValueMerger<KEY, VALUE> valueMerger);

    /**
     * Removes a key, returning it's associated value, if found.
     *
     * @param key key to remove.
     * @return value which was associated with the removed key, if found, otherwise {@code null}.
     * @throws UncheckedIOException on index access error.
     */
    VALUE remove(KEY key);

    /**
     * Executes custom write operation on tree.
     * When parallel writes enabled, operation could be executed up to two times: first in optimistic mode, then in
     * pessimistic, with full lock on the tree.
     * Care should be taken when creating custom write operation. It should work well together with other operations
     * and adhere structure and concurrency coordination practices. See {@link TreeWriteOperation}
     *
     * @param operation operation to execute
     */
    void execute(TreeWriteOperation<KEY, VALUE> operation);
}
