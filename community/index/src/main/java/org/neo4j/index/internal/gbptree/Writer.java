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
import java.util.function.Function;

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
     * Aggregates multiple values within specified range into one using provided function.
     * Resulting value is placed as a value associated with the rightmost key in the range.
     * All other keys whose values were aggregated are removed.
     * @param fromInclusive lower bound of the range to aggregate (inclusive).
     * @param toExclusive higher bound of the range to aggregate (exclusive).
     * @param aggregator function to aggregate values
     * @return number of modified entries:
     *          if 0 - no changes done to the tree
     *          positive number N means N-1 entries were removed and 1 entry has its value updated
     */
    int aggregate(KEY fromInclusive, KEY toExclusive, ValueAggregator<VALUE> aggregator);

    /**
     * Updates value associated with the least key greater than or equal to the given key and strictly less than provided upper boundary.
     * @param searchKey - key to search
     * @param upperBound - upper bound
     * @param updateFunction - update function
     */
    void updateCeilingValue(KEY searchKey, KEY upperBound, Function<VALUE, VALUE> updateFunction);
}
