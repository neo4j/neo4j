/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.index;

import java.io.Closeable;
import java.io.IOException;

/**
 * Able to {@link #merge(Object, Object, ValueMerger)} and {@link #remove(Object)} key/value pairs
 * into an {@link Index}. After all modifications have taken place the writer must be {@link #close() closed},
 * typically using try-with-resource clause.
 *
 * @param <KEY> type of keys
 * @param <VALUE> type of values
 */
public interface IndexWriter<KEY,VALUE> extends Closeable
{
    /**
     * Associate given {@code key} with given {@code value}.
     * Any existing {@code value} associated with {@code key} will be overwritten.
     *
     * @param key key to associate with value
     * @param value value to associate with key
     * @throws IOException on index access error.
     */
    void put( KEY key, VALUE value ) throws IOException;

    /**
     * If the {@code key} doesn't already exist in the index the {@code key} will be added and the {@code value}
     * associated with it. If the {@code key} already exists then its existing {@code value} will be merged with
     * the given {@code value}, using the {@link ValueMerger}. If the {@link ValueMerger} returns a non-null
     * value that value will be associated with the {@code key}, otherwise (if it returns {@code null}) nothing will
     * be written.
     *
     * @param key key for which to merge values.
     * @param value value to merge with currently associated value for the {@code key}.
     * @param valueMerger {@link ValueMerger} to consult if key already exists.
     * @throws IOException on index access error.
     */
    void merge( KEY key, VALUE value, ValueMerger<VALUE> valueMerger ) throws IOException;

    /**
     * Removes a key, returning it's associated value, if found.
     *
     * @param key key to remove.
     * @return value which was associated with the remove key, if found, otherwise {@code null}.
     * @throws IOException on index access error.
     */
    VALUE remove( KEY key ) throws IOException;

    /**
     * Options which applies to modifications, these options can aid implementations to do more things
     * more efficiently in various scenarios.
     */
    interface Options
    {
        /**
         * Decides relatively where a split happens, i.e. which position will be the split key.
         * Keys (and its values/children) including the split key will go to the right tree node,
         * everything before it goes into the left.
         *
         * @return a factor between 0..1 where 0 means far to the left, 1 means far to the right and
         * as an example 0.5 will select the middle item (floor division).
         */
        float splitLeftChildSize();

        class Defaults implements Options
        {
            @Override
            public float splitLeftChildSize()
            {
                return 0.5f;
            }
        }

        /**
         * Default options best suitable for most occasions.
         */
        Options DEFAULTS = new Defaults();

        /**
         * Options best suitable in batching scenarios, where insertions come in sequentially (by order of key)
         * and are typically densely packed.
         */
        Options BATCHING_SEQUENTIAL = new Defaults()
        {
            @Override
            public float splitLeftChildSize()
            {
                return 1f;
            }
        };
    }
}
