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
         * Decides relatively how many keys (with values/children) that are retained in left node during a split.
         * Using 0.5 will spread the keys as evenly as possible among the nodes. However, in certain scenarios it can
         * be beneficial to use a different factor.
         * <p>
         * When batch inserting keys in order it is possible to leave every node completely full.
         * This will make the batch insert finish quicker, but the first subsequent writes will all cause splits all
         * the way to the root. If the tree is not expected to change after the batch insert, this
         * can be a good trade off.
         * <p>
         * Factor 1 will leave all keys in left part of split, use when batch inserting in order.
         * Factor 0 will move all keys to right part of split, use when batch inserting in reverse order.
         * <p>
         * <pre>
         *     Retention factor = 0.5
         *     [K1 K2 K3 K4] // insert K5
         *     [K1 K2 __ __] [K3 K4 K5 __]
         *
         *     Retention factor = 1
         *     [K1 K2 K3 K4] // insert K5
         *     [K1 K2 K3 K4] [K5 __ __ __]
         *
         *     Retention factor = 0
         *     [K2 K3 K4 K5] // insert K1 (note reversed insert order)
         *     [K1 __ __ __] [K2 K3 K4 K5]
         * </pre>
         *
         * @return a factor between 0..1 where lower means more keys moved to right sibling,
         * and higher means more keys retained in left sibling. 0.5 will split keys evenly among left and right.
         */
        float splitRetentionFactor();

        class Defaults implements Options
        {
            @Override
            public float splitRetentionFactor()
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
            public float splitRetentionFactor()
            {
                return 1f;
            }
        };
    }
}
