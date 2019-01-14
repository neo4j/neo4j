/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
package org.neo4j.index.internal.gbptree;

/**
 * Decides what to do when inserting key which already exists in index. Different implementations of
 * {@link ValueMerger} can result in unique/non-unique indexes for example.
 *
 * @param <KEY> type of keys to merge.
 * @param <VALUE> type of values to merge.
 */
public interface ValueMerger<KEY,VALUE>
{
    /**
     * Merge an existing value with a new value, returning potentially a combination of the two, or {@code null}
     * if no merge was done effectively meaning that nothing should be written.
     *
     * @param existingKey existing key
     * @param newKey new key
     * @param existingValue existing value
     * @param newValue new value
     * @return {@code newValue}, now merged with {@code existingValue}, or {@code null} if no merge was done.
     */
    VALUE merge( KEY existingKey, KEY newKey, VALUE existingValue, VALUE newValue );
}
