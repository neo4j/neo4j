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

/**
 * Decides what to do when inserting key which already exists in index. Different implementations of
 * {@link ValueMerger} can result in unique/non-unique indexes for example.
 *
 * @param <VALUE> type of values to merge.
 */
public interface ValueMerger<VALUE>
{
    /**
     * Merge an existing value with a new value, returning potentially a combination of the two, or {@code null}
     * if no merge was done effectively meaning that nothing should be written.
     *
     * @param existingValue existing value
     * @param newValue new value
     * @return {@code value}, now merged with {@code withValue}, or {@code null} if no merge was done.
     */
    VALUE merge( VALUE existingValue, VALUE newValue );
}
