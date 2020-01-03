/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.collector;

import java.util.function.Consumer;

/**
 * Bounded buffer which holds the last n elements. When the buffer is full, each
 * produce will replace the elements in the buffer that was added first.
 *
 * This collection thread-safely allows
 *  - multiple threads concurrently calling `produce`
 *  - serialized calling of `clear` and `foreach`
 *
 * @param <T> type of elements in this buffer.
 */
public interface RecentBuffer<T>
{
    /**
     * Produce element into the buffer.
     *
     * @param t element to produce
     */
    void produce( T t );

    /**
     * Clear all elements from the buffer.
     */
    void clear();

    /**
     * Iterate over all elements in the buffer. No elements are removed from the buffer.
     *
     * @param consumer consumer to apply on each element
     */
    void foreach( Consumer<T> consumer );
}
