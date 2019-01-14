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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Abstraction over primitive arrays.
 *
 * @see NumberArrayFactory
 */
public interface NumberArray<N extends NumberArray<N>> extends MemoryStatsVisitor.Visitable, AutoCloseable
{
    /**
     * @return length of the array, i.e. the capacity.
     */
    long length();

    /**
     * Swaps items from {@code fromIndex} to {@code toIndex}, such that
     * {@code fromIndex} and {@code toIndex}, {@code fromIndex+1} and {@code toIndex} a.s.o swaps places.
     * The number of items swapped is equal to the length of the default value of the array.
     *  @param fromIndex where to start swapping from.
     * @param toIndex where to start swapping to.
     */
    void swap( long fromIndex, long toIndex );

    /**
     * Sets all values to a default value.
     */
    void clear();

    /**
     * Releases any resources that GC won't release automatically.
     */
    @Override
    void close();

    /**
     * Part of the nature of {@link NumberArray} is that {@link #length()} can be dynamically growing.
     * For that to work some implementations (those coming from e.g
     * {@link NumberArrayFactory#newDynamicIntArray(long, int)} and such dynamic calls) has an indirection,
     * one that is a bit costly when comparing to raw array access. In scenarios where there will be two or
     * more access to the same index in the array it will be more efficient to resolve this indirection once
     * and return the "raw" array for that given index so that it can be used directly in multiple calls,
     * knowing that the returned array holds the given index.
     *
     * @param index index into the array which the returned array will contain.
     * @return array sure to hold the given index.
     */
    N at( long index );
}
