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
package org.neo4j.unsafe.impl.batchimport.cache;

/**
 * Abstraction over primitive arrays.
 *
 * @see NumberArrayFactory
 */
public interface NumberArray extends MemoryStatsVisitor.Visitable, AutoCloseable
{
    /**
     * @return length of the array, i.e. the capacity.
     */
    long length();

    /**
     * Swaps {@code numberOfEntries} items from {@code fromIndex} to {@code toIndex}, such that
     * {@code fromIndex} and {@code toIndex}, {@code fromIndex+1} and {@code toIndex} a.s.o swaps places.
     *
     * @param fromIndex where to start swapping from.
     * @param toIndex where to start swapping to.
     * @param numberOfEntries number of entries to swap, starting from the given from/to indexes.
     */
    void swap( long fromIndex, long toIndex, int numberOfEntries );

    /**
     * Sets all values to a default value.
     */
    void clear();

    /**
     * Releases any resources that GC won't release automatically.
     */
    @Override
    void close();
}
