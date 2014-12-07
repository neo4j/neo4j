/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
public interface NumberArray extends AutoCloseable
{
    /**
     * @return length of the array, i.e. the capacity.
     */
    long length();

    /**
     * @return number of indexes occupied, i.e. setting the same index multiple times doesn't increment size.
     */
    long size();

    void swap( long fromIndex, long toIndex, int numberOfEntries );

    /**
     * Sets all values to a default value.
     */
    void clear();

    /**
     * @return highest set index or -1 if no set.
     */
    long highestSetIndex();

    void visitMemoryStats( MemoryStatsVisitor visitor );

    /**
     * Releases any resources that GC won't release automatically.
     */
    @Override
    public void close();
}
