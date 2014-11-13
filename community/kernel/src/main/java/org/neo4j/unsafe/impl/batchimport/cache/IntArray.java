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
 * Implemented as just a {@link LongArray} with an {@link IntArray} interface to it. An attempt was made
 * to have each long in the underlying {@link LongArray} store two ints, but there was concurrency issues
 * with false sharing and where added synchronization would be too much of an overhead.
 *
 * The reason it just wraps a {@link LongArray} is that there's a bunch of classes around {@link LongArray},
 * f.ex {@link LongArrayFactory}, {@link HeapLongArray} and {@link OffHeapLongArray} which would be bad to duplicate.
 *
 * Moving forward the implementation should rather use 4 bytes per int, instead of currently 8 bytes.
 */
public class IntArray implements NumberArray
{
    private final LongArray longs;

    public IntArray( LongArrayFactory factory, long chunkSize, int defaultValue )
    {
        this.longs = factory.newDynamicLongArray( chunkSize, defaultValue );
    }

    @Override
    public long length()
    {
        return longs.length();
    }

    public int get( long index )
    {
        return (int) longs.get( index );
    }

    public void set( long index, int value )
    {
        longs.set( index, value );
    }

    @Override
    public void clear()
    {
        longs.clear();
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        longs.swap( fromIndex, toIndex, numberOfEntries );
    }

    @Override
    public long highestSetIndex()
    {
        return longs.highestSetIndex();
    }

    @Override
    public long size()
    {
        return longs.size();
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        longs.visitMemoryStats( visitor );
    }
}
