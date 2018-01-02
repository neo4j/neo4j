/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
 * Dynamically growing {@link LongArray}. Is given a chunk size and chunks are added as higher and higher
 * items are requested.
 *
 * @see NumberArrayFactory#newDynamicIntArray(long, int)
 */
public class DynamicIntArray extends DynamicNumberArray<IntArray> implements IntArray
{
    private final int defaultValue;

    public DynamicIntArray( NumberArrayFactory factory, long chunkSize, int defaultValue )
    {
        super( factory, chunkSize );
        this.defaultValue = defaultValue;
    }

    @Override
    public int get( long index )
    {
        IntArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get( index( index ) ) : defaultValue;
    }

    @Override
    public void set( long index, int value )
    {
        ensureChunkAt( index ).set( index( index ), value );
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        // Let's just do this the stupid way. There's room for optimization here
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            int intermediary = get( fromIndex+i );
            set( fromIndex+i, get( toIndex+i ) );
            set( toIndex+i, intermediary );
        }
    }

    @Override
    protected IntArray addChunk( long chunkSize )
    {
        return factory.newIntArray( chunkSize, defaultValue );
    }

    @Override
    public IntArray fixate()
    {
        return new FixedIntArray( chunks, chunkSize, defaultValue );
    }
}
