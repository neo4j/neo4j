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

import java.util.Arrays;

/**
 * Dynamically growing {@link LongArray}. Is given a chunk size and chunks are added as higher and higher
 * items are requested.
 */
public class DynamicLongArray implements LongArray
{
    private final LongArrayFactory factory;
    private final long chunkSize;
    private LongArray[] chunks = new LongArray[0];
    private Long setAllValue;

    public DynamicLongArray( LongArrayFactory factory, long chunkSize )
    {
        this.factory = factory;
        this.chunkSize = chunkSize;
    }

    /**
     * @return the current length of this dynamically growing array.
     */
    @Override
    public long length()
    {
        return chunks.length*chunkSize;
    }

    @Override
    public long get( long index )
    {
        int chunkIndex = chunkIndex( index );
        return chunkIndex < chunks.length ? chunks[chunkIndex].get( index( index ) ) : defaultValue();
    }

    private long defaultValue()
    {
        return setAllValue != null ? setAllValue.longValue() : 0;
    }

    @Override
    public void set( long index, long value )
    {
        while ( index >= length() )
        {
            addChunk();
        }

        chunk( index ).set( index( index ), value );
    }

    @Override
    public long highestSetIndex()
    {
        for ( int i = chunks.length-1; i >= 0; i-- )
        {
            LongArray chunk = chunks[i];
            long highestSetInChunk = chunk.highestSetIndex();
            if ( highestSetInChunk > -1 )
            {
                return i*chunkSize + highestSetInChunk;
            }
        }
        return -1;
    }

    private long index( long index )
    {
        return index % chunkSize;
    }

    private LongArray chunk( long index )
    {
        return chunks[chunkIndex( index )];
    }

    private int chunkIndex( long index )
    {
        return (int) (index/chunkSize);
    }

    private void addChunk()
    {
        chunks = Arrays.copyOf( chunks, chunks.length+1 );
        LongArray newLongArray = factory.newLongArray( chunkSize );
        if ( setAllValue != null )
        {
            newLongArray.setAll( setAllValue );
        }
        chunks[chunks.length-1] = newLongArray;
    }

    @Override
    public LongArray setAll( long value )
    {
        for ( LongArray chunk : chunks )
        {
            chunk.setAll( value );
        }
        setAllValue = value;
        return this;
    }

    @Override
    public void swap( long fromIndex, long toIndex, int numberOfEntries )
    {
        // Let's just do this the stupid way. There's room for optimization here
        for ( int i = 0; i < numberOfEntries; i++ )
        {
            long intermediary = get( fromIndex+i );
            set( fromIndex+i, get( toIndex+i ) );
            set( toIndex+i, intermediary );
        }
    }

    @Override
    public void visitMemoryStats( MemoryStatsVisitor visitor )
    {
        for ( LongArray chunk : chunks )
        {
            chunk.visitMemoryStats( visitor );
        }
    }
}
