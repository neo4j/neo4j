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

import java.util.Arrays;

/**
 * Base class for common functionality for any {@link NumberArray} where the data is dynamically growing,
 * where parts can live inside and parts off-heap.
 *
 * @see NumberArrayFactory#newDynamicLongArray(long, long)
 * @see NumberArrayFactory#newDynamicIntArray(long, int)
 */
abstract class DynamicNumberArray<N extends NumberArray> extends ChunkedNumberArray<N>
{
    protected final NumberArrayFactory factory;

    DynamicNumberArray( NumberArrayFactory factory, long chunkSize )
    {
        super( chunkSize );
        this.factory = factory;
        this.chunks = new NumberArray[0];
    }

    protected N ensureChunkAt( long index )
    {
        if ( index >= length() )
        {
            synchronized ( this )
            {
                if ( index >= length() )
                {
                    NumberArray[] newChunks = Arrays.copyOf( chunks, chunkIndex( index )+1 );
                    for ( int i = chunks.length; i < newChunks.length; i++ )
                    {
                        newChunks[i] = addChunk( chunkSize );
                    }
                    chunks = newChunks;
                }
            }
        }
        return chunkAt( index );
    }

    protected abstract N addChunk( long chunkSize );
}
