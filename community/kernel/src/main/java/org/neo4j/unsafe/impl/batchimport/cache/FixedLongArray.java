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

class FixedLongArray extends FixedNumberArray<LongArray> implements LongArray
{
    private final long defaultValue;

    FixedLongArray( NumberArray[] chunks, long chunkSize, long defaultValue )
    {
        super( chunks, chunkSize );
        this.defaultValue = defaultValue;
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
    public long get( long index )
    {
        LongArray chunk = chunkOrNullAt( index );
        return chunk != null ? chunk.get( index( index ) ) : defaultValue;
    }

    @Override
    public void set( long index, long value )
    {
        chunkAt( index ).set( index( index ), value );
    }

    @Override
    public LongArray fixate()
    {
        return this;
    }
}
