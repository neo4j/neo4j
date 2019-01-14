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
package org.neo4j.kernel.impl.store.id;

import org.neo4j.kernel.impl.store.id.validation.IdValidator;

import static org.neo4j.collection.primitive.PrimitiveLongCollections.EMPTY_LONG_ARRAY;

/**
 * {@link IdSequence} w/o any synchronization, purely a long incrementing.
 */
public class BatchingIdSequence implements IdSequence
{
    private final long startId;
    private long nextId;

    public BatchingIdSequence()
    {
        this( 0 );
    }

    public BatchingIdSequence( long startId )
    {
        this.startId = startId;
        this.nextId = startId;
    }

    @Override
    public long nextId()
    {
        long result = peek();
        nextId++;
        return result;
    }

    @Override
    public IdRange nextIdBatch( int size )
    {
        while ( IdValidator.hasReservedIdInRange( nextId, nextId + size ) )
        {
            nextId += size;
        }

        long startId = nextId;
        nextId += size;
        return new IdRange( EMPTY_LONG_ARRAY, startId, size );
    }

    public void reset()
    {
        nextId = startId;
    }

    public void set( long nextId )
    {
        this.nextId = nextId;
    }

    public long peek()
    {
        if ( IdValidator.isReservedId( nextId ) )
        {
            nextId++;
        }
        return nextId;
    }
}
