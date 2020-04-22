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
package org.neo4j.internal.id.indexed;

import java.util.function.LongConsumer;

/**
 * A long[] that grows in chunks w/o array copy, as a linked list.
 *
 * <pre>
 * [ , , , , ] -> [ , , , , , , , , , ] -> [ , , , , , , , , , , , , , , , , , , , ] -> ...
 * </pre>
 */
class LinkedChunkLongArray
{
    private Chunk first;
    private Chunk current;
    private int size;

    LinkedChunkLongArray( int initialChunkSize )
    {
        first = new Chunk( initialChunkSize );
        current = first;
    }

    void add( long id )
    {
        if ( !current.add( id ) )
        {
            Chunk newChunk = new Chunk( current.ids.length * 2 );
            current.next = newChunk;
            current = newChunk;
            current.add( id );
        }
        size++;
    }

    int size()
    {
        return size;
    }

    void accept( LongConsumer visitor )
    {
        Chunk chunk = first;
        while ( chunk != null )
        {
            for ( int i = 0; i < chunk.cursor; i++ )
            {
                visitor.accept( chunk.ids[i] );
            }
            chunk = chunk.next;
        }
    }

    private static class Chunk
    {
        private final long[] ids;
        private int cursor;
        private Chunk next;

        Chunk( int size )
        {
            this.ids = new long[size];
        }

        boolean add( long id )
        {
            if ( cursor < ids.length )
            {
                ids[cursor++] = id;
                return true;
            }
            return false;
        }
    }
}
