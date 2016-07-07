/*
 * Copyright (c) 2002-2016 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.coreedge.raft.log.segmented;

import java.util.Arrays;

/**
 * Keeps track of all the terms in memory for efficient lookup.
 * The implementation favours lookup of recent entries.
 *
 * Exposed methods shadow the regular RAFT log manipulation
 * functions and must be invoked from respective places.
 *
 * During recovery truncate should be called between every segment
 * switch to "simulate" eventual truncations as a reason for switching
 * segments. It is ok to call truncate even if the reason was not a
 * truncation.
 */
public class Terms
{
    private int size;

    private long[] indexes;
    private long[] terms;

    private long min; // inclusive
    private long max; // inclusive

    Terms( long prevIndex, long prevTerm )
    {
        skip( prevIndex, prevTerm );
    }

    synchronized void append( long index, long term )
    {
        if ( index != max + 1 )
        {
            throw new IllegalStateException( "Must append in order" );
        }
        else if ( term < terms[size - 1] )
        {
            throw new IllegalStateException( "Non-monotonic term" );
        }

        max = index;

        if ( term != terms[size - 1] )
        {
            setSize( size + 1 );
            indexes[size - 1] = index;
            terms[size - 1] = term;
        }
    }

    private void setSize( int newSize )
    {
        if ( newSize != size )
        {
            size = newSize;
            indexes = Arrays.copyOf( indexes, size );
            terms = Arrays.copyOf( terms, size );
        }
    }

    /**
     * Truncate from the specified index.
     *
     * @param fromIndex The index to truncate from (inclusive).
     */
    synchronized void truncate( long fromIndex )
    {
        max = fromIndex - 1;

        int newSize = size;
        while ( newSize > 0 && indexes[newSize - 1] >= fromIndex )
        {
            newSize--;
        }

        setSize( newSize );
    }

    /**
     * Prune up to specified index.
     *
     * @param upToIndex The last index to prune (inclusive).
     */
    synchronized void prune( long upToIndex )
    {
        min = upToIndex + 1;
        // could also prune out array
    }

    synchronized void skip( long prevIndex, long prevTerm )
    {
        min = max = prevIndex;
        size = 1;
        indexes = new long[size];
        terms = new long[size];
        indexes[0] = prevIndex;
        terms[0] = prevTerm;
    }

    synchronized long get( long logIndex )
    {
        if ( logIndex == -1 || logIndex < min || logIndex > max )
        {
            return -1;
        }

        for ( int i = size - 1; i >= 0; i-- )
        {
            if ( logIndex >= indexes[i] )
            {
                return terms[i];
            }
        }

        throw new RuntimeException( "Should be possible to find index >= min" );
    }
}
