/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.core.consensus.log.segmented;

import java.util.Arrays;

import static java.lang.Math.max;
import static java.lang.String.format;

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
            throw new IllegalStateException( format( "Must append in order. %s but expected index is %d",
                    appendMessage( index, term ), max + 1 ) );
        }
        else if ( size > 0 && term < terms[size - 1] )
        {
            throw new IllegalStateException( format( "Non-monotonic term. %s but highest term is %d",
                    appendMessage( index, term ), terms[size - 1] ) );
        }

        max = index;

        if ( size == 0 || term != terms[size - 1] )
        {
            setSize( size + 1 );
            indexes[size - 1] = index;
            terms[size - 1] = term;
        }
    }

    private String appendMessage( long index, long term )
    {
        return format( "Tried to append [index: %d, term: %d]", index, term );
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
        if ( fromIndex < 0 || fromIndex < min )
        {
            throw new IllegalStateException( "Cannot truncate a negative index. Tried to truncate from " + fromIndex );
        }

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
     * @param upToIndex The last index to prune (exclusive).
     */
    synchronized void prune( long upToIndex )
    {
        min = max( upToIndex, min );

        int lastToPrune = findRangeContaining( min ) - 1; // we can prune the ranges preceding

        if ( lastToPrune < 0 )
        {
            return;
        }

        size = (indexes.length - 1) - lastToPrune;
        indexes = Arrays.copyOfRange( indexes, lastToPrune + 1, indexes.length );
        terms = Arrays.copyOfRange( terms, lastToPrune + 1, terms.length );
    }

    private int findRangeContaining( long index )
    {
        for ( int i = 0; i < indexes.length; i++ )
        {
            if ( indexes[i] > index )
            {
                return i - 1;
            }
            else if ( indexes[i] == index )
            {
                return i;
            }
        }

        return index > indexes[indexes.length - 1] ? indexes.length - 1 : -1;
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

    synchronized long latest()
    {
        return size == 0 ? -1 : terms[size - 1];
    }
}
