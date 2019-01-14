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
package org.neo4j.kernel.impl.index.labelscan;

import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongResourceIterator;
import org.neo4j.graphdb.ResourceUtils;

/**
 * {@link PrimitiveLongIterator} acting as a combining of multiple {@link PrimitiveLongIterator}
 * for merging their results lazily as iterating commences. Both {@code AND} and {@code OR} merging is supported.
 * <p>
 * Source iterators must be sorted in ascending order.
 */
class CompositeLabelScanValueIterator extends PrimitiveLongBaseIterator implements PrimitiveLongResourceIterator
{
    private final PriorityQueue<IdAndSource> sortedIterators = new PriorityQueue<>();
    private final int atLeastNumberOfLabels;
    private final List<PrimitiveLongResourceIterator> toClose;
    private long last = -1;

    /**
     * Constructs a {@link CompositeLabelScanValueIterator}.
     *
     * @param iterators {@link PrimitiveLongIterator iterators} to merge.
     * @param trueForAll if {@code true} using {@code AND} merging, otherwise {@code OR} merging.
     */
    CompositeLabelScanValueIterator( List<PrimitiveLongResourceIterator> iterators, boolean trueForAll )
    {
        this.toClose = iterators;
        this.atLeastNumberOfLabels = trueForAll ? iterators.size() : 1;
        for ( PrimitiveLongIterator iterator : iterators )
        {
            if ( iterator.hasNext() )
            {
                sortedIterators.add( new IdAndSource( iterator.next(), iterator ) );
            }
        }
    }

    @Override
    protected boolean fetchNext()
    {
        int numberOfLabels = 0;
        long next = last;
        while ( next == last || numberOfLabels < atLeastNumberOfLabels )
        {
            IdAndSource idAndSource = sortedIterators.poll();
            if ( idAndSource == null )
            {
                return false;
            }

            if ( idAndSource.latestReturned == next )
            {
                numberOfLabels++;
            }
            else
            {
                next = idAndSource.latestReturned;
                numberOfLabels = 1;
            }

            if ( idAndSource.source.hasNext() )
            {
                idAndSource.latestReturned = idAndSource.source.next();
                sortedIterators.offer( idAndSource );
            }
        }
        last = next;
        next( last );
        return true;
    }

    @Override
    public void close()
    {
        ResourceUtils.closeAll( toClose );
        sortedIterators.clear();
        toClose.clear();
    }

    private static class IdAndSource implements Comparable<IdAndSource>
    {
        private long latestReturned;
        private final PrimitiveLongIterator source;

        private IdAndSource( long latestReturned, PrimitiveLongIterator source )
        {
            this.latestReturned = latestReturned;
            this.source = source;
        }

        @Override
        public int compareTo( IdAndSource o )
        {
            int keyComparison = Long.compare( latestReturned, o.latestReturned );
            if ( keyComparison == 0 )
            {
                return Integer.compare( source.hashCode(), o.source.hashCode() );
            }
            return keyComparison;
        }

        @Override
        public boolean equals( Object o )
        {
            if ( this == o )
            {
                return true;
            }
            if ( o == null || getClass() != o.getClass() )
            {
                return false;
            }
            IdAndSource that = (IdAndSource) o;
            return compareTo( that ) == 0;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash( latestReturned, source );
        }
    }
}
