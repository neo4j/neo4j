package org.neo4j.kernel.impl.index.labelscan;

import java.util.List;
import java.util.Objects;
import java.util.PriorityQueue;

import org.neo4j.collection.primitive.PrimitiveLongCollections.PrimitiveLongBaseIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;

class CompositeLabelScanValueIterator extends PrimitiveLongBaseIterator
{
    private final PriorityQueue<IdAndSource> sortedIterators = new PriorityQueue<>();
    private int atLeastNumberOfLabels;
    private long last = -1;

    CompositeLabelScanValueIterator( List<PrimitiveLongIterator> iterators, boolean trueForAll )
    {
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

    private class IdAndSource implements Comparable<IdAndSource>
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
        @SuppressWarnings( "unchecked" )
        public boolean equals( Object o )
        {
            if ( this == o )
            { return true; }
            if ( o == null || getClass() != o.getClass() )
            { return false; }
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
