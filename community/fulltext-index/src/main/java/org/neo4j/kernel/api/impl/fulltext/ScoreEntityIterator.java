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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

/**
 * Iterator over entity ids together with their respective score.
 */
public class ScoreEntityIterator implements Iterator<ScoreEntityIterator.ScoreEntry>
{
    private final ValuesIterator iterator;
    private final Predicate<ScoreEntry> predicate;
    private ScoreEntry next;

    ScoreEntityIterator( ValuesIterator sortedValuesIterator )
    {
        this.iterator = sortedValuesIterator;
        this.predicate = null;
    }

    private ScoreEntityIterator( ValuesIterator sortedValuesIterator, Predicate<ScoreEntry> predicate )
    {
        this.iterator = sortedValuesIterator;
        this.predicate = predicate;
    }

    public Stream<ScoreEntry> stream()
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), false );
    }

    @Override
    public boolean hasNext()
    {
        while ( next == null && iterator.hasNext() )
        {
            long entityId = iterator.next();
            float score = iterator.currentScore();
            ScoreEntry tmp = new ScoreEntry( entityId, score );
            if ( predicate == null || predicate.test( tmp ) )
            {
                next = tmp;
            }
        }
        return next != null;
    }

    @Override
    public ScoreEntry next()
    {
        if ( hasNext() )
        {
            ScoreEntry tmp = next;
            next = null;
            return tmp;
        }
        else
        {
            throw new NoSuchElementException( "The iterator is exhausted" );
        }
    }

    ScoreEntityIterator filter( Predicate<ScoreEntry> predicate )
    {
        if ( this.predicate != null )
        {
            predicate = this.predicate.and( predicate );
        }
        return new ScoreEntityIterator( iterator, predicate );
    }

    /**
     * Merges the given iterators into a single iterator, that maintains the aggregate descending score sort order.
     *
     * @param iterators to concatenate
     * @return a {@link ScoreEntityIterator} that iterates over all of the elements in all of the given iterators
     */
    static ScoreEntityIterator mergeIterators( List<ScoreEntityIterator> iterators )
    {
        return new ConcatenatingScoreEntityIterator( iterators );
    }

    private static class ConcatenatingScoreEntityIterator extends ScoreEntityIterator
    {
        private final List<? extends ScoreEntityIterator> iterators;
        private final ScoreEntry[] buffer;
        private boolean fetched;
        private ScoreEntry nextHead;

        ConcatenatingScoreEntityIterator( List<? extends ScoreEntityIterator> iterators )
        {
            super( null );
            this.iterators = iterators;
            this.buffer = new ScoreEntry[iterators.size()];
        }

        @Override
        public boolean hasNext()
        {
            if ( !fetched )
            {
                fetch();
            }
            return nextHead != null;
        }

        private void fetch()
        {
            int candidateHead = -1;
            for ( int i = 0; i < iterators.size(); i++ )
            {
                ScoreEntry entry = buffer[i];
                //Fill buffer if needed.
                if ( entry == null && iterators.get( i ).hasNext() )
                {
                    entry = iterators.get( i ).next();
                    buffer[i] = entry;
                }

                //Check if entry might be candidate for next to return.
                if ( entry != null && (nextHead == null || entry.score > nextHead.score) )
                {
                    nextHead = entry;
                    candidateHead = i;
                }
            }
            if ( candidateHead != -1 )
            {
                buffer[candidateHead] = null;
            }
            fetched = true;
        }

        @Override
        public ScoreEntry next()
        {
            if ( hasNext() )
            {
                fetched = false;
                ScoreEntry best = nextHead;
                nextHead = null;
                return best;
            }
            else
            {
                throw new NoSuchElementException( "The iterator is exhausted" );
            }
        }
    }

    /**
     * A ScoreEntry consists of an entity id together with its score.
     */
    static class ScoreEntry
    {
        private final long entityId;
        private final float score;

        long entityId()
        {
            return entityId;
        }

        float score()
        {
            return score;
        }

        ScoreEntry( long entityId, float score )
        {
            this.entityId = entityId;
            this.score = score;
        }

        @Override
        public String toString()
        {
            return "ScoreEntry[entityId=" + entityId + ", score=" + score + "]";
        }
    }
}
