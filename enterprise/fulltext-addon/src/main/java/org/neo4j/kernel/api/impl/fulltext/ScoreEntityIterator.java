/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.api.impl.fulltext;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.helpers.collection.Pair;
import org.neo4j.kernel.api.impl.index.collector.ValuesIterator;

public class ScoreEntityIterator implements Iterator<Pair<Long,Float>>
{
    private static final ScoreEntityIterator EMPTY = new ScoreEntityIterator()
    {
        @Override
        public Stream<Pair<Long,Float>> stream()
        {
            return Stream.empty();
        }

        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Pair<Long,Float> next()
        {
            throw new NoSuchElementException( "The iterator is exhausted" );
        }
    };
    private final ValuesIterator iterator;

    ScoreEntityIterator( ValuesIterator sortedValuesIterator )
    {
        this.iterator = sortedValuesIterator;
    }

    private ScoreEntityIterator()
    {
        this.iterator = null;
    }

    public Stream<Pair<Long,Float>> stream()
    {
        return StreamSupport.stream( Spliterators.spliteratorUnknownSize( this, Spliterator.ORDERED ), false );
    }

    @Override
    public boolean hasNext()
    {
        return iterator.hasNext();
    }

    @Override
    public Pair<Long,Float> next()
    {
        if ( hasNext() )
        {
            return Pair.of( currentIterator().next(), currentIterator().currentScore() );
        }
        else
        {
            throw new NoSuchElementException( "The iterator is exhausted" );
        }
    }

    ValuesIterator currentIterator()
    {
        return iterator;
    }

    public static ScoreEntityIterator concat( List<ScoreEntityIterator> iterators )
    {
        return new ConcatenatingScoreEntityIterator( iterators.iterator() );
    }

    public static ScoreEntityIterator emptyIterator()
    {
        return EMPTY;
    }

    private static class ConcatenatingScoreEntityIterator extends ScoreEntityIterator
    {
        private final Iterator<? extends ScoreEntityIterator> iterators;
        private ScoreEntityIterator currentIterator;

        ConcatenatingScoreEntityIterator( Iterator<? extends ScoreEntityIterator> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        public boolean hasNext()
        {
            if ( currentIterator == null || !currentIterator.hasNext() )
            {
                while ( iterators.hasNext() )
                {
                    currentIterator = iterators.next();
                    if ( currentIterator.hasNext() )
                    {
                        break;
                    }
                }
            }
            return currentIterator != null && currentIterator.hasNext();
        }

        @Override
        final ValuesIterator currentIterator()
        {
            return currentIterator.iterator;
        }
    }
}
