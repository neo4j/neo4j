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
package org.neo4j.helpers.collection;

import java.util.Collections;
import java.util.Iterator;

/**
 * Combining one or more {@link Iterator}s, making them look like they were
 * one big iterator. All iteration/combining is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class CombiningIterator<T> extends PrefetchingIterator<T>
{
    private Iterator<? extends Iterator<T>> iterators;
    private Iterator<T> currentIterator;

    public CombiningIterator( Iterable<? extends Iterator<T>> iterators )
    {
        this( iterators.iterator() );
    }
    
   public CombiningIterator( Iterator<? extends Iterator<T>> iterators )
   {
        this.iterators = iterators;
   }

    public CombiningIterator( T first, Iterator<T> rest )
    {
        this( Collections.<Iterator<T>>emptyList() );
        this.hasFetchedNext = true;
        this.nextObject = first;
        this.currentIterator = rest;
    }

    @Override
    protected T fetchNextOrNull()
    {
        if ( currentIterator == null || !currentIterator.hasNext() )
        {
            while ( (currentIterator = nextIteratorOrNull()) != null )
            {
                if ( currentIterator.hasNext() )
                {
                    break;
                }
            }
        }
        return currentIterator != null && currentIterator.hasNext() ? currentIterator.next() : null;
    }

    protected Iterator<T> nextIteratorOrNull()
    {
        if(iterators.hasNext())
        {
            return iterators.next();
        }
        return null;
    }

    protected Iterator<T> currentIterator()
    {
        return currentIterator;
    }
}
