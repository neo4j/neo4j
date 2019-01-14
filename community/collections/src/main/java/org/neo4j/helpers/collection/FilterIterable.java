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
package org.neo4j.helpers.collection;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.function.Predicate;

class FilterIterable<T> implements Iterable<T>
{
    private final Iterable<T> iterable;

    private final Predicate<? super T> specification;

    FilterIterable( Iterable<T> iterable, Predicate<? super T> specification )
    {
        this.iterable = iterable;
        this.specification = specification;
    }

    @Override
    public Iterator<T> iterator()
    {
        return new FilterIterator<>( iterable.iterator(), specification );
    }

    static class FilterIterator<T> implements Iterator<T>
    {
        private final Iterator<T> iterator;

        private final Predicate<? super T> specification;

        private T currentValue;
        boolean finished;
        boolean nextConsumed = true;

        FilterIterator( Iterator<T> iterator, Predicate<? super T> specification )
        {
            this.specification = specification;
            this.iterator = iterator;
        }

        boolean moveToNextValid()
        {
            boolean found = false;
            while ( !found && iterator.hasNext() )
            {
                T currentValue = iterator.next();
                boolean satisfies = specification.test( currentValue );

                if ( satisfies )
                {
                    found = true;
                    this.currentValue = currentValue;
                    nextConsumed = false;
                }
            }
            if ( !found )
            {
                finished = true;
            }
            return found;
        }

        @Override
        public T next()
        {
            if ( !nextConsumed )
            {
                nextConsumed = true;
                return currentValue;
            }
            else
            {
                if ( !finished )
                {
                    if ( moveToNextValid() )
                    {
                        nextConsumed = true;
                        return currentValue;
                    }
                }
            }
            throw new NoSuchElementException( "This iterator is exhausted." );
        }

        @Override
        public boolean hasNext()
        {
            return !finished && (!nextConsumed || moveToNextValid());
        }

        @Override
        public void remove()
        {
        }
    }
}
