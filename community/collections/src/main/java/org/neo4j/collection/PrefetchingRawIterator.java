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
package org.neo4j.collection;

import java.util.NoSuchElementException;

public abstract class PrefetchingRawIterator<T, EXCEPTION extends Exception> implements RawIterator<T, EXCEPTION>
{
    boolean hasFetchedNext;
    T nextObject;

    /**
     * @return {@code true} if there is a next item to be returned from the next
     * call to {@link #next()}.
     */
    @Override
    public boolean hasNext() throws EXCEPTION
    {
        return peek() != null;
    }

    /**
     * @return the next element that will be returned from {@link #next()} without
     * actually advancing the iterator
     */
    public T peek() throws EXCEPTION
    {
        if ( hasFetchedNext )
        {
            return nextObject;
        }

        nextObject = fetchNextOrNull();
        hasFetchedNext = true;
        return nextObject;
    }

    /**
     * Uses {@link #hasNext()} to try to fetch the next item and returns it
     * if found, otherwise it throws a {@link java.util.NoSuchElementException}.
     *
     * @return the next item in the iteration, or throws
     * {@link java.util.NoSuchElementException} if there's no more items to return.
     */
    @Override
    public T next() throws EXCEPTION
    {
        if ( !hasNext() )
        {
            throw new NoSuchElementException();
        }
        T result = nextObject;
        nextObject = null;
        hasFetchedNext = false;
        return result;
    }

    protected abstract T fetchNextOrNull() throws EXCEPTION;

    @Override
    public void remove()
    {
        throw new UnsupportedOperationException();
    }
}
