/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.util.Iterator;
import java.util.NoSuchElementException;

/**
 * Abstract class for how you usually implement iterators when you don't know
 * how many objects there are (which is pretty much every time)
 *
 * Basically the {@link #hasNext()} method will look up the next object and
 * cache it with {@link #setPrefetchedNext(Object)}. The cached object is
 * then set to {@code null} in {@link #next()}. So you only have to implement
 * one method, {@code fetchNextOrNull} which returns {@code null} when the
 * iteration has reached the end, and you're done.
 */
public abstract class PrefetchingIterator<T> implements Iterator<T>
{
    boolean hasFetchedNext;
    T nextObject;

	/**
	 * Tries to fetch the next item and caches it so that consecutive calls
	 * (w/o an intermediate call to {@link #next()} will remember it and won't
	 * try to fetch it again.
	 *
	 * @return {@code true} if there was a next item to return in the next
	 * call to {@link #next()}.
	 */
	public boolean hasNext()
	{
		if ( hasFetchedNext )
		{
		    return getPrefetchedNextOrNull() != null;
		}

		T nextOrNull = fetchNextOrNull();
        hasFetchedNext = true;
		if ( nextOrNull != null )
		{
			setPrefetchedNext( nextOrNull );
		}
		return nextOrNull != null;
	}

	/**
	 * Uses {@link #hasNext()} to try to fetch the next item and returns it
	 * if found, otherwise it throws a {@link NoSuchElementException}.
	 *
	 * @return the next item in the iteration, or throws
	 * {@link NoSuchElementException} if there's no more items to return.
	 */
	public T next()
	{
		if ( !hasNext() )
		{
			throw new NoSuchElementException();
		}
		T result = getPrefetchedNextOrNull();
		setPrefetchedNext( null );
		hasFetchedNext = false;
		return result;
	}

	protected abstract T fetchNextOrNull();

	protected void setPrefetchedNext( T nextOrNull )
	{
		this.nextObject = nextOrNull;
	}

	protected T getPrefetchedNextOrNull()
	{
		return nextObject;
	}

	public void remove()
	{
		throw new UnsupportedOperationException();
	}
}
