package org.neo4j.commons.iterator;

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
    private boolean hasFetchedNext;
	private T nextObject;
	
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
