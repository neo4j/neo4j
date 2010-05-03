package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * Wraps an {@link Iterable} so that it returns items of another type. The
 * iteration is done lazily.
 *
 * @param <T> the type of items to return
 * @param <U> the type of items to wrap/convert from
 */
public abstract class IterableWrapper<T, U> implements Iterable<T>
{
	private Iterable<U> source;
	
	public IterableWrapper( Iterable<U> iterableToWrap )
	{
		this.source = iterableToWrap;
	}
	
	protected abstract T underlyingObjectToObject( U object );
	
	public Iterator<T> iterator()
	{
		return new MyIteratorWrapper( source.iterator() );
	}
	
	private class MyIteratorWrapper extends IteratorWrapper<T, U>
	{
		public MyIteratorWrapper( Iterator<U> iteratorToWrap )
		{
			super( iteratorToWrap );
		}

		@Override
		protected T underlyingObjectToObject( U object )
		{
			return IterableWrapper.this.underlyingObjectToObject( object );
		}
	}
}
