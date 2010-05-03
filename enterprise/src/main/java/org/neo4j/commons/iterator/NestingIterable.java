package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * For each item in the supplied iterator (called "surface item") there's
 * instantiated an iterator from that item which is iterated before moving
 * on to the next surface item.
 *
 * @param <T> the type of items.
 * @param <U> the type of items in the surface item iterator
 */
public abstract class NestingIterable<T, U> implements Iterable<T>
{
	private Iterable<U> source;
	
	public NestingIterable( Iterable<U> source )
	{
		this.source = source;
	}
	
	public Iterator<T> iterator()
	{
		return new NestingIterator<T, U>( source.iterator() )
		{
			@Override
			protected Iterator<T> createNestedIterator( U item )
			{
				return NestingIterable.this.createNestedIterator( item );
			}
		};
	}
	
	protected abstract Iterator<T> createNestedIterator( U item );
}
