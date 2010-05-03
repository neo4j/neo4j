package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public abstract class FilteringIterable<T> implements Iterable<T>
{
	private Iterable<T> source;
	
	public FilteringIterable( Iterable<T> source )
	{
		this.source = source;
	}
	
	public Iterator<T> iterator()
	{
		return new FilteringIterator<T>( source.iterator() )
		{
			@Override
			protected boolean passes( T item )
			{
				return FilteringIterable.this.passes( item );
			}
		};
	}
	
	protected abstract boolean passes( T item );
}
