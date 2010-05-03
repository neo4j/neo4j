package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * An iterator which filters another iterator, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public abstract class FilteringIterator<T> extends PrefetchingIterator<T>
{
	private Iterator<T> source;
	
	public FilteringIterator( Iterator<T> source )
	{
		this.source = source;
	}
	
	@Override
	protected T fetchNextOrNull()
	{
		while ( source.hasNext() )
		{
			T testItem = source.next();
			if ( passes( testItem ) )
			{
				return testItem;
			}
		}
		return null;
	}
	
	protected abstract boolean passes( T item );
}
