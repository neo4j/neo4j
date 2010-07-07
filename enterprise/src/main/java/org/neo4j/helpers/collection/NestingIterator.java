package org.neo4j.helpers.collection;

import java.util.Iterator;

/**
 * For each item in the supplied iterator (called "surface item") there's
 * instantiated an iterator from that item which is iterated before moving
 * on to the next surface item.
 *
 * @param <T> the type of items to return
 * @param <U> the type of items in the surface item iterator
 */
public abstract class NestingIterator<T, U> extends PrefetchingIterator<T>
{
	private Iterator<U> source;
	private Iterator<T> currentNestedIterator;
	private U currentSurfaceItem;
	
	public NestingIterator( Iterator<U> source )
	{
		this.source = source;
	}
	
	protected abstract Iterator<T> createNestedIterator( U item );
	
	public U getCurrentSurfaceItem()
	{
		if ( this.currentSurfaceItem == null )
		{
			throw new IllegalStateException( "Has no surface item right now," +
				" you must do at least one next() first" );
		}
		return this.currentSurfaceItem;
	}
	
	@Override
	protected T fetchNextOrNull()
	{
		if ( currentNestedIterator == null ||
			!currentNestedIterator.hasNext() )
		{
			while ( source.hasNext() )
			{
				currentSurfaceItem = source.next();
				currentNestedIterator =
					createNestedIterator( currentSurfaceItem );
				if ( currentNestedIterator.hasNext() )
				{
					break;
				}
			}
		}
		return currentNestedIterator != null &&
			currentNestedIterator.hasNext() ?
			currentNestedIterator.next() : null;
	}
}
