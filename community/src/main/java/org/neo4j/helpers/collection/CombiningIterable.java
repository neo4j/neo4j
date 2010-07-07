package org.neo4j.helpers.collection;

import java.util.Iterator;
import java.util.LinkedList;

/**
 * Combining one or more {@link Iterable}s, making them look like they were
 * one big iterable. All iteration/combining is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public class CombiningIterable<T> implements Iterable<T>
{
	private Iterable<Iterable<T>> iterables;
	
	public CombiningIterable( Iterable<Iterable<T>> iterables )
	{
		this.iterables = iterables;
	}
	
	public Iterator<T> iterator()
	{
		LinkedList<Iterator<T>> iterators = new LinkedList<Iterator<T>>();
		for ( Iterable<T> iterable : iterables )
		{
			iterators.add( iterable.iterator() );
		}
		return new CombiningIterator<T>( iterators );
	}
}
