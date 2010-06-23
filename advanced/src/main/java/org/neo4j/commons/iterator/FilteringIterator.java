package org.neo4j.commons.iterator;

import java.util.Iterator;

import org.neo4j.commons.Predicate;

/**
 * An iterator which filters another iterator, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterator<T> extends PrefetchingIterator<T>
{
	private final Iterator<T> source;
	private final Predicate<T> predicate;
	
	public FilteringIterator( Iterator<T> source, Predicate<T> predicate )
	{
		this.source = source;
		this.predicate = predicate;
	}
	
	@Override
	protected T fetchNextOrNull()
	{
		while ( source.hasNext() )
		{
			T testItem = source.next();
			if ( predicate.accept( testItem ) )
			{
				return testItem;
			}
		}
		return null;
	}

    public static <T> Iterator<T> notNull( Iterator<T> source )
    {
        return new FilteringIterator<T>( source, FilteringIterable.<T>notNullPredicate() );
    }
    
    public static <T> Iterator<T> noDuplicates( Iterator<T> source )
    {
        return new FilteringIterator<T>( source, FilteringIterable.<T>noDuplicatesPredicate() );
    }
}
