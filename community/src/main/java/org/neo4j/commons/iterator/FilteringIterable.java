package org.neo4j.commons.iterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

import org.neo4j.commons.Predicate;

/**
 * An iterable which filters another iterable, only letting items with certain
 * criterias pass through. All iteration/filtering is done lazily.
 *
 * @param <T> the type of items in the iteration.
 */
public class FilteringIterable<T> implements Iterable<T>
{
	private final Iterable<T> source;
	private final Predicate<T> predicate;

	public FilteringIterable( Iterable<T> source, Predicate<T> predicate )
	{
		this.source = source;
		this.predicate = predicate;
	}

	public Iterator<T> iterator()
	{
		return new FilteringIterator<T>( source.iterator(), predicate );
	}

    public static <T> Iterable<T> notNull( Iterable<T> source )
    {
        return new FilteringIterable<T>( source, FilteringIterable.<T>notNullPredicate() );
    }
    
    public static <T> Iterable<T> noDuplicates( Iterable<T> source )
    {
        return new FilteringIterable<T>( source, FilteringIterable.<T>noDuplicatesPredicate() );
    }
    
    public static <T> Predicate<T> noDuplicatesPredicate()
    {
        return new Predicate<T>()
        {
            private final Set<T> visitedItems = new HashSet<T>();
            
            public boolean accept( T item )
            {
                return visitedItems.add( item );
            }
        };
    }

    @SuppressWarnings("unchecked")
    public static <T> Predicate<T> notNullPredicate()
    {
        return (Predicate<T>) NOT_NULL_PREDICATE;
    }
    
    @SuppressWarnings("unchecked")
    private static final Predicate NOT_NULL_PREDICATE = new Predicate()
    {
        public boolean accept( Object item )
        {
            return item != null;
        }
    };
}
