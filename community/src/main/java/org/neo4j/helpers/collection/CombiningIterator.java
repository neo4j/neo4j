package org.neo4j.helpers.collection;

import java.util.Iterator;

/**
 * Combining one or more {@link Iterator}s, making them look like they were
 * one big iterator. All iteration/combining is done lazily.
 * 
 * @param <T> the type of items in the iteration.
 */
public class CombiningIterator<T> extends PrefetchingIterator<T>
{
    private Iterator<Iterator<T>> iterators;
    private Iterator<T> currentIterator;
    
    public CombiningIterator( Iterable<Iterator<T>> iterators )
    {
        this.iterators = iterators.iterator();
    }

    @Override
    protected T fetchNextOrNull()
    {
        if ( currentIterator == null || !currentIterator.hasNext() )
        {
            while ( iterators.hasNext() )
            {
                currentIterator = iterators.next();
                if ( currentIterator.hasNext() )
                {
                    break;
                }
            }
        }
        return currentIterator != null && currentIterator.hasNext() ?
            currentIterator.next() : null;
    }
}
