package org.neo4j.commons.iterator;

import java.util.HashSet;
import java.util.Iterator;
import java.util.Set;

/**
 * Basic {@link FilteringIterable} which keeps of track of which items has
 * already been returned previously in the iteration and will just skip those
 * which've already been returned (equality is based on {@link #equals(Object)}/
 * {@link #hashCode()}.
 *
 * @param <T> the type of items returned.
 */
public class NoDuplicatesFilteringIterable<T> implements Iterable<T>
{
    private final Iterable<T> source;

    public NoDuplicatesFilteringIterable( Iterable<T> source )
    {
        this.source = source;
    }

    public Iterator<T> iterator()
    {
        return new FilteringIterator<T>( source.iterator() )
        {
            final Set<T> items = new HashSet<T>();

            @Override
            protected boolean passes( T item )
            {
                return items.add( item );
            }
        };
    }
}
