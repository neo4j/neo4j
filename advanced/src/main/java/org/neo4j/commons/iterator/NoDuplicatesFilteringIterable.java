package org.neo4j.commons.iterator;

import java.util.HashSet;
import java.util.Set;

/**
 * Basic {@link FilteringIterable} which keeps of track of which items has
 * already been returned previously in the iteration and will just skip those
 * which've already been returned (equality is based on {@link #equals(Object)}/
 * {@link #hashCode()}.
 * 
 * @param <T> the type of items returned.
 */
public class NoDuplicatesFilteringIterable<T> extends FilteringIterable<T>
{
    private final Set<T> items = new HashSet<T>();
    
    public NoDuplicatesFilteringIterable( Iterable<T> source )
    {
        super( source );
    }

    @Override
    protected boolean passes( T item )
    {
        return items.add( item );
    }
}
