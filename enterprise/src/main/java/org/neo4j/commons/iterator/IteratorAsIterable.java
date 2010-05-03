package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * Wraps an {@link Iterator}, making it look like an {@link Iterable}.
 * Important: It breaks the contract of the {@link Iterable} by returning the
 * same {@link Iterator} instance for every call to {@link #iterator()}.
 * 
 * This class exists ONLY because the java for-each loop won't accept an
 * {@link Iterator}.
 * @param <T> the type of items in the iterator.
 */
public class IteratorAsIterable<T> implements Iterable<T>
{
    private final Iterator<T> iterator;
    
    public IteratorAsIterable( Iterator<T> iterator )
    {
        this.iterator = iterator;
    }
    
    public Iterator<T> iterator()
    {
        return this.iterator;
    }
}
