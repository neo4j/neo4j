package org.neo4j.commons.iterator;

import java.util.Iterator;

/**
 * Contains common functionality regarding {@link Iterator}s.
 */
public class IteratorUtil
{
    /**
     * Returns the given iterator's only value or {@code null} if there was no
     * items in the iterator. If there is more than one value in the iterator
     * an {@link IllegalArgumentException} will be thrown.
     * 
     * @param <T> the type of items in {@code iterator}.
     * @param iterator the {@link Iterator} to get items from.
     * @return the only value in the {@code iterator}, or {@code null} if no
     * value was found. Throws {@link IllegalArgumentException} if more than
     * one value was found.
     */
    public static <T> T singleValueOrNull( Iterator<T> iterator )
    {
        T value = iterator.hasNext() ? iterator.next() : null;
        if ( iterator.hasNext() )
        {
            throw new IllegalArgumentException( "More than one item in " +
                iterator + ". First value is '" + value +
                "' and the second value is '" + iterator.next() + "'" );
        }
        return value;
    }
}
