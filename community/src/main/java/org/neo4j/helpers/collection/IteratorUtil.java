package org.neo4j.helpers.collection;

import java.util.Collection;
import java.util.Iterator;

/**
 * Contains common functionality regarding {@link Iterator}s.
 */
public abstract class IteratorUtil
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
    
    /**
     * Adds all the items in {@code iterator} to {@code collection}.
     * @param <C> the type of {@link Collection} to add to items to.
     * @param <T> the type of items in the collection and iterator.
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<T>,T> C addToCollection( Iterator<T> iterator,
            C collection )
    {
        while ( iterator.hasNext() )
        {
            collection.add( iterator.next() );
        }
        return collection;
    }

    /**
     * Adds all the items in {@code iterator} to {@code collection}.
     * @param <C> the type of {@link Collection} to add to items to.
     * @param <T> the type of items in the collection and iterator.
     * @param iterable the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<T>,T> C addToCollection( Iterable<T> iterable,
            C collection )
    {
        return addToCollection(iterable.iterator(), collection);
    }

    /**
     * Exposes {@code iterator} as an {@link Iterable}. It breaks the contract
     * of {@link Iterable} in that it returns the supplied iterator instance for
     * each call to {@code iterator()} on the returned {@link Iterable}
     * instance. This method mostly exists to make it easy to use an
     * {@link Iterator} in a for-loop.
     * 
     * @param <T> the type of items in the iterator.
     * @param iterator the iterator to expose as an {@link Iterable}.
     * @return the supplied iterator posing as an {@link Iterable}.
     */
    public static <T> Iterable<T> asIterable( final Iterator<T> iterator )
    {
        return new Iterable<T>()
        {
            public Iterator<T> iterator()
            {
                return iterator;
            }
        };
    }
    
    /**
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterator the {@link Iterator} to count items in.
     * @return the number of found in {@code iterator}.
     */
    public static <T> int count( Iterator<T> iterator )
    {
        int result = 0;
        while ( iterator.hasNext() )
        {
            iterator.next();
            result++;
        }
        return result;
    }

    /**
     * Counts the number of items in the {@code iterable} by looping through it.
     * 
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of found in {@code iterator}.
     */
    public static <T> int count( Iterable<T> iterable )
    {
        return count( iterable.iterator() );
    }
}
