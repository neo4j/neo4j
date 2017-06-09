/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.helpers.collection;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.neo4j.collection.RawIterator;
import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingFunction;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import static java.util.Collections.emptyIterator;

/**
 * Contains common functionality regarding {@link Iterator}s and
 * {@link Iterable}s.
 */
public abstract class Iterators
{
    private static final ResourceIterator EMPTY_RESOURCE_ITERATOR =
            resourceIterator( emptyIterator(), Resource.EMPTY );

    /**
     * Returns the given iterator's first element or {@code null} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull( Iterator<T> iterator )
    {
        return iterator.hasNext() ? iterator.next() : null;
    }

    /**
     * Returns the given iterator's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first( Iterator<T> iterator )
    {
        return assertNotNull( iterator, firstOrNull( iterator ) );
    }

    /**
     * Returns the given iterator's last element or {@code null} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the last element in the {@code iterator}, or {@code null} if no
     * element found.
     */
    public static <T> T lastOrNull( Iterator<T> iterator )
    {
        T result = null;
        while ( iterator.hasNext() )
        {
            result = iterator.next();
        }
        return result;
    }

    /**
     * Returns the given iterator's last element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the last element in the {@code iterator}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T last( Iterator<T> iterator )
    {
        return assertNotNull( iterator, lastOrNull( iterator ) );
    }

    /**
     * Returns the given iterator's single element or {@code null} if no
     * element found. If there is more than one element in the iterator a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the single item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in {@code iterator}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T singleOrNull( Iterator<T> iterator )
    {
        return single( iterator, null );
    }

    /**
     * Returns the given iterator's single element. If there are no elements
     * or more than one element in the iterator a {@link NoSuchElementException}
     * will be thrown.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the single item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in the {@code iterator}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single( Iterator<T> iterator )
    {
        return assertNotNull( iterator, singleOrNull( iterator ) );
    }

    /**
     * Returns the iterator's n:th item from the end of the iteration.
     * If the iterator has got less than n-1 items in it
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @param n the n:th item from the end to get.
     * @return the iterator's n:th item from the end of the iteration.
     * @throws NoSuchElementException if the iterator contains less than n-1 items.
     */
    public static <T> T fromEnd( Iterator<T> iterator, int n )
    {
        return assertNotNull( iterator, fromEndOrNull( iterator, n ) );
    }

    /**
     * Returns the iterator's n:th item from the end of the iteration.
     * If the iterator has got less than n-1 items in it {@code null} is returned.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @param n the n:th item from the end to get.
     * @return the iterator's n:th item from the end of the iteration,
     * or {@code null} if the iterator doesn't contain that many items.
     */
    public static <T> T fromEndOrNull( Iterator<T> iterator, int n )
    {
        Deque<T> trail = new ArrayDeque<>( n );
        while ( iterator.hasNext() )
        {
            if ( trail.size() > n )
            {
                trail.removeLast();
            }
            trail.addFirst( iterator.next() );
        }
        return trail.size() == n + 1 ? trail.getLast() : null;
    }

    /**
     * Iterates over the full iterators, and checks equality for each item in them. Note that this
     * will consume the iterators.
     *
     * @param first the first iterator
     * @param other the other iterator
     * @return {@code true} if all items are equal; otherwise
     */
    public static boolean iteratorsEqual( Iterator<?> first, Iterator<?> other )
    {
        while ( true )
        {
            if ( first.hasNext() && other.hasNext() )
            {
                if ( !first.next().equals( other.next() ) )
                {
                    return false;
                }
            }
            else
            {
                return first.hasNext() == other.hasNext();
            }
        }
    }

    public static <T> Iterator<T> consumableIterator( Iterator<T> iterator, Consumer<T> consumer )
    {
        return new IteratorWrapper<T,T>( iterator )
        {
            @Override
            protected T underlyingObjectToObject( T object )
            {
                consumer.accept( object );
                return object;
            }
        };
    }

    private static <T> T assertNotNull( Iterator<T> iterator, T result )
    {
        if ( result == null )
        {
            throw new NoSuchElementException( "No element found in " + iterator );
        }
        return result;
    }

    /**
     * Returns the given iterator's single element or {@code itemIfNone} if no
     * element found. If there is more than one element in the iterator a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@code iterator} implements {@link Resource} it will be {@link Resource#close() closed}
     * in a {@code finally} block after the single item has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @param itemIfNone item to use if none is found
     * @return the single element in {@code iterator}, or {@code itemIfNone} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T single( Iterator<T> iterator, T itemIfNone )
    {
        try
        {
            T result = iterator.hasNext() ? iterator.next() : itemIfNone;
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one element in " + iterator + ". First element is '"
                        + result + "' and the second element is '" + iterator.next() + "'" );
            }
            return result;
        }
        finally
        {
            if ( iterator instanceof Resource )
            {
                ((Resource) iterator).close();
            }
        }
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
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<T>,T> C addToCollectionUnique( Iterator<T> iterator,
            C collection )
    {
        while ( iterator.hasNext() )
        {
            addUnique( collection, iterator.next() );
        }
        return collection;
    }

    private static <T, C extends Collection<T>> void addUnique( C collection, T item )
    {
        if ( !collection.add( item ) )
        {
            throw new IllegalStateException( "Encountered an already added item:" + item +
                    " when adding items uniquely to a collection:" + collection );
        }
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
    public static <C extends Collection<T>,T> C addToCollectionUnique( Iterable<T> iterable,
            C collection )
    {
        return addToCollectionUnique( iterable.iterator(), collection );
    }

    /**
     * Convenience method for looping over an {@link Iterator}. Converts the
     * {@link Iterator} to an {@link Iterable} by wrapping it in an
     * {@link Iterable} that returns the {@link Iterator}. It breaks the
     * contract of {@link Iterable} in that it returns the supplied iterator
     * instance for each call to {@code iterator()} on the returned
     * {@link Iterable} instance. This method exists to make it easy to use an
     * {@link Iterator} in a for-loop.
     *
     * @param <T> the type of items in the iterator.
     * @param iterator the iterator to expose as an {@link Iterable}.
     * @return the supplied iterator posing as an {@link Iterable}.
     */
    public static <T> Iterable<T> loop( final Iterator<T> iterator )
    {
        return () -> iterator;
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
    //@Deprecated * @deprecated use {@link #loop(Iterator) the loop method} instead.
    public static <T> Iterable<T> asIterable( final Iterator<T> iterator )
    {
        return loop( iterator );
    }

    public static <T> long count( Iterator<T> iterator )
    {
        return count( iterator, Predicates.alwaysTrue() );
    }

    /**
     * Counts the number of filtered in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterator the {@link Iterator} to count items in.
     * @param filter the filter to test items against
     * @return the number of filtered items found in {@code iterator}.
     */
    public static <T> long count( Iterator<T> iterator, Predicate<T> filter )
    {
        long result = 0;
        while ( iterator.hasNext() )
        {
            if ( filter.test( iterator.next() ) )
            {
                result++;
            }
        }
        return result;
    }

    public static <T> Collection<T> asCollection( Iterator<T> iterable )
    {
        return addToCollection( iterable, new ArrayList<T>() );
    }

    public static <T> List<T> asList( Iterator<T> iterator )
    {
        return addToCollection( iterator, new ArrayList<T>() );
    }

    public static <T, EX extends Exception> List<T> asList( RawIterator<T, EX> iterator ) throws EX
    {
        List<T> out = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            out.add( iterator.next() );
        }
        return out;
    }

    public static <T> Set<T> asSet( Iterator<T> iterator )
    {
        return addToCollection( iterator, new HashSet<T>() );
    }

    /**
     * Creates a {@link Set} from an array of items.an
     *
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    @SafeVarargs
    public static <T> Set<T> asSet( T... items )
    {
        return new HashSet<>( Arrays.asList( items ) );
    }

    /**
     * Alias for asSet()
     *
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    @SafeVarargs
    public static <T> Set<T> set( T... items )
    {
        return asSet( items );
    }

    /**
     * Creates a {@link Set} from an array of items.
     *
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    @SafeVarargs
    public static <T> Set<T> asUniqueSet( T... items )
    {
        HashSet<T> set = new HashSet<>();
        for ( T item : items )
        {
            addUnique( set, item );
        }
        return set;
    }

    /**
     * Creates a {@link Set} from an array of items.
     *
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    public static <T> Set<T> asUniqueSet( Iterator<T> items )
    {
        HashSet<T> set = new HashSet<>();
        while ( items.hasNext() )
        {
            addUnique( set, items.next() );
        }
        return set;
    }

    public static Iterator<Long> asIterator( final long... array )
    {
        return new PrefetchingIterator<Long>()
        {
            private int index;

            @Override
            protected Long fetchNextOrNull()
            {
                try
                {
                    return index < array.length ? array[index] : null;
                }
                finally
                {
                    index++;
                }
            }
        };
    }

    public static Iterator<Integer> asIterator( final int... array )
    {
        return new PrefetchingIterator<Integer>()
        {
            private int index;

            @Override
            protected Integer fetchNextOrNull()
            {
                try
                {
                    return index < array.length ? array[index] : null;
                }
                finally
                {
                    index++;
                }
            }
        };
    }

    @SafeVarargs
    public static <T> Iterator<T> asIterator( final int maxItems, final T... array )
    {
        return new PrefetchingIterator<T>()
        {
            private int index;

            @Override
            protected T fetchNextOrNull()
            {
                try
                {
                    return index < array.length && index < maxItems ? array[index] : null;
                }
                finally
                {
                    index++;
                }
            }
        };
    }

    public static <T> Iterator<T> iterator( final T item )
    {
        if ( item == null )
        {
            return emptyIterator();
        }

        return new Iterator<T>()
        {
            T myItem = item;

            @Override
            public boolean hasNext()
            {
                return myItem != null;
            }

            @Override
            public T next()
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                T toReturn = myItem;
                myItem = null;
                return toReturn;
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    @SafeVarargs
    public static <T> Iterator<T> iterator( T ... items )
    {
        return asIterator( items.length, items );
    }

    @SafeVarargs
    public static <T> Iterator<T> iterator( int maxItems, T ... items )
    {
        return asIterator( maxItems, items );
    }

    @SuppressWarnings( "unchecked" )
    public static <T> ResourceIterator<T> emptyResourceIterator()
    {
        return EMPTY_RESOURCE_ITERATOR;
    }

    public static <T> boolean contains( Iterator<T> iterator, T item )
    {
        try
        {
            for ( T element : loop( iterator ) )
            {
                if ( item == null ? element == null : item.equals( element ) )
                {
                    return true;
                }
            }
            return false;
        }
        finally
        {
            if ( iterator instanceof ResourceIterator<?> )
            {
                ((ResourceIterator<?>) iterator).close();
            }
        }
    }

    public static <T> ResourceIterator<T> asResourceIterator( final Iterator<T> iterator )
    {
        if ( iterator instanceof ResourceIterator<?> )
        {
            return (ResourceIterator<T>) iterator;
        }
        return new WrappingResourceIterator<>( iterator );
    }

    public static <T> ResourceIterator<T> resourceIterator( final Iterator<T> iterator, final Resource resource )
    {
        return new PrefetchingResourceIterator<T>()
        {
            @Override
            public void close()
            {
                resource.close();
            }

            @Override
            protected T fetchNextOrNull()
            {
                return iterator.hasNext() ? iterator.next() : null;
            }
        };
    }

    @SafeVarargs
    public static <T> T[] array( T... items )
    {
        return items;
    }

    public static <X> Iterator<X> filter( Predicate<? super X> specification, Iterator<X> i )
    {
        return new FilterIterable.FilterIterator<>( i, specification );
    }

    public static <FROM, TO> Iterator<TO> map( Function<? super FROM, ? extends TO> function, Iterator<FROM> from )
    {
        return new MapIterable.MapIterator<>( from, function );
    }

    public static <FROM, TO, EX extends Exception> RawIterator<TO, EX> map(
            ThrowingFunction<? super FROM, ? extends TO, EX> function, RawIterator<FROM, EX> from )
    {
        return new RawMapIterator<>( from, function );
    }

    public static <T, EX extends Exception> RawIterator<T, EX> asRawIterator( Iterator<T> iter )
    {
        return new RawIterator<T,EX>()
        {
            @Override
            public boolean hasNext() throws EX
            {
                return iter.hasNext();
            }

            @Override
            public T next() throws EX
            {
                return iter.next();
            }
        };
    }

    public static <FROM, TO> Iterator<TO> flatMap( Function<? super FROM, ? extends Iterator<TO>> function, Iterator<FROM> from )
    {
        return new CombiningIterator<>( map(function, from) );
    }

    @SafeVarargs
    @SuppressWarnings( "unchecked" )
    public static <T> Iterator<T> concat( Iterator<? extends T>... iterators )
    {
        return concat( Arrays.asList( (Iterator<T>[]) iterators ).iterator() );
    }

    public static <T> ResourceIterator<T> concatResourceIterators( Iterator<ResourceIterator<T>> iterators )
    {
        return new CombiningResourceIterator<>( iterators );
    }

    public static <T> Iterator<T> concat( Iterator<Iterator<T>> iterators )
    {
        return new CombiningIterator<>( iterators );
    }

    public static <T> ResourceIterable<T> asResourceIterable( final ResourceIterator<T> it )
    {
        return () -> it;
    }

    public static String join( String joinString, Iterator<?> iter )
    {
        StringBuilder sb = new StringBuilder();
        while ( iter.hasNext() )
        {
            sb.append( iter.next().toString() );
            if ( iter.hasNext() )
            {
                sb.append( joinString );
            }
        }
        return sb.toString();
    }

    public static <T> PrefetchingIterator<T> prefetching( Iterator<T> iterator )
    {
        return iterator instanceof PrefetchingIterator ? (PrefetchingIterator<T>) iterator :
               new PrefetchingIterator<T>()
               {
                   @Override
                   protected T fetchNextOrNull()
                   {
                       return iterator.hasNext() ? iterator.next() : null;
                   }
               };
    }

    /**
     * Create a stream from the given iterator.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterator implements
     * {@link Resource}.
     *
     * @param iterator the iterator to convert to stream
     * @param <T> the type of elements in the given iterator
     * @return stream over the iterator elements
     * @throws NullPointerException when the given stream is {@code null}
     */
    public static <T> Stream<T> stream( Iterator<T> iterator )
    {
        return stream( iterator, 0 );
    }

    /**
     * Create a stream from the given iterator with given characteristics.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterator implements
     * {@link Resource}.
     *
     * @param iterator the iterator to convert to stream
     * @param characteristics the logical OR of characteristics for the underlying {@link Spliterator}
     * @param <T> the type of elements in the given iterator
     * @return stream over the iterator elements
     * @throws NullPointerException when the given iterator is {@code null}
     */
    public static <T> Stream<T> stream( Iterator<T> iterator, int characteristics )
    {
        Objects.requireNonNull( iterator );
        Spliterator<T> spliterator = Spliterators.spliteratorUnknownSize( iterator, characteristics );
        Stream<T> stream = StreamSupport.stream( spliterator, false );
        if ( iterator instanceof Resource )
        {
            return stream.onClose( ((Resource) iterator)::close );
        }
        return stream;
    }
}
