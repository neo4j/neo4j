/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;

import static org.neo4j.helpers.collection.Iterators.asResourceIterator;

public final class Iterables
{
    private Iterables()
    {
        throw new AssertionError( "no instance" );
    }

    public static <T> Iterable<T> empty()
    {
        return Collections.emptyList();
    }

    @SuppressWarnings( "unchecked" )
    public static <T> Iterable<T> emptyResourceIterable()
    {
        return (Iterable<T>) EmptyResourceIterable.EMPTY_RESOURCE_ITERABLE;
    }

    public static <T> Iterable<T> limit( final int limitItems, final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                int count;

                @Override
                public boolean hasNext()
                {
                    return count < limitItems && iterator.hasNext();
                }

                @Override
                public T next()
                {
                    count++;
                    return iterator.next();
                }

                @Override
                public void remove()
                {
                    iterator.remove();
                }
            };
        };
    }

    public static <T> Function<Iterable<T>, Iterable<T>> limit( final int limitItems )
    {
        return ts -> limit( limitItems, ts );
    }

    public static <T> Iterable<T> unique( final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                Set<T> items = new HashSet<>();
                T nextItem;

                @Override
                public boolean hasNext()
                {
                    while ( iterator.hasNext() )
                    {
                        nextItem = iterator.next();
                        if ( items.add( nextItem ) )
                        {
                            return true;
                        }
                    }

                    return false;
                }

                @Override
                public T next()
                {
                    if ( nextItem == null && !hasNext() )
                    {
                        throw new NoSuchElementException();
                    }

                    return nextItem;
                }

                @Override
                public void remove()
                {
                }
            };
        };
    }

    public static <T, C extends Collection<T>> C addAll( C collection, Iterable<? extends T> iterable )
    {
        Iterator<? extends T> iterator = iterable.iterator();
        try
        {
            while ( iterator.hasNext() )
            {
                collection.add( iterator.next() );
            }
        }
        finally
        {
            if ( iterator instanceof AutoCloseable )
            {
                try
                {
                    ((AutoCloseable) iterator).close();
                }
                catch ( Exception e )
                {
                    // Ignore
                }
            }
        }

        return collection;
    }

    public static <X> Iterable<X> filter( Predicate<? super X> specification, Iterable<X> i )
    {
        return new FilterIterable<>( i, specification );
    }

    public static <X> Iterable<X> skip( final int skip, final Iterable<X> iterable )
    {
        return () ->
        {
            Iterator<X> iterator = iterable.iterator();

            for ( int i = 0; i < skip; i++ )
            {
                if ( iterator.hasNext() )
                {
                    iterator.next();
                }
                else
                {
                    return Collections.emptyIterator();
                }
            }

            return iterator;
        };
    }

    public static <X> Iterable<X> reverse( Iterable<X> iterable )
    {
        List<X> list = asList( iterable );
        Collections.reverse( list );
        return list;
    }

    @SafeVarargs
    public static <X, I extends Iterable<? extends X>> Iterable<X> flatten( I... multiIterator )
    {
        return new FlattenIterable<>( Arrays.asList(multiIterator) );
    }

    public static <X, S extends Iterable<? extends X>, I extends Iterable<S>> Iterable<X> flattenIterable(
            I multiIterator )
    {
        return new FlattenIterable<>( multiIterator );
    }

    public static <FROM, TO> Iterable<TO> map( Function<? super FROM, ? extends TO> function, Iterable<FROM> from )
    {
        return new MapIterable<>( from, function );
    }

    public static <FROM, TO> Iterable<TO> flatMap( Function<? super FROM, ? extends Iterable<TO>> function, Iterable<FROM> from )
    {
        return new CombiningIterable<>( map(function, from) );
    }

    @SafeVarargs
    @SuppressWarnings( "unchecked" )
    public static <T, C extends T> Iterable<T> iterable( C... items )
    {
        return (Iterable<T>) Arrays.asList( items );
    }

    @SuppressWarnings( "unchecked" )
    public static <T, C> Iterable<T> cast( Iterable<C> iterable )
    {
        return (Iterable<T>) iterable;
    }

    @SafeVarargs
    @SuppressWarnings( "unchecked" )
    public static <T> Iterable<T> concat( Iterable<? extends T>... iterables )
    {
        return concat( Arrays.asList( (Iterable<T>[]) iterables ) );
    }

    public static <T> Iterable<T> concat( final Iterable<Iterable<T>> iterables )
    {
        return new CombiningIterable<>( iterables );
    }

    public static <T, C extends T> Iterable<T> prepend( final C item, final Iterable<T> iterable )
    {
        return () -> new Iterator<T>()
        {
            T first = item;
            Iterator<T> iterator;

            @Override
            public boolean hasNext()
            {
                if ( first != null )
                {
                    return true;
                }
                if ( iterator == null )
                {
                    iterator = iterable.iterator();
                }

                return iterator.hasNext();
            }

            @Override
            public T next()
            {
                if ( first != null )
                {
                    try
                    {
                        return first;
                    }
                    finally
                    {
                        first = null;
                    }
                }
                return iterator.next();
            }

            @Override
            public void remove()
            {
            }
        };
    }

    public static <T, C extends T> Iterable<T> append( final C item, final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<T>()
            {
                T last = item;

                @Override
                public boolean hasNext()
                {
                    return iterator.hasNext() || last != null;
                }

                @Override
                public T next()
                {
                    if ( iterator.hasNext() )
                    {
                        return iterator.next();
                    }
                    try
                    {
                        return last;
                    }
                    finally
                    {
                        last = null;
                    }
                }

                @Override
                public void remove()
                {
                }
            };
        };
    }

    public static <T> Iterable<T> cache( Iterable<T> iterable )
    {
        return new CacheIterable<>( iterable );
    }

    public static Object[] asArray( Iterable<Object> iterable )
    {
        return asArray( Object.class, iterable );
    }

    @SuppressWarnings( "unchecked" )
    public static <T> T[] asArray( Class<T> componentType, Iterable<T> iterable )
    {
        if ( iterable == null )
        {
            return null;
        }

        List<T> list = asList( iterable );
        return list.toArray( (T[]) Array.newInstance( componentType, list.size() ) );
    }

    public static <T> ResourceIterable<T> asResourceIterable( final Iterable<T> iterable )
    {
        if ( iterable instanceof ResourceIterable<?> )
        {
            return (ResourceIterable<T>) iterable;
        }
        return () -> asResourceIterator( iterable.iterator() );
    }

    public static String toString( Iterable<?> values, String separator )
    {
        Iterator<?> it = values.iterator();
        StringBuilder sb = new StringBuilder();
        while ( it.hasNext() )
        {
            sb.append( it.next().toString() );
            if ( it.hasNext() )
            {
                sb.append( separator );
            }
        }
        return sb.toString();
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull( Iterable<T> iterable )
    {
        Iterator<T> iterator = iterable.iterator();
        try
        {
            return Iterators.firstOrNull( iterator );
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
     * Returns the given iterable's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first( Iterable<T> iterable )
    {
        return Iterators.first( iterable.iterator() );
    }

    /**
     * Returns the given iterable's last element or {@code null} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the last element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T lastOrNull( Iterable<T> iterable )
    {
        return Iterators.lastOrNull( iterable.iterator() );
    }

    /**
     * Returns the given iterable's last element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the last element in the {@code iterable}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T last( Iterable<T> iterable )
    {
        return Iterators.last( iterable.iterator() );
    }

    /**
     * Returns the given iterable's single element or {@code null} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T singleOrNull( Iterable<T> iterable )
    {
        return Iterators.singleOrNull( iterable.iterator() );
    }

    /**
     * Returns the given iterable's single element. If there are no elements
     * or more than one element in the iterable a {@link NoSuchElementException}
     * will be thrown.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in the {@code iterable}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single( Iterable<T> iterable )
    {
        return Iterators.single( iterable.iterator() );
    }

    /**
     * Returns the given iterable's single element or {@code itemIfNone} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     *
     * If the {@link Iterable#iterator() iterator} created by the {@code iterable} implements {@link Resource}
     * it will be {@link Resource#close() closed} in a {@code finally} block after the single item
     * has been retrieved, or failed to be retrieved.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @param itemIfNone item to use if none is found
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T single( Iterable<T> iterable, T itemIfNone )
    {
        return Iterators.single( iterable.iterator(), itemIfNone );
    }

    /**
     * Returns the iterator's n:th item from the end of the iteration.
     * If the iterator has got less than n-1 items in it
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterable the {@link Iterable} to get elements from.
     * @param n the n:th item from the end to get.
     * @return the iterator's n:th item from the end of the iteration.
     * @throws NoSuchElementException if the iterator contains less than n-1 items.
     */
    public static <T> T fromEnd( Iterable<T> iterable, int n )
    {
        return Iterators.fromEnd( iterable.iterator(), n );
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
        return Iterators.addToCollection( iterable.iterator(), collection );
    }

    /**
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of items found in {@code iterable}.
     */
    public static <T> long count( Iterable<T> iterable )
    {
        return count( iterable, Predicates.alwaysTrue() );
    }

    /**
     * Counts the number of filtered items in the {@code iterable} by looping through it.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @param filter the filter to test items against
     * @return the number of found in {@code iterable}.
     */
    public static <T> long count( Iterable<T> iterable, Predicate<T> filter )
    {
        Iterator<T> iterator = iterable.iterator();
        try
        {
            return Iterators.count( iterator, filter );
        }
        finally
        {
            if ( iterator instanceof ResourceIterator )
            {
                ((ResourceIterator<?>) iterator).close();
            }
        }
    }

    /**
     * Creates a collection from an iterable.
     *
     * @param iterable The iterable to create the collection from.
     * @param <T> The generic type of both the iterable and the collection.
     * @return a collection containing all items from the iterable.
     */
    public static <T> Collection<T> asCollection( Iterable<T> iterable )
    {
        return addToCollection( iterable, new ArrayList<>() );
    }

    public static <T> List<T> asList( Iterable<T> iterator )
    {
        return addToCollection( iterator, new ArrayList<>() );
    }

    public static <T, U> Map<T, U> asMap( Iterable<Pair<T, U>> pairs )
    {
        Map<T, U> map = new HashMap<>();
        for ( Pair<T,U> pair : pairs )
        {
            map.put( pair.first(), pair.other() );
        }
        return map;
    }

    /**
     * Creates a {@link Set} from an {@link Iterable}.
     *
     * @param iterable The items to create the set from.
     * @param <T> The generic type of items.
     * @return a set containing all items from the {@link Iterable}.
     */
    public static <T> Set<T> asSet( Iterable<T> iterable )
    {
        return addToCollection( iterable, new HashSet<>() );
    }

    /**
     * Creates a {@link Set} from an {@link Iterable}.
     *
     * @param iterable The items to create the set from.
     * @param <T> The generic type of items.
     * @return a set containing all items from the {@link Iterable}.
     */
    public static <T> Set<T> asUniqueSet( Iterable<T> iterable )
    {
        return Iterators.addToCollectionUnique( iterable, new HashSet<>() );
    }

    public static Iterable<Long> asIterable( final long... array )
    {
        return () -> Iterators.asIterator( array );
    }

    public static Iterable<Integer> asIterable( final int... array )
    {
        return () -> Iterators.asIterator( array );
    }

    @SafeVarargs
    public static <T> Iterable<T> asIterable( final T... array )
    {
        return () -> Iterators.iterator( array );
    }

    public static <T> ResourceIterable<T> resourceIterable( final Iterable<T> iterable )
    {
        return () -> Iterators.resourceIterator( iterable.iterator(), Resource.EMPTY );
    }

    private static class FlattenIterable<T, I extends Iterable<? extends T>> implements Iterable<T>
    {
        private final Iterable<I> iterable;

        FlattenIterable( Iterable<I> iterable )
        {
            this.iterable = iterable;
        }

        @Override
        public Iterator<T> iterator()
        {
            return new FlattenIterator<>( iterable.iterator() );
        }

        static class FlattenIterator<T, I extends Iterable<? extends T>>
                implements Iterator<T>
        {
            private final Iterator<I> iterator;
            private Iterator<? extends T> currentIterator;

            FlattenIterator( Iterator<I> iterator )
            {
                this.iterator = iterator;
                currentIterator = null;
            }

            @Override
            public boolean hasNext()
            {
                if ( currentIterator == null )
                {
                    if ( iterator.hasNext() )
                    {
                        I next = iterator.next();
                        currentIterator = next.iterator();
                    }
                    else
                    {
                        return false;
                    }
                }

                while ( !currentIterator.hasNext() && iterator.hasNext() )
                {
                    currentIterator = iterator.next().iterator();
                }

                return currentIterator.hasNext();
            }

            @Override
            public T next()
            {
                return currentIterator.next();
            }

            @Override
            public void remove()
            {
                if ( currentIterator == null )
                {
                    throw new IllegalStateException();
                }

                currentIterator.remove();
            }
        }
    }

    private static class CacheIterable<T> implements Iterable<T>
    {
        private final Iterable<T> iterable;
        private Iterable<T> cache;

        private CacheIterable( Iterable<T> iterable )
        {
            this.iterable = iterable;
        }

        @Override
        public Iterator<T> iterator()
        {
            if ( cache != null )
            {
                return cache.iterator();
            }

            final Iterator<T> source = iterable.iterator();

            return new Iterator<T>()
            {
                List<T> iteratorCache = new ArrayList<>();

                @Override
                public boolean hasNext()
                {
                    boolean hasNext = source.hasNext();
                    if ( !hasNext )
                    {
                        cache = iteratorCache;
                    }
                    return hasNext;
                }

                @Override
                public T next()
                {
                    T next = source.next();
                    iteratorCache.add( next );
                    return next;
                }

                @Override
                public void remove()
                {

                }
            };
        }
    }

    /**
     * Returns the index of the first occurrence of the specified element
     * in this iterable, or -1 if this iterable does not contain the element.
     * More formally, returns the lowest index {@code i} such that
     * {@code (o==null ? get(i)==null : o.equals(get(i)))},
     * or -1 if there is no such index.
     *
     * @param itemToFind element to find
     * @param iterable iterable to look for the element in
     * @param <T> the type of the elements
     * @return the index of the first occurrence of the specified element
     *         (or {@code null} if that was specified) or {@code -1}
     */
    public static <T> int indexOf( T itemToFind, Iterable<T> iterable )
    {
        if ( itemToFind == null )
        {
            int index = 0;
            for ( T item : iterable )
            {
                if ( item == null )
                {
                    return index;
                }
                index++;
            }
        }
        else
        {
            int index = 0;
            for ( T item : iterable )
            {
                if ( itemToFind.equals( item ) )
                {
                    return index;
                }
                index++;
            }
        }
        return -1;
    }

    public static <T> Iterable<T> option( final T item )
    {
        if ( item == null )
        {
            return Collections.emptyList();
        }

        return () -> Iterators.iterator( item );
    }

    public static <T, S extends Comparable<? super S>> Iterable<T> sort( Iterable<T> iterable, final Function<T, S> compareFunction )
    {
        List<T> list = asList( iterable );
        list.sort( Comparator.comparing( compareFunction ) );
        return list;
    }

    public static String join( String joinString, Iterable<?> iter )
    {
        return Iterators.join( joinString, iter.iterator() );
    }

    /**
     * Create a stream from the given iterable.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream( Iterable<T> iterable )
    {
        return stream( iterable, 0 );
    }

    /**
     * Create a stream from the given iterable with given characteristics.
     * <p>
     * <b>Note:</b> returned stream needs to be closed via {@link Stream#close()} if the given iterable implements
     * {@link Resource}.
     *
     * @param iterable the iterable to convert to stream
     * @param characteristics the logical OR of characteristics for the underlying {@link Spliterator}
     * @param <T> the type of elements in the given iterable
     * @return stream over the iterable elements
     * @throws NullPointerException when the given iterable is {@code null}
     */
    public static <T> Stream<T> stream( Iterable<T> iterable, int characteristics )
    {
        Objects.requireNonNull( iterable );
        return Iterators.stream( iterable.iterator(), characteristics );
    }

    private static class EmptyResourceIterable<T> implements ResourceIterable<T>
    {
        private static final ResourceIterable<Object> EMPTY_RESOURCE_ITERABLE = new EmptyResourceIterable<>();

        @Override
        public ResourceIterator<T> iterator()
        {
            return Iterators.emptyResourceIterator();
        }
    }
}
