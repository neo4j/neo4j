/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
package org.neo4j.internal.helpers.collection;

import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import org.neo4j.function.Predicates;
import org.neo4j.function.ThrowingConsumer;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.internal.helpers.Exceptions;

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

    public static <X> Iterable<X> reverse( Iterable<X> iterable )
    {
        List<X> list = asList( iterable );
        Collections.reverse( list );
        return list;
    }

    public static <FROM, TO> Iterable<TO> map( Function<? super FROM, ? extends TO> function, Iterable<FROM> from )
    {
        return new MapIterable<>( from, function );
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

    public static <T> Iterable<T> concat( final Iterable<? extends Iterable<T>> iterables )
    {
        return new CombiningIterable<>( iterables );
    }

    public static <T, C extends T> Iterable<T> append( final C item, final Iterable<T> iterable )
    {
        return () ->
        {
            final Iterator<T> iterator = iterable.iterator();

            return new Iterator<>()
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
        return () -> Iterators.asResourceIterator( iterable.iterator() );
    }

    public static String toString( Iterable<?> values, String separator )
    {
        Iterator<?> it = values.iterator();
        StringBuilder sb = new StringBuilder();
        while ( it.hasNext() )
        {
            sb.append( it.next() );
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
        return addAll( new ArrayList<>(), iterable );
    }

    public static <T> List<T> asList( Iterable<T> iterator )
    {
        return addAll( new ArrayList<>(), iterator );
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
        return addAll( new HashSet<>(), iterable );
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

    public static <T> Iterable<T> option( final T item )
    {
        if ( item == null )
        {
            return Collections.emptyList();
        }

        return () -> Iterators.iterator( item );
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

    /**
     * Method for calling a lambda function on many objects when it is expected that the function might
     * throw an exception. First exception will be thrown and subsequent will be suppressed.
     * This method guarantees that all subjects will be consumed, unless {@link Error} happens.
     *
     * @param consumer lambda function to call on each object passed
     * @param subjects {@link Iterable} of objects to call the function on
     * @param <E> the type of exception anticipated, inferred from the lambda
     * @throws E if consumption fails with this exception
     */
    @SuppressWarnings( "unchecked" )
    public static <T, E extends Exception> void safeForAll( ThrowingConsumer<T,E> consumer, Iterable<T> subjects ) throws E
    {
        E exception = null;
        for ( T instance : subjects )
        {
            try
            {
                consumer.accept( instance );
            }
            catch ( Exception e )
            {
                exception = Exceptions.chain( exception, (E) e );
            }
        }
        if ( exception != null )
        {
            throw exception;
        }
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
