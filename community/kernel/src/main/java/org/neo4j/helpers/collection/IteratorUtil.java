/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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

import java.io.File;
import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.collection.primitive.PrimitiveIntIterator;
import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.graphdb.Resource;
import org.neo4j.graphdb.ResourceIterable;
import org.neo4j.graphdb.ResourceIterator;
import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.helpers.Function;
import org.neo4j.helpers.Pair;
import org.neo4j.kernel.impl.util.PrimitiveLongResourceIterator;

import static java.util.EnumSet.allOf;

import static org.neo4j.helpers.collection.Iterables.map;

/**
 * Contains common functionality regarding {@link Iterator}s and
 * {@link Iterable}s.
 */
public abstract class IteratorUtil
{
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

    private static <T> T assertNotNull( Iterator<T> iterator, T result )
    {
        if ( result == null )
        {
            throw new NoSuchElementException( "No element found in " + iterator );
        }
        return result;
    }

    /**
     * Returns the given iterable's first element or {@code null} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the first element in the {@code iterable}, or {@code null} if no
     * element found.
     */
    public static <T> T firstOrNull( Iterable<T> iterable )
    {
        return firstOrNull( iterable.iterator() );
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
        return first( iterable.iterator() );
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
        return lastOrNull( iterable.iterator() );
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
        return last( iterable.iterator() );
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
        return singleOrNull( iterable.iterator() );
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
        return single( iterable.iterator() );
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
        return single( iterable.iterator(), itemIfNone );
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
        return fromEnd( iterable.iterator(), n );
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
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<Long>> C addToCollection( PrimitiveLongIterator iterator, C collection )
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
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<Integer>> C addToCollection( PrimitiveIntIterator iterator, C collection )
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
    public static <C extends Collection<T>,T> C addToCollection( Iterable<T> iterable,
            C collection )
    {
        return addToCollection( iterable.iterator(), collection );
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
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return iterator;
            }
        };
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

    public static <T> int count( Iterator<T> iterator )
    {
        return count( iterator, Predicates.<T>alwaysTrue() );
    }

    /**
     * Counts the number of filtered in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterator the {@link Iterator} to count items in.
     * @param filter the filter to test items against
     * @return the number of filtered items found in {@code iterator}.
     */
    public static <T> int count( Iterator<T> iterator, Predicate<T> filter )
    {
        int result = 0;
        while ( iterator.hasNext() )
        {
            if ( filter.test( iterator.next() ) )
            {
                result++;
            }
        }
        return result;
    }

    /**
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @return the number of items found in {@code iterable}.
     */
    public static <T> int count( Iterable<T> iterable )
    {
        return count( iterable, Predicates.<T>alwaysTrue() );
    }

    /**
     * Counts the number of items in the {@code iterable} by looping through it.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @param filter the filter to apply
     * @return the number of items found in {@code iterator}.
     */
    public static <T> int count( Iterable<T> iterable, org.neo4j.helpers.Predicate<T> filter )
    {
        return count( iterable.iterator(), org.neo4j.helpers.Predicates.upgrade( filter ) );
    }

    /**
     * Counts the number of filtered items in the {@code iterable} by looping through it.
     *
     * @param <T> the type of items in the iterator.
     * @param iterable the {@link Iterable} to count items in.
     * @param filter the filter to test items against
     * @return the number of found in {@code iterable}.
     */
    public static <T> int count( Iterable<T> iterable, Predicate<T> filter )
    {
        return count( iterable.iterator(), filter );
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
        return addToCollection( iterable, new ArrayList<T>() );
    }
    public static <T> Collection<T> asCollection( Iterator<T> iterable )
    {
        return addToCollection( iterable, new ArrayList<T>() );
    }

    public static <T> List<T> asList( Iterator<T> iterator )
    {
        return addToCollection( iterator, new ArrayList<T>() );
    }

    public static <T> List<T> asList( Iterable<T> iterator )
    {
        return addToCollection( iterator, new ArrayList<T>() );
    }

    public static List<Long> asList( PrimitiveLongIterator iterator )
    {
        List<Long> out = new ArrayList<>();
        while(iterator.hasNext())
        {
            out.add(iterator.next());
        }
        return out;
    }

    public static List<Integer> asList( PrimitiveIntIterator iterator )
    {
        List<Integer> out = new ArrayList<>();
        while(iterator.hasNext())
        {
            out.add(iterator.next());
        }
        return out;
    }

    public static <T, U> Map<T, U> asMap( Iterable<Pair<T, U>> pairs )
    {
        Map<T, U> map = new HashMap<>();
        for (Pair<T, U> pair: pairs) {
            map.put(pair.first(), pair.other());
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
        return addToCollection( iterable, new HashSet<T>() );
    }

    public static <T> Set<T> asSet( Iterator<T> iterator )
    {
        return addToCollection( iterator, new HashSet<T>() );
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
        return addToCollectionUnique( iterable, new HashSet<T>() );
    }

    /**
     * Creates a {@link Set} from an array of items.
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

    public static <T> Set<T> emptySetOf( @SuppressWarnings("unused"/*just used as a type marker*/) Class<T> type )
    {
        return Collections.emptySet();
    }

    public static <T> List<T> emptyListOf( @SuppressWarnings("unused"/*just used as a type marker*/) Class<T> type )
    {
        return Collections.emptyList();
    }

    /**
     * Alias for asSet()
     * 
     * @param items the items to add to the set.
     * @param <T> the type of the items
     * @return the {@link Set} containing the items.
     */
    @SafeVarargs
    public static <T> Set<T> set( T... items)
    {
        return asSet(items);
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
        while( items.hasNext() )
        {
            addUnique( set, items.next() );
        }
        return set;
    }

    /**
     * Function for converting Enum to String
     */
    @SuppressWarnings( "rawtypes" )
    public final static Function<Enum, String> ENUM_NAME = new Function<Enum, String>()
    {
        @Override
        public String apply( Enum from )
        {
            return from.name();
        }
    };

    /**
     * Converts an {@link Iterable} of enums to {@link Set} of their names.
     *
     * @param enums the enums to convert.
     * @return the set of enum names
     */
    @SuppressWarnings( "rawtypes" )
    public static Set<String> asEnumNameSet( Iterable<Enum> enums )
    {
        return asSet( map( ENUM_NAME, enums ) );
    }

    /**
     * Converts an enum class to to {@link Set} of the names of all valid enum values
     *
     * @param clazz enum class
     * @param <E> enum type bound
     * @return the set of enum names
     */
    public static <E extends Enum<E>> Set<String> asEnumNameSet( Class<E> clazz)
    {
        return asSet( map( ENUM_NAME, allOf( clazz ) ) );
    }

    /**
     * Creates an {@link Iterable} for iterating over the lines of a text file.
     * @param file the file to get the lines for.
     * @param encoding the encoding of the file
     * @return an {@link Iterable} for iterating over the lines of a text file.
     */
    public static ClosableIterable<String> asIterable( final File file, final String encoding )
    {
        return new ClosableIterable<String>()
        {
            private ClosableIterator<String> mostRecentIterator;

            @Override
            public Iterator<String> iterator()
            {
                try
                {
                    if ( mostRecentIterator != null )
                    {
                        mostRecentIterator.close();
                    }
                    mostRecentIterator = asIterator( file, encoding );
                    return mostRecentIterator;
                }
                catch ( IOException e )
                {
                    throw new RuntimeException( e );
                }
            }

            @Override
            public void close()
            {
                if ( mostRecentIterator != null )
                {
                    mostRecentIterator.close();
                }
            }
        };
    }

    /**
     * Creates an {@link Iterator} for iterating over the lines of a text file.
     * The opened file is closed if an exception occurs during reading or when
     * the files has been read through all the way.
     * @param file the file to get the lines for.
     * @param encoding to be used for reading the file
     * @return an {@link Iterator} for iterating over the lines of a text file.
     * @throws IOException from underlying I/O operations
     */
    public static ClosableIterator<String> asIterator( File file, String encoding ) throws IOException
    {
        return new LinesOfFileIterator( file, encoding );
    }

    public static Iterable<Long> asIterable( final long... array )
    {
        return new Iterable<Long>()
        {
            @Override
            public Iterator<Long> iterator()
            {
                return asIterator( array );
            }
        };
    }

    @SafeVarargs
    public static <T> Iterable<T> asIterable( final T... array )
    {
        return new Iterable<T>()
        {
            @Override
            public Iterator<T> iterator()
            {
                return IteratorUtil.iterator( array );
            }
        };
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

    @SuppressWarnings( "rawtypes" )
    private static final ResourceIterator EMPTY_ITERATOR = new ResourceIterator()
    {
        @Override
        public boolean hasNext()
        {
            return false;
        }

        @Override
        public Object next()
        {
            throw new NoSuchElementException();
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void close()
        {
            // do nothing
        }
    };

    @SuppressWarnings( "unchecked" )
    public static <T> ResourceIterator<T> emptyIterator()
    {
        return EMPTY_ITERATOR;
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

    public static <T extends CloneableInPublic> Iterable<T> cloned( Iterable<T> items, final Class<T> itemClass )
    {
        return Iterables.map( new Function<T,T>()
        {
            @Override
            public T apply( T from )
            {
                return itemClass.cast( from.clone() );
            }
        }, items );
    }

    public static <T> ResourceIterator<T> asResourceIterator( final Iterator<T> iterator )
    {
        return new WrappingResourceIterator<>( iterator );
    }

    @SuppressWarnings("UnusedDeclaration"/*Useful when debugging in tests, but not used outside of debugging sessions*/)
    public static Iterator<Long> toJavaIterator( final PrimitiveLongIterator primIterator )
    {
        return new Iterator<Long>()
        {
            @Override
            public boolean hasNext()
            {
                return primIterator.hasNext();
            }

            @Override
            public Long next()
            {
                return primIterator.next();
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException(  );
            }
        };
    }

    public static List<Long> primitivesList( PrimitiveLongIterator iterator )
    {
        ArrayList<Long> result = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            result.add( iterator.next() );
        }
        return result;
    }

    public static Set<Long> asSet( PrimitiveLongIterator iterator )
    {
        return internalAsSet( iterator, false );
    }

    private static Set<Long> internalAsSet( PrimitiveLongIterator iterator, boolean allowDuplicates )
    {
        Set<Long> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            long value = iterator.next();
            if ( !set.add( value ) && !allowDuplicates )
            {
                throw new IllegalStateException( "Duplicates found. Tried to add " + value + " to " + set );
            }
        }
        return set;
    }

    public static Set<Integer> asSet( PrimitiveIntIterator iterator )
    {
        Set<Integer> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    /**
     * Creates a {@link Set} from an array of iterator.
     *
     * @param iterator the iterator to add to the set.
     * @return the {@link Set} containing the iterator.
     */
    public static Set<Long> asUniqueSet( PrimitiveLongIterator iterator )
    {
        HashSet<Long> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            addUnique( set, iterator.next() );
        }
        return set;
    }

    public static PrimitiveLongResourceIterator resourceIterator( final PrimitiveLongIterator iterator,
            final Resource resource )
    {
        return new PrimitiveLongResourceIterator()
        {
            @Override
            public void close()
            {
                if ( resource != null )
                {
                    resource.close();
                }
            }

            @Override
            public long next()
            {
                return iterator.next();
            }

            @Override
            public boolean hasNext()
            {
                return iterator.hasNext();
            }
        };
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

    public static <T> ResourceIterable<T> resourceIterable( final Iterable<T> iterable )
    {
        return new ResourceIterable<T>()
        {
            @Override
            public ResourceIterator<T> iterator()
            {
                return resourceIterator( iterable.iterator(), Resource.EMPTY );
            }
        };
    }

    @SafeVarargs
    public static <T> T[] array( T... items )
    {
        return items;
    }
}
