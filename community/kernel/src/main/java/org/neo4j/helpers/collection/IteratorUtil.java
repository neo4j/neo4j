/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static java.util.Arrays.asList;
import static java.util.EnumSet.allOf;
import static org.neo4j.helpers.collection.Iterables.map;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.helpers.Function;

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
     * @throws {@link NoSuchElementException} if no element found.
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
     * @throws {@link NoSuchElementException} if no element found.
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
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in {@code iterator}, or {@code null} if no
     * element found.
     * @throws {@link NoSuchElementException} if more than one element was
     * found.
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
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in the {@code iterator}.
     * @throws {@link NoSuchElementException} if there isn't exactly one
     * element.
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
        Deque<T> trail = new ArrayDeque<T>( n );
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
     * @param first
     * @param other
     * @return
     */
    public static boolean iteratorsEqual(Iterator<?> first, Iterator<?> other)
    {
        while(true)
        {
            if(first.hasNext() && other.hasNext())
            {
                if(!first.next().equals( other.next() ))
                {
                    return false;
                }
            } else
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
     * @throws {@link NoSuchElementException} if no element found.
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
     * @throws {@link NoSuchElementException} if no element found.
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
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws {@link NoSuchElementException} if more than one element was
     * found.
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
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in the {@code iterable}.
     * @throws {@link NoSuchElementException} if there isn't exactly one
     * element.
     */
    public static <T> T single( Iterable<T> iterable )
    {
        return single( iterable.iterator() );
    }
    
    /**
     * Returns the given iterable's single element or {@code null} if no
     * element found. If there is more than one element in the iterable a
     * {@link NoSuchElementException} will be thrown.
     * 
     * @param <T> the type of elements in {@code iterable}.
     * @param iterable the {@link Iterable} to get elements from.
     * @return the single element in {@code iterable}, or {@code null} if no
     * element found.
     * @throws {@link NoSuchElementException} if more than one element was
     * found.
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
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in {@code iterator}, or {@code itemIfNone} if no
     * element found.
     * @throws {@link NoSuchElementException} if more than one element was
     * found.
     */
    public static <T> T single( Iterator<T> iterator, T itemIfNone )
    {
        T result = iterator.hasNext() ? iterator.next() : itemIfNone;
        if ( iterator.hasNext() )
        {
            throw new NoSuchElementException( "More than one element in " +
                iterator + ". First element is '" + result +
                "' and the second element is '" + iterator.next() + "'" );
        }
        return result;
    }
    
    /**
     * Returns the iterator's n:th item from the end of the iteration.
     * If the iterator has got less than n-1 items in it {@code null} is returned.
     * 
     * @param <T> the type of elements in {@code iterator}.
     * @param iterable the {@link Iterable} to get elements from.
     * @param countFromEnd the n:th item from the end to get.
     * @return the iterator's n:th item from the end of the iteration,
     * or {@code null} if the iterator doesn't contain that many items.
     */
    public static <T> T fromEndOrNull( Iterable<T> iterable, int countFromEnd )
    {
        return fromEndOrNull( iterable.iterator(), countFromEnd );
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

    /**
     * Creates a {@link Set} from an array of items.
     *
     * @param items the items to add to the set.
     * @return the {@link Set} containing the items.
     */
    public static <T> Set<T> asSet( T... items )
    {
        return new HashSet<T>( asList( items ) );
    }

    /**
     * Function for converting Enum to String
     */
    public static Function<Enum, String> ENUM_NAME = new Function<Enum, String>()
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
     * @return an {@link Iterable} for iterating over the lines of a text file.
     */
    public static ClosableIterable<String> asIterable( final File file )
    {
        return new ClosableIterable<String>()
        {
            private ClosableIterator<String> mostRecentIterator;
            
            @Override
            public Iterator<String> iterator()
            {
                try
                {
                    if ( mostRecentIterator != null ) mostRecentIterator.close();
                    mostRecentIterator = asIterator( file );
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
                if ( mostRecentIterator != null ) mostRecentIterator.close();
            }
        };
    }
    
    /**
     * Creates an {@link Iterator} for iterating over the lines of a text file.
     * The opened file is closed if an exception occurs during reading or when
     * the files has been read through all the way.
     * @param file the file to get the lines for.
     * @return an {@link Iterator} for iterating over the lines of a text file.
     */
    public static ClosableIterator<String> asIterator( File file ) throws IOException
    {
        return new LinesOfFileIterator( file );
    }
    
    public static <T> void streamToFile( Iterable<T> iterable, File file, String encoding ) throws IOException
    {
        streamToFile( iterable.iterator(), file, encoding );
    }

    public static <T> void streamToFile( Iterator<T> iterator, File file, String encoding ) throws IOException
    {
        if ( file.exists() ) throw new IOException( "File '" + file + "' already exists" );
        PrintStream out = null;
        try
        {
            out = new PrintStream( file, encoding );
            while ( iterator.hasNext() ) out.println( iterator.next().toString() );
        }
        finally
        {
            safeClose( out );
        }
    }
    
    private static void safeClose( Closeable closeable )
    {
        if ( closeable != null )
        {
            try
            {
                closeable.close();
            }
            catch ( IOException e )
            {
                // What can we do?
                e.printStackTrace();
            }
        }
    }
    
    public static Iterable<Long> asIterable( final long... array )
    {
        return new Iterable<Long>()
        {
            @Override
            public Iterator<Long> iterator()
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
        };
    }

    public static <T> Iterator<T> asIterator( final T... array )
    {
        return new PrefetchingIterator<T>()
        {
            private int index;
            
            @Override
            protected T fetchNextOrNull()
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
    
    @SuppressWarnings( "rawtypes" )
    private static final Iterator EMPTY_ITERATOR = new Iterator()
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
    };
    
    @SuppressWarnings( "unchecked" )
    public static <T> Iterator<T> emptyIterator()
    {
        return EMPTY_ITERATOR;
    }
}
