/**
 * Copyright (c) 2002-2015 "Neo Technology,"
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

import java.lang.reflect.Array;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.NoSuchElementException;
import java.util.Set;

import org.neo4j.function.Function;
import org.neo4j.function.Function2;
import org.neo4j.helpers.CloneableInPublic;
import org.neo4j.helpers.Predicate;
import org.neo4j.helpers.Predicates;

import static org.neo4j.helpers.Exceptions.launderedException;

/**
 * Collection of useful, common and generic implementations of {@link Iterator}. Most implementations
 * extend {@link BaseIterator} due to it's one-method-simplicity as opposed to two-method of implementing
 * {@link Iterator} directly. In here you'll find iterators that {@link #filter(Iterator, Predicate)},
 * {@link #map(Iterator, Function)}, {@link #reversed(Iterator) reverse},
 * {@link #catching(Iterator, Predicate) catches certain exceptions}, {@link #interleave(Iterable)},
 * {@link #skip(Iterator, int)}, {@link #limit(Iterator, int)} and more.
 *
 * @author Mattias Persson
 * @see NewIterables for {@link Iterable} versions of these iterators, basically.
 */
public abstract class NewIterators
{
    private NewIterators()
    {   // Singleton
    }

    // Base
    /**
     * Convenient base class for when implementing an {@link Iterator}. Instead of implementing {@link #hasNext()}
     * and {@link #next()} and keeping and clearing state between those, just implement {@link #fetchNextOrNull()}
     * which marks the end of the iterator by returning {@code null}.
     *
     * @param <T> type of items in this iterator.
     */
    public static abstract class BaseIterator<T> implements Iterator<T>
    {
        private T next;

        protected abstract T fetchNextOrNull();

        @Override
        public boolean hasNext()
        {
            if ( next == null )
            {
                next = fetchNextOrNull();
            }
            return next != null;
        }

        @Override
        public T next()
        {
            try
            {
                if ( !hasNext() )
                {
                    throw new NoSuchElementException();
                }
                return next;
            }
            finally
            {
                next = null;
            }
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }
    }

    // Array
    @SafeVarargs
    public static <T> Iterator<T> iterator( final T... items )
    {
        return new BaseIterator<T>()
        {
            private int index = -1;

            @Override
            protected T fetchNextOrNull()
            {
                return ++index < items.length ? items[index] : null;
            }
        };
    }

    @SafeVarargs
    public static <T> Iterator<T> reversed( final T... items )
    {
        return new BaseIterator<T>()
        {
            private int index = items.length;

            @Override
            protected T fetchNextOrNull()
            {
                return --index >= 0 ? items[index] : null;
            }
        };
    }

    // Caching
    public static <T> ListIterator<T> caching( Iterator<T> source )
    {
        return new CachingIterator<>( source );
    }

    public static class CachingIterator<T> extends BaseIterator<T> implements ListIterator<T>
    {
        private final Iterator<T> source;
        private final List<T> visited;
        private int lastReturnedPosition = -1;

        /**
         * Creates a new caching iterator using {@code source} as its underlying
         * {@link Iterator} to get items lazily from.
         * @param source the underlying {@link Iterator} to lazily get items from.
         */
        public CachingIterator( Iterator<T> source )
        {
            this( source, new ArrayList<T>() );
        }

        public CachingIterator( Iterator<T> source, List<T> visited )
        {
            this.source = source;
            this.visited = visited;
        }

        @Override
        protected T fetchNextOrNull()
        {
            T item = null;
            int position = position();
            if ( position < visited.size() )
            {
                item = visited.get( position );
            }
            else
            {
                if ( !source.hasNext() )
                {
                    return null;
                }
                item = source.next();
                visited.add( item );
            }

            lastReturnedPosition++;
            return item;
        }

        /**
         * Returns the current position of the iterator, initially 0. The position
         * represents the index of the item which will be returned by the next call
         * to {@link #next()} and also the index of the next item returned by
         * {@link #previous()} plus one. An example:
         *
         * <ul>
         * <li>Instantiate an iterator which would iterate over the strings "first", "second" and "third".</li>
         * <li>Get the two first items ("first" and "second") from it by using {@link #next()},
         * {@link #position()} will now return 2.</li>
         * <li>Call {@link #previous()} (which will return "second") and {@link #position()} will now be 1</li>
         * </ul>
         *
         * @return the position of the iterator.
         */
        public int position()
        {
            return lastReturnedPosition + 1;
        }

        private int highestVisitedPosition()
        {
            return visited.size() - 1;
        }

        /**
         * Sets the position of the iterator. {@code 0} means all the way back to
         * the beginning. It is also possible to set the position to one higher
         * than the last item, so that the next call to {@link #previous()} would
         * return the last item. Items will be cached along the way if necessary.
         *
         * @param newPosition the position to set for the iterator, must be
         * non-negative.
         * @return the position before changing to the new position.
         */
        public int position( int newPosition )
        {
            if ( newPosition < 0 )
            {
                throw new IllegalArgumentException( "Position must be non-negative, was " + newPosition );
            }

            int previousPosition = position();
            while ( newPosition > highestVisitedPosition() )
            {
                if ( source.hasNext() )
                {
                    visited.add( source.next() );
                }
                else
                {
                    throw new NoSuchElementException( "Requested position " + newPosition +
                            ", but didn't get further than to " + highestVisitedPosition() );
                }
            }
            lastReturnedPosition = newPosition;
            return previousPosition;
        }

        /**
         * Returns whether or not a call to {@link #previous()} will be able to
         * return an item or not. So it will return {@code true} if
         * {@link #position()} is bigger than 0.
         *
         * {@inheritDoc}
         */
        @Override
        public boolean hasPrevious()
        {
            return lastReturnedPosition >= 0;
        }

        @Override
        public T previous()
        {
            if ( !hasPrevious() )
            {
                throw new NoSuchElementException( "Position is " + position() );
            }
            T item = visited.get( lastReturnedPosition-- );
            return item;
        }

        /**
         * Returns the last item returned by {@link #next()}/{@link #previous()}.
         * If no call has been made to {@link #next()} or {@link #previous()} since
         * this iterator was created or since a call to {@link #position(int)} has
         * been made a {@link NoSuchElementException} will be thrown.
         *
         * @return the last item returned by {@link #next()}/{@link #previous()}.
         * @throws NoSuchElementException if no call has been made to {@link #next()}
         * or {@link #previous()} since this iterator was created or since a call to
         * {@link #position(int)} has been made.
         */
        public T current()
        {
            if ( lastReturnedPosition == -1 )
            {
                throw new NoSuchElementException();
            }
            return visited.get( lastReturnedPosition );
        }

        @Override
        public int nextIndex()
        {
            return lastReturnedPosition + 1;
        }

        @Override
        public int previousIndex()
        {
            return lastReturnedPosition;
        }

        @Override
        public void set( T e )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void add( T e )
        {
            throw new UnsupportedOperationException();
        }
    }

    // Catching
    public static <T> Iterator<T> catching( Iterator<T> source, final Predicate<Throwable> catchAndIgnoreException )
    {
        return new CatchingIterator<T>( source )
        {
            @Override
            protected boolean exceptionOk( Throwable t )
            {
                return catchAndIgnoreException.accept( t );
            }
        };
    }

    public static abstract class CatchingIterator<T> extends BaseIterator<T>
    {
        private final Iterator<T> source;

        public CatchingIterator( Iterator<T> source )
        {
            this.source = source;
        }

        @Override
        protected T fetchNextOrNull()
        {
            while ( source.hasNext() )
            {
                T nextItem = null;
                try
                {
                    return source.next();
                }
                catch ( Throwable t )
                {
                    if ( exceptionOk( t ) )
                    {
                        itemIgnored( nextItem );
                        continue;
                    }
                    throw launderedException( t );
                }
            }
            return null;
        }

        protected void itemIgnored( T item )
        {   // Do nothing by default
        }

        protected abstract boolean exceptionOk( Throwable t );
    }

    // Concating
    public static <T> Iterator<T> concat( Iterator<? extends Iterator<T>> iterators )
    {
        return new ConcatingIterator<>( iterators );
    }

    public static <T> Iterator<T> concatIterables( Iterator<Iterable<T>> iterables )
    {
        return new ConcatingIterator<>( new MappingIterator<Iterable<T>, Iterator<T>>( iterables )
        {
            @Override
            protected Iterator<T> map( Iterable<T> item )
            {
                return item.iterator();
            }
        } );
    }

    public static <T> Iterator<T> prepend( final T item, final Iterator<T> iterator )
    {
        return new BaseIterator<T>()
        {
            private boolean singleItemReturned;

            @Override
            protected T fetchNextOrNull()
            {
                if ( !singleItemReturned )
                {
                    singleItemReturned = true;
                    return item;
                }
                return iterator.hasNext() ? iterator.next() : null;
            }
        };
    }

    public static <T> Iterator<T> append( final Iterator<T> iterator, final T item )
    {
        return new BaseIterator<T>()
        {
            private boolean singleItemReturned;

            @Override
            protected T fetchNextOrNull()
            {
                if ( iterator.hasNext() )
                {
                    return iterator.next();
                }
                else if ( !singleItemReturned )
                {
                    singleItemReturned = true;
                    return item;
                }
                return null;
            }
        };
    }

    public static class ConcatingIterator<T> extends BaseIterator<T>
    {
        private final Iterator<? extends Iterator<T>> iterators;
        private Iterator<T> currentIterator;

        public ConcatingIterator( Iterator<? extends Iterator<T>> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected T fetchNextOrNull()
        {
            if ( currentIterator == null || !currentIterator.hasNext() )
            {
                while ( iterators.hasNext() )
                {
                    currentIterator = iterators.next();
                    if ( currentIterator.hasNext() )
                    {
                        break;
                    }
                }
            }
            return currentIterator != null && currentIterator.hasNext() ? currentIterator.next() : null;
        }

        protected final Iterator<T> currentIterator()
        {
            return currentIterator;
        }
    }

    // Interleaving
    // Needs to be an Iterable<Iterator<T>> opposed to Iterator<Iterator<T>> since interleaving will
    // go over the iterators multiple passes.
    public static <T> Iterator<T> interleave( Iterable<Iterator<T>> iterators )
    {
        return new InterleavingIterator<>( iterators );
    }

    public static class InterleavingIterator<T> extends BaseIterator<T>
    {
        private final Iterable<Iterator<T>> iterators;
        private Iterator<Iterator<T>> currentRound;

        public InterleavingIterator( Iterable<Iterator<T>> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected T fetchNextOrNull()
        {
            if ( currentRound == null || !currentRound.hasNext() )
            {
                currentRound = iterators.iterator();
            }
            while ( currentRound.hasNext() )
            {
                Iterator<T> iterator = currentRound.next();
                if ( iterator.hasNext() )
                {
                    return iterator.next();
                }
            }
            currentRound = null;
            return null;
        }
    }

    // Filtering
    public static <T> Iterator<T> filter( Iterator<T> source, final Predicate<T> filter )
    {
        return new FilteringIterator<T>( source )
        {
            @Override
            public boolean accept( T item )
            {
                return filter.accept( item );
            }
        };
    }

    public static <T> Iterator<T> dedup( Iterator<T> source )
    {
        // Overriding #accept saves one object instantiation (the Predicate)
        return new FilteringIterator<T>( source )
        {
            private final Set<T> visitedItems = new HashSet<>();

            @Override
            public boolean accept( T item )
            {
                return visitedItems.add( item );
            }
        };
    }

    public static <T> Iterator<T> notNull( Iterator<T> source )
    {
        return filter( source, Predicates.<T>notNull() );
    }

    public static <T> Iterator<T> skip( Iterator<T> source, final int skipTheFirstNItems )
    {
        return new FilteringIterator<T>( source )
        {
            private int skipped = 0;

            @Override
            public boolean accept( T item )
            {
                if ( skipped < skipTheFirstNItems )
                {
                    skipped++;
                    return false;
                }
                return true;
            }
        };
    }

    public static abstract class FilteringIterator<T> extends BaseIterator<T> implements Predicate<T>
    {
        private final Iterator<T> source;

        public FilteringIterator( Iterator<T> source )
        {
            this.source = source;
        }

        @Override
        protected T fetchNextOrNull()
        {
            while ( source.hasNext() )
            {
                T testItem = source.next();
                if ( accept( testItem ) )
                {
                    return testItem;
                }
            }
            return null;
        }

        @Override
        public abstract boolean accept( T item );
    }

    // Limiting
    public static <T> Iterator<T> limit( final Iterator<T> source, final int maxItems )
    {
        return new BaseIterator<T>()
        {
            private int visited;

            @Override
            protected T fetchNextOrNull()
            {
                if ( visited++ < maxItems )
                {
                    if ( source.hasNext() )
                    {
                        return source.next();
                    }
                }
                return null;
            }
        };
    }

    // Mapping
    public static <FROM,TO> Iterator<TO> map( Iterator<FROM> source, final Function<FROM,TO> function )
    {
        return new MappingIterator<FROM,TO>( source )
        {
            @Override
            protected TO map( FROM item )
            {
                return function.apply( item );
            }
        };
    }

    public static <FROM,TO> Iterator<TO> cast( Iterator<FROM> source, final Class<TO> to )
    {
        return new MappingIterator<FROM, TO>( source )
        {
            @Override
            protected TO map( FROM item )
            {
                return to.cast( item );
            }
        };
    }

    public static abstract class MappingIterator<FROM,TO> extends BaseIterator<TO>
    {
        protected final Iterator<FROM> source;

        public MappingIterator( Iterator<FROM> source )
        {
            this.source = source;
        }

        @Override
        protected TO fetchNextOrNull()
        {
            return source.hasNext() ? map( source.next() ) : null;
        }

        protected abstract TO map( FROM item );
    }

    // Nested
    public static <FROM,TO> Iterator<TO> nested( Iterator<FROM> source, final Function<FROM,Iterator<TO>> nester )
    {
        return new NestedIterator<FROM,TO>( source )
        {
            @Override
            protected Iterator<TO> nested( FROM surfaceItem )
            {
                return nester.apply( surfaceItem );
            }
        };
    }

    public static abstract class NestedIterator<FROM,TO> extends BaseIterator<TO>
    {
        private final Iterator<FROM> source;
        private Iterator<TO> currentNestedIterator;
        private FROM currentSurfaceItem;

        public NestedIterator( Iterator<FROM> source )
        {
            this.source = source;
        }

        protected abstract Iterator<TO> nested( FROM item );

        public FROM getCurrentSurfaceItem()
        {
            if ( this.currentSurfaceItem == null )
            {
                throw new IllegalStateException( "Has no surface item right now," +
                    " you must do at least one next() first" );
            }
            return this.currentSurfaceItem;
        }

        @Override
        protected TO fetchNextOrNull()
        {
            if ( currentNestedIterator == null || !currentNestedIterator.hasNext() )
            {
                while ( source.hasNext() )
                {
                    currentSurfaceItem = source.next();
                    currentNestedIterator = nested( currentSurfaceItem );
                    if ( currentNestedIterator.hasNext() )
                    {
                        break;
                    }
                }
            }
            return currentNestedIterator != null && currentNestedIterator.hasNext() ?
                    currentNestedIterator.next() : null;
        }
    }

    // Paging
    public static <T> PagingIterator<T> page( Iterator<T> source, int pageSize )
    {
        return new PagingIterator<>( source, pageSize );
    }

    /**
     * A {@link CachingIterator} which can more easily divide the items
     * into pages, where optionally each page can be seen as its own
     * {@link Iterator} instance for convenience using {@link #nextPage()}.
     *
     * @param <T> the type of items in this iterator.
     */
    public static class PagingIterator<T> extends CachingIterator<T>
    {
        private final int pageSize;

        /**
         * Creates a new paging iterator with {@code source} as its underlying
         * {@link Iterator} to lazily get items from.
         *
         * @param source the underlying {@link Iterator} to lazily get items from.
         * @param pageSize the max number of items in each page.
         */
        public PagingIterator( Iterator<T> source, int pageSize )
        {
            super( source );
            this.pageSize = pageSize;
        }

        /**
         * @return the page the iterator is currently at, starting a {@code 0}.
         * This value is based on the {@link #position()} and the page size.
         */
        public int page()
        {
            return position()/pageSize;
        }

        /**
         * Sets the current page of the iterator. {@code 0} means the first page.
         * @param newPage the current page to set for the iterator, must be
         * non-negative. The next item returned by the iterator will be the first
         * item in that page.
         * @return the page before changing to the new page.
         */
        public int page( int newPage )
        {
            int previousPage = page();
            position( newPage*pageSize );
            return previousPage;
        }

        /**
         * Returns a new {@link Iterator} instance which exposes the current page
         * as its own iterator, which fetches items lazily from the underlying
         * iterator. It is discouraged to use an {@link Iterator} returned from
         * this method at the same time as using methods like {@link #next()} or
         * {@link #previous()}, where the results may be unpredictable. So either
         * use only {@link #nextPage()} (in conjunction with {@link #page(int)} if
         * necessary) or go with regular {@link #next()}/{@link #previous()}.
         *
         * @return the next page as an {@link Iterator}.
         */
        public Iterator<T> nextPage()
        {
            page( page() );
            return new BaseIterator<T>()
            {
                private final int end = position()+pageSize;

                @Override
                protected T fetchNextOrNull()
                {
                    if ( position() >= end )
                    {
                        return null;
                    }
                    return PagingIterator.this.hasNext() ? PagingIterator.this.next() : null;
                }
            };
        }
    }

    // Range
    public static Iterator<Integer> intRange( int end )
    {
        return intRange( 0, end );
    }

    public static Iterator<Integer> intRange( int start, int end )
    {
        return intRange( start, end, 1 );
    }

    public static Iterator<Integer> intRange( int start, int end, int stride )
    {
        return new RangeIterator<>( start, end, stride, INTEGER_STRIDER, INTEGER_COMPARATOR );
    }

    public static class RangeIterator<T,S> extends BaseIterator<T>
    {
        private T current;
        private final T end;
        private final S stride;
        private final Function2<T, S, T> strider;
        private final Comparator<T> comparator;

        public RangeIterator( T start, T end, S stride, Function2<T, S, T> strider, Comparator<T> comparator )
        {
            this.current = start;
            this.end = end;
            this.stride = stride;
            this.strider = strider;
            this.comparator = comparator;
        }

        @Override
        protected T fetchNextOrNull()
        {
            try
            {
                return comparator.compare( current, end ) <= 0 ? current : null;
            }
            finally
            {
                current = strider.apply( current, stride );
            }
        }
    }

    static final Function2<Integer,Integer,Integer> INTEGER_STRIDER =
            new Function2<Integer, Integer, Integer>()
    {
        @Override
        public Integer apply( Integer from1, Integer from2 )
        {
            return from1 + from2;
        }
    };

    static final Comparator<Integer> INTEGER_COMPARATOR = new Comparator<Integer>()
    {
        @Override
        public int compare( Integer o1, Integer o2 )
        {
            return o1.intValue() - o2.intValue();
        }
    };

    // Singleton
    public static <T> Iterator<T> singleton( final T item )
    {
        return new BaseIterator<T>()
        {
            private T itemToReturn = item;

            @Override
            protected T fetchNextOrNull()
            {
                try
                {
                    return itemToReturn;
                }
                finally
                {
                    itemToReturn = null;
                }
            }
        };
    }

    // Cloning
    public static <T extends CloneableInPublic> Iterator<T> cloning( Iterator<T> items, final Class<T> itemClass )
    {
        return new MappingIterator<T,T>( items )
        {
            @Override
            protected T map( T item )
            {
                return itemClass.cast( item.clone() );
            }
        };
    }

    // Reversed
    public static <T> Iterator<T> reversed( Iterator<T> source )
    {
        List<T> allItems = asList( source );
        Collections.reverse( allItems );
        return allItems.iterator();
    }

    // === Operations ===
    /**
     * Returns the given iterator's first element. If no element is found a
     * {@link NoSuchElementException} is thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}, or {@code null} if no
     * element found.
     */
    public static <T> T first( Iterator<T> iterator )
    {
        return assertNotNull( iterator, first( iterator, null ) );
    }

    /**
     * Returns the given iterator's first element or {@code defaultItem} if no
     * element found.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the first element in the {@code iterator}.
     * @throws NoSuchElementException if no element found.
     */
    public static <T> T first( Iterator<T> iterator, T defaultItem )
    {
        return iterator.hasNext() ? iterator.next() : defaultItem;
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
        return assertNotNull( iterator, last( iterator, null ) );
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
    public static <T> T last( Iterator<T> iterator, T defaultItem )
    {
        T result = defaultItem;
        while ( iterator.hasNext() )
        {
            result = iterator.next();
        }
        return result;
    }

    /**
     * Returns the given iterator's single element. If there are no elements
     * or more than one element in the iterator a {@link NoSuchElementException}
     * will be thrown.
     *
     * @param <T> the type of elements in {@code iterator}.
     * @param iterator the {@link Iterator} to get elements from.
     * @return the single element in the {@code iterator}.
     * @throws NoSuchElementException if there isn't exactly one element.
     */
    public static <T> T single( Iterator<T> iterator )
    {
        return assertNotNull( iterator, single( iterator, null ) );
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
     * @throws NoSuchElementException if more than one element was found.
     */
    public static <T> T single( Iterator<T> iterator, T defaultItem )
    {
        if ( !iterator.hasNext() )
        {
            return defaultItem;
        }
        T item = iterator.next();
        if ( iterator.hasNext() )
        {
            throw new NoSuchElementException( "More than one item in " + iterator + ", first:" + item +
                    ", second:" + iterator.next() );
        }
        return item;
    }

    public static <T> T itemAt( Iterator<T> iterator, int index )
    {
        return assertNotNull( iterator, itemAt( iterator, index, null ) );
    }

    public static <T> T itemAt( Iterator<T> iterator, int index, T defaultItem )
    {
        if ( index >= 0 )
        {   // Look forwards
            for ( int i = 0; iterator.hasNext() && i < index; i++ )
            {
                iterator.next();
            }
            return iterator.hasNext() ? iterator.next() : defaultItem;
        }

        // Look backwards
        int fromEnd = index * -1;
        Deque<T> trail = new ArrayDeque<>( fromEnd );
        while ( iterator.hasNext() )
        {
            if ( trail.size() >= fromEnd )
            {
                trail.removeLast();
            }
            trail.addFirst( iterator.next() );
        }
        return trail.size() == fromEnd ? trail.getLast() : defaultItem;
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static <T> int indexOf( T item, Iterator<T> iterator )
    {
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            if ( item.equals( iterator.next() ) )
            {
                return i;
            }
        }
        return -1;
    }

    /**
     * Validates whether two {@link Iterator}s are equal or not, i.e. if they have contain same number of items
     * and each orderly item equals one another.
     *
     * @param first the {@link Iterator} containing the first items.
     * @param other the {@link Iterator} containing the other items.
     * @return whether the two iterators are equal or not.
     */
    public static boolean equals( Iterator<?> first, Iterator<?> other )
    {
        boolean firstHasNext, otherHasNext;
        // single | so that both iterator's hasNext() gets evaluated.
        while ( (firstHasNext = first.hasNext()) | (otherHasNext = other.hasNext()) )
        {
            if ( firstHasNext != otherHasNext || !first.next().equals( other.next() ) )
            {
                return false;
            }
        }
        return true;
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
     * Adds all the items in {@code iterator} to {@code collection}.
     * @param <C> the type of {@link Collection} to add to items to.
     * @param <T> the type of items in the collection and iterator.
     * @param iterator the {@link Iterator} to grab the items from.
     * @param collection the {@link Collection} to add the items to.
     * @return the {@code collection} which was passed in, now filled
     * with the items from {@code iterator}.
     */
    public static <C extends Collection<T>,T> C addToCollection( Iterator<T> iterator, C collection,
            Function2<T, C, Void> adder )
    {
        while ( iterator.hasNext() )
        {
            T item = iterator.next();
            adder.apply( item, collection );
        }
        return collection;
    }

    public static <C extends Collection<T>,T> C addToCollection( Iterator<T> iterator, C collection )
    {
        return addToCollection( iterator, collection, NewIterators.<T,C>add() );
    }

    public static <C extends Collection<T>,T> C addToCollectionAssertChanged( Iterator<T> iterator, C collection )
    {
        return addToCollection( iterator, collection, NewIterators.<T,C>addUnique() );
    }

    @SuppressWarnings( "unchecked" )
    private static final <T, C> Function2<T, C, Void> addUnique()
    {
        return ADD_UNIQUE;
    }

    public static class DuplicateItemException extends IllegalStateException
    {
        public DuplicateItemException( String message )
        {
            super( message );
        }
    }

    @SuppressWarnings( "rawtypes" )
    private static final Function2 ADD_UNIQUE = new Function2()
    {
        @SuppressWarnings( { "unchecked" } )
        @Override
        public Object apply( Object item, Object collection )
        {
            if ( !((Collection)collection).add( item ) )
            {
                throw new DuplicateItemException( "Encountered an already added item:" + item +
                        " when adding items uniquely to a collection:" + collection );
            }
            return null;
        }
    };

    @SuppressWarnings( "unchecked" )
    private static final <T, C> Function2<T, C, Void> add()
    {
        return ADD;
    }

    @SuppressWarnings( "rawtypes" )
    private static final Function2 ADD = new Function2()
    {
        @SuppressWarnings( { "unchecked" } )
        @Override
        public Object apply( Object item, Object collection )
        {
            ((Collection)collection).add( item );
            return null;
        }
    };

    public static <T> List<T> asList( Iterator<T> items )
    {
        return addToCollection( items, new ArrayList<T>() );
    }

    public static <T> Set<T> asSet( Iterator<T> items )
    {
        return addToCollectionAssertChanged( items, new HashSet<T>() );
    }

    public static <T> Set<T> asSetAllowDuplicates( Iterator<T> items )
    {
        return addToCollection( items, new HashSet<T>() );
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
     * Counts the number of items in the {@code iterator} by looping
     * through it.
     * @param <T> the type of items in the iterator.
     * @param iterator the {@link Iterator} to count items in.
     * @return the number of found in {@code iterator}.
     */
    public static int count( Iterator<?> iterator )
    {
        int count = 0;
        for ( ; iterator.hasNext(); iterator.next(), count++ )
        {   // Just loop through this
        }
        return count;
    }

    /**
     * Returns all items in {@code items} as an array.
     * @param items the {@link Iterator} to get the items from.
     * @param itemClass type of items in the iterator, and hence in the resulting array.
     * @return an array of all items in the iterator.
     */
    @SuppressWarnings( "unchecked" )
    public static <T> T[] asArray( Iterator<T> items, Class<T> itemClass )
    {
        List<T> allItems = asList( items );
        return allItems.toArray( (T[]) Array.newInstance( itemClass, allItems.size() ) );
    }
}
