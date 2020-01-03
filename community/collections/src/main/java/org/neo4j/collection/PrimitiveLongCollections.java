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
package org.neo4j.collection;

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;

import org.neo4j.graphdb.Resource;

import static java.util.Arrays.copyOf;

/**
 * Basic and common primitive int collection utils and manipulations.
 */
public class PrimitiveLongCollections
{
    public static final long[] EMPTY_LONG_ARRAY = new long[0];

    private PrimitiveLongCollections()
    {
        // nop
    }

    public static LongIterator iterator( final long... items )
    {
        return new PrimitiveLongResourceCollections.PrimitiveLongBaseResourceIterator( Resource.EMPTY )
        {
            private int index = -1;

            @Override
            protected boolean fetchNext()
            {
                return ++index < items.length && next( items[index] );
            }
        };
    }

    // Concating
    public static LongIterator concat( LongIterator... longIterators )
    {
        return concat( Arrays.asList( longIterators ) );
    }

    public static LongIterator concat( Iterable<LongIterator> primitiveLongIterators )
    {
        return new PrimitiveLongConcatingIterator( primitiveLongIterators.iterator() );
    }

    public static LongIterator filter( LongIterator source, final LongPredicate filter )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean test( long item )
            {
                return filter.test( item );
            }
        };
    }

    // Range
    public static LongIterator range( long start, long end )
    {
        return new PrimitiveLongRangeIterator( start, end );
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static int indexOf( LongIterator iterator, long item )
    {
        for ( int i = 0; iterator.hasNext(); i++ )
        {
            if ( item == iterator.next() )
            {
                return i;
            }
        }
        return -1;
    }

    public static MutableLongSet asSet( Collection<Long> collection )
    {
        final MutableLongSet set = new LongHashSet( collection.size() );
        for ( Long next : collection )
        {
            set.add( next );
        }
        return set;
    }

    public static MutableLongSet asSet( LongIterator iterator )
    {
        MutableLongSet set = new LongHashSet();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    public static int count( LongIterator iterator )
    {
        int count = 0;
        for ( ; iterator.hasNext(); iterator.next(), count++ )
        {   // Just loop through this
        }
        return count;
    }

    public static long[] asArray( LongIterator iterator )
    {
        long[] array = new long[8];
        int i = 0;
        for ( ; iterator.hasNext(); i++ )
        {
            if ( i >= array.length )
            {
                array = copyOf( array, i << 1 );
            }
            array[i] = iterator.next();
        }

        if ( i < array.length )
        {
            array = copyOf( array, i );
        }
        return array;
    }

    public static long[] asArray( Iterator<Long> iterator )
    {
        long[] array = new long[8];
        int i = 0;
        for ( ; iterator.hasNext(); i++ )
        {
            if ( i >= array.length )
            {
                array = copyOf( array, i << 1 );
            }
            array[i] = iterator.next();
        }

        if ( i < array.length )
        {
            array = copyOf( array, i );
        }
        return array;
    }

    public static LongIterator toPrimitiveIterator( final Iterator<Long> iterator )
    {
        return new PrimitiveLongBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                if ( iterator.hasNext() )
                {
                    Long nextValue = iterator.next();
                    if ( null == nextValue )
                    {
                        throw new IllegalArgumentException( "Cannot convert null Long to primitive long" );
                    }
                    return next( nextValue.longValue() );
                }
                return false;
            }
        };
    }

    public static <T> Iterator<T> map( final LongFunction<T> mapFunction, final LongIterator source )
    {
        return new Iterator<T>()
        {
            @Override
            public boolean hasNext()
            {
                return source.hasNext();
            }

            @Override
            public T next()
            {
                return mapFunction.apply( source.next() );
            }

            @Override
            public void remove()
            {
                throw new UnsupportedOperationException();
            }
        };
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link List}, boxing each long.
     *
     * @param iterator {@link LongIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Long> asList( LongIterator iterator )
    {
        List<Long> out = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            out.add( iterator.next() );
        }
        return out;
    }

    public static Iterator<Long> toIterator( final LongIterator primIterator )
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

    /**
     * Wraps a {@link LongIterator} in a {@link PrimitiveLongResourceIterator} which closes
     * the provided {@code resource} in {@link PrimitiveLongResourceIterator#close()}.
     *
     * @param iterator {@link LongIterator} to convert
     * @param resource {@link Resource} to close in {@link PrimitiveLongResourceIterator#close()}
     * @return Wrapped {@link LongIterator}.
     */
    public static PrimitiveLongResourceIterator resourceIterator( final LongIterator iterator,
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

    /**
     * Convert primitive set into a plain old java {@link Set}, boxing each long.
     *
     * @param set {@link LongSet} set of primitive values.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet( LongSet set )
    {
        return toSet( set.longIterator() );
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link Set}, boxing each long.
     *
     * @param iterator {@link LongIterator} to pull values from.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet( LongIterator iterator )
    {
        Set<Long> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            addUnique( set, iterator.next() );
        }
        return set;
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
     * Deduplicates values in the sorted {@code values} array.
     *
     * @param values sorted array of long values.
     * @return the provided array if no duplicates were found, otherwise a new shorter array w/o duplicates.
     */
    public static long[] deduplicate( long[] values )
    {
        if ( values.length < 2 )
        {
            return values;
        }
        long lastValue = values[0];
        int uniqueIndex = 1;
        for ( int i = 1; i < values.length; i++ )
        {
            long currentValue = values[i];
            if ( currentValue != lastValue )
            {
                values[uniqueIndex] = currentValue;
                lastValue = currentValue;
                uniqueIndex++;
            }
        }
        return uniqueIndex < values.length ? Arrays.copyOf( values, uniqueIndex ) : values;
    }

    /**
     * Base iterator for simpler implementations of {@link LongIterator}s.
     */
    public abstract static class PrimitiveLongBaseIterator implements LongIterator
    {
        private boolean hasNextDecided;
        private boolean hasNext;
        protected long next;

        @Override
        public boolean hasNext()
        {
            if ( !hasNextDecided )
            {
                hasNext = fetchNext();
                hasNextDecided = true;
            }
            return hasNext;
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException( "No more elements in " + this );
            }
            hasNextDecided = false;
            return next;
        }

        /**
         * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
         * item was found, that value must have been set inside the implementation of this method
         * using {@link #next(long)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
         * This method returns {@code true} so that it can be used in short-hand conditionals
         * (TODO what are they called?), like:
         * <pre>
         * protected boolean fetchNext()
         * {
         *     return source.hasNext() ? next( source.next() ) : false;
         * }
         * </pre>
         * @param nextItem the next item found.
         */
        protected boolean next( long nextItem )
        {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static class PrimitiveLongConcatingIterator extends PrimitiveLongBaseIterator
    {
        private final Iterator<? extends LongIterator> iterators;
        private LongIterator currentIterator;

        public PrimitiveLongConcatingIterator( Iterator<? extends LongIterator> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext()
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
            return (currentIterator != null && currentIterator.hasNext()) && next( currentIterator.next() );
        }

        protected final LongIterator currentIterator()
        {
            return currentIterator;
        }
    }

    public abstract static class PrimitiveLongFilteringIterator extends PrimitiveLongBaseIterator
            implements LongPredicate
    {
        protected final LongIterator source;

        PrimitiveLongFilteringIterator( LongIterator source )
        {
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                long testItem = source.next();
                if ( test( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        @Override
        public abstract boolean test( long testItem );
    }

    public static class PrimitiveLongRangeIterator extends PrimitiveLongBaseIterator
    {
        private long current;
        private final long end;

        PrimitiveLongRangeIterator( long start, long end )
        {
            this.current = start;
            this.end = end;
        }

        @Override
        protected boolean fetchNext()
        {
            try
            {
                return current <= end && next( current );
            }
            finally
            {
                current++;
            }
        }
    }

    public static MutableLongSet mergeToSet( LongIterable a, LongIterable b )
    {
        final MutableLongSet set = new LongHashSet( a.size() + b.size() );
        set.addAll( a );
        set.addAll( b );
        return set;
    }
}
