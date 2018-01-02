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
package org.neo4j.collection.primitive;

import java.util.Iterator;
import java.util.NoSuchElementException;
import java.util.Objects;

import org.neo4j.collection.primitive.base.Empty;
import org.neo4j.function.LongPredicate;
import org.neo4j.function.primitive.FunctionFromPrimitiveLong;
import org.neo4j.function.primitive.PrimitiveLongPredicate;

import static java.util.Arrays.copyOf;

import static org.neo4j.collection.primitive.PrimitiveCommons.closeSafely;

/**
 * Basic and common primitive int collection utils and manipulations.
 *
 * @see PrimitiveIntCollections
 * @see Primitive
 */
public class PrimitiveLongCollections
{
    public static final long[] EMPTY_LONG_ARRAY = new long[0];

    /**
     * Base iterator for simpler implementations of {@link PrimitiveLongIterator}s.
     */
    public static abstract class PrimitiveLongBaseIterator implements PrimitiveLongIterator
    {
        private boolean hasNext;
        protected long next;

        @Override
        public boolean hasNext()
        {
            return hasNext ? true : (hasNext = fetchNext());
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException( "No more elements in " + this );
            }
            hasNext = false;
            return next;
        }

        /**
         * Fetches the next item in this iterator. Returns whether or not a next item was found. If a next
         * item was found, that value must have been set inside the implementation of this method
         * using {@link #next(long)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #computeNext()} if a next item was found.
         * This method returns {@code true} so that it can be used in short-hand conditionals
         * (TODO what are they called?), like:
         * <pre>
         * @Override
         * protected boolean fetchNext()
         * {
         *     return source.hasNext() ? next( source.next() ) : false;
         * }
         * </pre>
         * @param nextItem the next item found.
         * @see #end()
         */
        protected boolean next( long nextItem )
        {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static PrimitiveLongIterator iterator( final long... items )
    {
        return new PrimitiveLongBaseIterator()
        {
            private int index = -1;

            @Override
            protected boolean fetchNext()
            {
                return ++index < items.length ? next( items[index] ) : false;
            }
        };
    }

    public static PrimitiveLongIterator reversed( final long... items )
    {
        return new PrimitiveLongBaseIterator()
        {
            private int index = items.length;

            @Override
            protected boolean fetchNext()
            {
                return --index >= 0 ? next( items[index] ) : false;
            }
        };
    }

    public static PrimitiveLongIterator reversed( PrimitiveLongIterator source )
    {
        long[] items = asArray( source );
        return reversed( items );
    }

    // Concating
    public static PrimitiveLongIterator concat( Iterator<PrimitiveLongIterator> iterators )
    {
        return new PrimitiveLongConcatingIterator( iterators );
    }

    public static PrimitiveLongIterator prepend( final long item, final PrimitiveLongIterator iterator )
    {
        return new PrimitiveLongBaseIterator()
        {
            private boolean singleItemReturned;

            @Override
            protected boolean fetchNext()
            {
                if ( !singleItemReturned )
                {
                    singleItemReturned = true;
                    return next( item );
                }
                return iterator.hasNext() ? next( iterator.next() ) : false;
            }
        };
    }

    public static PrimitiveLongIterator append( final PrimitiveLongIterator iterator, final long item )
    {
        return new PrimitiveLongBaseIterator()
        {
            private boolean singleItemReturned;

            @Override
            protected boolean fetchNext()
            {
                if ( iterator.hasNext() )
                {
                    return next( iterator.next() );
                }
                else if ( !singleItemReturned )
                {
                    singleItemReturned = true;
                    return next( item );
                }
                return false;
            }
        };
    }

    public static class PrimitiveLongConcatingIterator extends PrimitiveLongBaseIterator
    {
        private final Iterator<? extends PrimitiveLongIterator> iterators;
        private PrimitiveLongIterator currentIterator;

        public PrimitiveLongConcatingIterator( Iterator<? extends PrimitiveLongIterator> iterators )
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
            return currentIterator != null && currentIterator.hasNext() ? next( currentIterator.next() ) : false;
        }

        protected final PrimitiveLongIterator currentIterator()
        {
            return currentIterator;
        }
    }

    // Interleave
    public static class PrimitiveLongInterleavingIterator extends PrimitiveLongBaseIterator
    {
        private final Iterable<PrimitiveLongIterator> iterators;
        private Iterator<PrimitiveLongIterator> currentRound;

        public PrimitiveLongInterleavingIterator( Iterable<PrimitiveLongIterator> iterators )
        {
            this.iterators = iterators;
        }

        @Override
        protected boolean fetchNext()
        {
            if ( currentRound == null || !currentRound.hasNext() )
            {
                currentRound = iterators.iterator();
            }
            while ( currentRound.hasNext() )
            {
                PrimitiveLongIterator iterator = currentRound.next();
                if ( iterator.hasNext() )
                {
                    return next( iterator.next() );
                }
            }
            currentRound = null;
            return false;
        }
    }

    /**
     * @deprecated use {@link #filter(PrimitiveLongIterator, LongPredicate)} instead
     */
    @Deprecated
    public static PrimitiveLongIterator filter( PrimitiveLongIterator source, final PrimitiveLongPredicate filter )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean accept( long item )
            {
                return filter.accept( item );
            }
        };
    }

    public static PrimitiveLongIterator filter( PrimitiveLongIterator source, final LongPredicate filter )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean accept( long item )
            {
                return filter.test( item );
            }
        };
    }

    public static PrimitiveLongIterator dedup( PrimitiveLongIterator source )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            private final PrimitiveLongSet visited = Primitive.longSet();

            @Override
            public boolean accept( long testItem )
            {
                return visited.add( testItem );
            }
        };
    }

    public static PrimitiveLongIterator not( PrimitiveLongIterator source, final long disallowedValue )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            @Override
            public boolean accept( long testItem )
            {
                return testItem != disallowedValue;
            }
        };
    }

    public static PrimitiveLongIterator skip( PrimitiveLongIterator source, final int skipTheFirstNItems )
    {
        return new PrimitiveLongFilteringIterator( source )
        {
            private int skipped = 0;

            @Override
            public boolean accept( long item )
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

    public static abstract class PrimitiveLongFilteringIterator extends PrimitiveLongBaseIterator
            implements PrimitiveLongPredicate
    {
        private final PrimitiveLongIterator source;

        public PrimitiveLongFilteringIterator( PrimitiveLongIterator source )
        {
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                long testItem = source.next();
                if ( accept( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        /**
         * @deprecated use {@link LongPredicate} instead
         */
        @Deprecated
        @Override
        public abstract boolean accept( long testItem );
    }

    // Limitinglic
    public static PrimitiveLongIterator limit( final PrimitiveLongIterator source, final int maxItems )
    {
        return new PrimitiveLongBaseIterator()
        {
            private int visited;

            @Override
            protected boolean fetchNext()
            {
                if ( visited++ < maxItems )
                {
                    if ( source.hasNext() )
                    {
                        return next( source.next() );
                    }
                }
                return false;
            }
        };
    }

    // Range
    public static PrimitiveLongIterator range( long end )
    {
        return range( 0, end );
    }

    public static PrimitiveLongIterator range( long start, long end )
    {
        return range( start, end, 1 );
    }

    public static PrimitiveLongIterator range( long start, long end, long stride )
    {
        return new PrimitiveLongRangeIterator( start, end, stride );
    }

    public static class PrimitiveLongRangeIterator extends PrimitiveLongBaseIterator
    {
        private long current;
        private final long end;
        private final long stride;

        public PrimitiveLongRangeIterator( long start, long end, long stride )
        {
            this.current = start;
            this.end = end;
            this.stride = stride;
        }

        @Override
        protected boolean fetchNext()
        {
            try
            {
                return current <= end ? next( current ) : false;
            }
            finally
            {
                current += stride;
            }
        }
    }

    public static PrimitiveLongIterator singleton( final long item )
    {
        return new PrimitiveLongBaseIterator()
        {
            private boolean returned;

            @Override
            protected boolean fetchNext()
            {
                try
                {
                    return !returned ? next( item ) : false;
                }
                finally
                {
                    returned = true;
                }
            }
        };
    }

    public static long first( PrimitiveLongIterator iterator )
    {
        assertMoreItems( iterator );
        return iterator.next();
    }

    private static void assertMoreItems( PrimitiveLongIterator iterator )
    {
        if ( !iterator.hasNext() )
        {
            throw new NoSuchElementException( "No element in " + iterator );
        }
    }

    public static long first( PrimitiveLongIterator iterator, long defaultItem )
    {
        return iterator.hasNext() ? iterator.next() : defaultItem;
    }

    public static long last( PrimitiveLongIterator iterator )
    {
        assertMoreItems( iterator );
        return last( iterator, 0 /*will never be used*/ );
    }

    public static long last( PrimitiveLongIterator iterator, long defaultItem )
    {
        long result = defaultItem;
        while ( iterator.hasNext() )
        {
            result = iterator.next();
        }
        return result;
    }

    public static long single( PrimitiveLongIterator iterator )
    {
        try
        {
            assertMoreItems( iterator );
            long item = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one item in " + iterator + ", first:" + item +
                        ", second:" + iterator.next() );
            }
            closeSafely( iterator );
            return item;
        }
        catch ( NoSuchElementException exception )
        {
            closeSafely( iterator, exception );
            throw exception;
        }
    }

    public static long single( PrimitiveLongIterator iterator, long defaultItem )
    {
        try
        {
            if ( !iterator.hasNext() )
            {
                closeSafely( iterator );
                return defaultItem;
            }
            long item = iterator.next();
            if ( iterator.hasNext() )
            {
                throw new NoSuchElementException( "More than one item in " + iterator + ", first:" + item +
                        ", second:" + iterator.next() );
            }
            closeSafely( iterator );
            return item;
        }
        catch ( NoSuchElementException exception )
        {
            closeSafely( iterator, exception );
            throw exception;
        }
    }

    public static long itemAt( PrimitiveLongIterator iterator, int index )
    {
        if ( index >= 0 )
        {   // Look forwards
            for ( int i = 0; iterator.hasNext() && i < index; i++ )
            {
                iterator.next();
            }
            assertMoreItems( iterator );
            return iterator.next();
        }

        // Look backwards
        int fromEnd = index * -1;
        long[] trail = new long[fromEnd];
        int cursor = 0;
        for ( ; iterator.hasNext(); cursor++ )
        {
            trail[cursor%trail.length] = iterator.next();
        }
        if ( cursor < fromEnd )
        {
            throw new NoSuchElementException( "Item " + index + " not found in " + iterator );
        }
        return trail[cursor%fromEnd];
    }

    public static long itemAt( PrimitiveLongIterator iterator, int index, long defaultItem )
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
        long[] trail = new long[fromEnd];
        int cursor = 0;
        for ( ; iterator.hasNext(); cursor++ )
        {
            trail[cursor%trail.length] = iterator.next();
        }
        return cursor < fromEnd ? defaultItem : trail[cursor%fromEnd];
    }

    /**
     * Returns the index of the given item in the iterator(zero-based). If no items in {@code iterator}
     * equals {@code item} {@code -1} is returned.
     *
     * @param item the item to look for.
     * @param iterator of items.
     * @return index of found item or -1 if not found.
     */
    public static int indexOf( PrimitiveLongIterator iterator, long item )
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

    /**
     * Validates whether two {@link Iterator}s are equal or not, i.e. if they have contain same number of items
     * and each orderly item equals one another.
     *
     * @param first the {@link Iterator} containing the first items.
     * @param other the {@link Iterator} containing the other items.
     * @return whether the two iterators are equal or not.
     */
    public static boolean equals( PrimitiveLongIterator first, PrimitiveLongIterator other )
    {
        boolean firstHasNext, otherHasNext;
        // single | so that both iterator's hasNext() gets evaluated.
        while ( (firstHasNext = first.hasNext()) | (otherHasNext = other.hasNext()) )
        {
            if ( firstHasNext != otherHasNext || first.next() != other.next() )
            {
                return false;
            }
        }
        return true;
    }

    public static PrimitiveLongSet asSet( PrimitiveLongIterator iterator )
    {
        PrimitiveLongSet set = Primitive.longSet();
        while ( iterator.hasNext() )
        {
            long next = iterator.next();
            if ( !set.add( next ) )
            {
                throw new IllegalStateException( "Duplicate " + next + " from " + iterator );
            }
        }
        return set;
    }

    public static PrimitiveLongSet asSetAllowDuplicates( PrimitiveLongIterator iterator )
    {
        PrimitiveLongSet set = Primitive.longSet();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    public static int count( PrimitiveLongIterator iterator )
    {
        int count = 0;
        for ( ; iterator.hasNext(); iterator.next(), count++ )
        {   // Just loop through this
        }
        return count;
    }

    public static long[] asArray( PrimitiveLongIterator iterator )
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

    private static final PrimitiveLongIterator EMPTY = new PrimitiveLongBaseIterator()
    {
        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    };

    public static PrimitiveLongIterator emptyIterator()
    {
        return EMPTY;
    }

    public static PrimitiveLongIterator toPrimitiveIterator( final Iterator<Long> iterator )
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

    public static PrimitiveLongSet emptySet()
    {
        return Empty.EMPTY_PRIMITIVE_LONG_SET;
    }

    public static PrimitiveLongSet setOf( long... values )
    {
        Objects.requireNonNull( values, "Values array is null" );
        PrimitiveLongSet set = Primitive.longSet( values.length );
        for ( long value : values )
        {
            set.add( value );
        }
        return set;
    }

    public static <T> Iterator<T> map( final FunctionFromPrimitiveLong<T> mapFunction,
            final PrimitiveLongIterator source )
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

    public static PrimitiveLongIterator constant( final long value )
    {
        return new PrimitiveLongBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                return next( value );
            }
        };
    }

    @SuppressWarnings( "unchecked" )
    public static <T> PrimitiveLongObjectMap<T> emptyObjectMap()
    {
        return Empty.EMPTY_PRIMITIVE_LONG_OBJECT_MAP;
    }
}
