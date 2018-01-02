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

import java.util.Collection;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.collection.primitive.base.Empty;
import org.neo4j.function.IntPredicate;
import org.neo4j.function.IntPredicates;
import org.neo4j.function.primitive.FunctionFromPrimitiveInt;
import org.neo4j.function.primitive.PrimitiveIntPredicate;

import static java.util.Arrays.copyOf;

import static org.neo4j.collection.primitive.PrimitiveCommons.closeSafely;

/**
 * Basic and common primitive int collection utils and manipulations.
 *
 * @see PrimitiveLongCollections
 * @see Primitive
 */
public class PrimitiveIntCollections
{
    /**
     * Base iterator for simpler implementations of {@link PrimitiveIntIterator}s.
     */
    public static abstract class PrimitiveIntBaseIterator implements PrimitiveIntIterator
    {
        private boolean hasNext;
        private int next;

        @Override
        public boolean hasNext()
        {
            return hasNext ? true : (hasNext = fetchNext());
        }

        @Override
        public int next()
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
         * using {@link #next(int)}.
         */
        protected abstract boolean fetchNext();

        /**
         * Called from inside an implementation of {@link #fetchNext()} if a next item was found.
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
         */
        protected boolean next( int nextItem )
        {
            next = nextItem;
            hasNext = true;
            return true;
        }
    }

    public static PrimitiveIntIterator iterator( final int... items )
    {
        return new PrimitiveIntBaseIterator()
        {
            private int index = -1;

            @Override
            protected boolean fetchNext()
            {
                return ++index < items.length ? next( items[index] ) : false;
            }
        };
    }

    public static PrimitiveIntIterator reversed( final int... items )
    {
        return new PrimitiveIntBaseIterator()
        {
            private int index = items.length;

            @Override
            protected boolean fetchNext()
            {
                return --index >= 0 ? next( items[index] ) : false;
            }
        };
    }

    public static PrimitiveIntIterator reversed( PrimitiveIntIterator source )
    {
        int[] items = asArray( source );
        return reversed( items );
    }

    // Concating
    public static PrimitiveIntIterator concat( Iterator<PrimitiveIntIterator> iterators )
    {
        return new PrimitiveIntConcatingIterator( iterators );
    }

    public static PrimitiveIntIterator prepend( final int item, final PrimitiveIntIterator iterator )
    {
        return new PrimitiveIntBaseIterator()
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

    public static PrimitiveIntIterator append( final PrimitiveIntIterator iterator, final int item )
    {
        return new PrimitiveIntBaseIterator()
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

    public static class PrimitiveIntConcatingIterator extends PrimitiveIntBaseIterator
    {
        private final Iterator<PrimitiveIntIterator> iterators;
        private PrimitiveIntIterator currentIterator;

        public PrimitiveIntConcatingIterator( Iterator<PrimitiveIntIterator> iterators )
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

        protected final PrimitiveIntIterator currentIterator()
        {
            return currentIterator;
        }
    }

    // Interleave
    public static class PrimitiveIntInterleavingIterator extends PrimitiveIntBaseIterator
    {
        private final Iterable<PrimitiveIntIterator> iterators;
        private Iterator<PrimitiveIntIterator> currentRound;

        public PrimitiveIntInterleavingIterator( Iterable<PrimitiveIntIterator> iterators )
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
                PrimitiveIntIterator iterator = currentRound.next();
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
     * @deprecated use {@link #filter(PrimitiveIntIterator, IntPredicate)} instead
     */
    @Deprecated
    public static PrimitiveIntIterator filter( PrimitiveIntIterator source, final PrimitiveIntPredicate filter )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            @Override
            public boolean accept( int item )
            {
                return filter.accept( item );
            }
        };
    }

    public static PrimitiveIntIterator filter( PrimitiveIntIterator source, final IntPredicate filter )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            @Override
            public boolean accept( int item )
            {
                return filter.test( item );
            }
        };
    }

    public static PrimitiveIntIterator dedup( PrimitiveIntIterator source )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            private final PrimitiveIntSet visited = Primitive.intSet();

            @Override
            public boolean accept( int testItem )
            {
                return visited.add( testItem );
            }
        };
    }

    public static PrimitiveIntIterator not( PrimitiveIntIterator source, final int disallowedValue )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            @Override
            public boolean accept( int testItem )
            {
                return testItem != disallowedValue;
            }
        };
    }

    public static PrimitiveIntIterator skip( PrimitiveIntIterator source, final int skipTheFirstNItems )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            private int skipped = 0;

            @Override
            public boolean accept( int item )
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

    public static abstract class PrimitiveIntFilteringIterator extends PrimitiveIntBaseIterator
            implements PrimitiveIntPredicate
    {
        private final PrimitiveIntIterator source;

        public PrimitiveIntFilteringIterator( PrimitiveIntIterator source )
        {
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                int testItem = source.next();
                if ( accept( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        /**
         * @deprecated use {@link IntPredicate} instead
         */
        @Deprecated
        @Override
        public abstract boolean accept( int testItem );
    }

    // Limitinglic
    public static PrimitiveIntIterator limit( final PrimitiveIntIterator source, final int maxItems )
    {
        return new PrimitiveIntBaseIterator()
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
    public static PrimitiveIntIterator range( int end )
    {
        return range( 0, end );
    }

    public static PrimitiveIntIterator range( int start, int end )
    {
        return range( start, end, 1 );
    }

    public static PrimitiveIntIterator range( int start, int end, int stride )
    {
        return new PrimitiveIntRangeIterator( start, end, stride );
    }

    public static class PrimitiveIntRangeIterator extends PrimitiveIntBaseIterator
    {
        private int current;
        private final int end;
        private final int stride;

        public PrimitiveIntRangeIterator( int start, int end, int stride )
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

    public static PrimitiveIntIterator singleton( final int item )
    {
        return new PrimitiveIntBaseIterator()
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

    public static int first( PrimitiveIntIterator iterator )
    {
        assertMoreItems( iterator );
        return iterator.next();
    }

    private static void assertMoreItems( PrimitiveIntIterator iterator )
    {
        if ( !iterator.hasNext() )
        {
            throw new NoSuchElementException( "No element in " + iterator );
        }
    }

    public static int first( PrimitiveIntIterator iterator, int defaultItem )
    {
        return iterator.hasNext() ? iterator.next() : defaultItem;
    }

    public static int last( PrimitiveIntIterator iterator )
    {
        assertMoreItems( iterator );
        return last( iterator, 0 /*will never be used*/ );
    }

    public static int last( PrimitiveIntIterator iterator, int defaultItem )
    {
        int result = defaultItem;
        while ( iterator.hasNext() )
        {
            result = iterator.next();
        }
        return result;
    }

    public static int single( PrimitiveIntIterator iterator )
    {
        try
        {
            assertMoreItems( iterator );
            int item = iterator.next();
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

    public static int single( PrimitiveIntIterator iterator, int defaultItem )
    {
        try
        {
            if ( !iterator.hasNext() )
            {
                closeSafely( iterator );
                return defaultItem;
            }
            int item = iterator.next();
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

    public static int itemAt( PrimitiveIntIterator iterator, int index )
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
        int[] trail = new int[fromEnd];
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

    public static int itemAt( PrimitiveIntIterator iterator, int index, int defaultItem )
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
        int[] trail = new int[fromEnd];
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
    public static int indexOf( PrimitiveIntIterator iterator, int item )
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
    public static boolean equals( PrimitiveIntIterator first, PrimitiveIntIterator other )
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

    public static PrimitiveIntSet asSet( PrimitiveIntIterator iterator )
    {
        PrimitiveIntSet set = Primitive.intSet();
        while ( iterator.hasNext() )
        {
            int next = iterator.next();
            if ( !set.add( next ) )
            {
                throw new IllegalStateException( "Duplicate " + next + " from " + iterator );
            }
        }
        return set;
    }

    public static PrimitiveIntSet asSetAllowDuplicates( PrimitiveIntIterator iterator )
    {
        PrimitiveIntSet set = Primitive.intSet();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    public static int count( PrimitiveIntIterator iterator )
    {
        int count = 0;
        for ( ; iterator.hasNext(); iterator.next(), count++ )
        {   // Just loop through this
        }
        return count;
    }

    public static int[] asArray( PrimitiveIntIterator iterator )
    {
        int[] array = new int[8];
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

    public static int[] asArray( Collection<Integer> values )
    {
        int[] array = new int[values.size()];
        int i = 0;
        for ( int value : values )
        {
            array[i++] = value;
        }
        return array;
    }

    private static final PrimitiveIntIterator EMPTY = new PrimitiveIntBaseIterator()
    {
        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    };

    public static PrimitiveIntIterator emptyIterator()
    {
        return EMPTY;
    }

    public static PrimitiveIntSet emptySet()
    {
        return Empty.EMPTY_PRIMITIVE_INT_SET;
    }

    public static PrimitiveIntIterator toPrimitiveIterator( final Iterator<Integer> iterator )
    {
        return new PrimitiveIntBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                if ( iterator.hasNext() )
                {
                    Integer nextValue = iterator.next();
                    if ( null == nextValue )
                    {
                        throw new IllegalArgumentException( "Cannot convert null Integer to primitive int" );
                    }
                    return next( nextValue.intValue() );
                }
                return false;
            }
        };
    }

    public static PrimitiveIntIterator flatten( final Iterator<PrimitiveIntIterator> source )
    {
        return new PrimitiveIntBaseIterator()
        {
            private PrimitiveIntIterator current;

            @Override
            protected boolean fetchNext()
            {
                while ( current == null || !current.hasNext() )
                {
                    if ( !source.hasNext() )
                    {
                        return false;
                    }
                    current = source.next();
                }
                return source.hasNext() ? next( current.next() ) : false;
            }
        };
    }

    public static <T> Iterator<T> map( final FunctionFromPrimitiveInt<T> mapFunction,
            final PrimitiveIntIterator source )
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

    private static final PrimitiveIntPredicate TRUE = new PrimitiveIntPredicate()
    {
        @Override
        public boolean accept( int value )
        {
            return true;
        }
    };

    /**
     * @deprecated use {@link IntPredicates#alwaysTrue()} instead
     */
    @Deprecated
    public static PrimitiveIntPredicate alwaysTrue()
    {
        return TRUE;
    }

    public static PrimitiveIntIterator constant( final int value )
    {
        return new PrimitiveIntBaseIterator()
        {
            @Override
            protected boolean fetchNext()
            {
                return next( value );
            }
        };
    }

    public static PrimitiveIntSet asSet( int[] values )
    {
        PrimitiveIntSet set = Primitive.intSet( values.length );
        for ( int value : values )
        {
            set.add( value );
        }
        return set;
    }
}
