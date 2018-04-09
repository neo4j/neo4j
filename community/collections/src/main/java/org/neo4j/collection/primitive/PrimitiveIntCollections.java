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

import org.eclipse.collections.api.IntIterable;
import org.eclipse.collections.api.iterator.IntIterator;
import org.eclipse.collections.api.set.primitive.IntSet;
import org.eclipse.collections.api.set.primitive.MutableIntSet;
import org.eclipse.collections.impl.set.mutable.primitive.IntHashSet;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.LongToIntFunction;

/**
 * Basic and common primitive int collection utils and manipulations.
 *
 * @see PrimitiveLongCollections
 */
public class PrimitiveIntCollections
{
    private PrimitiveIntCollections()
    {
    }

    /**
     * Base iterator for simpler implementations of {@link IntIterator}s.
     */
    public abstract static class PrimitiveIntBaseIterator implements IntIterator
    {
        private boolean hasNextDecided;
        private boolean hasNext;
        private int next;

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
        public int next()
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

    public static IntIterator iterator( final int... items )
    {
        return new PrimitiveIntBaseIterator()
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
    public static IntIterator concat( Iterator<IntIterator> iterators )
    {
        return new PrimitiveIntConcatingIterator( iterators );
    }

    public static class PrimitiveIntConcatingIterator extends PrimitiveIntBaseIterator
    {
        private final Iterator<IntIterator> iterators;
        private IntIterator currentIterator;

        public PrimitiveIntConcatingIterator( Iterator<IntIterator> iterators )
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

        protected final IntIterator currentIterator()
        {
            return currentIterator;
        }
    }

    public static IntIterator filter( IntIterator source, final IntPredicate filter )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            @Override
            public boolean test( int item )
            {
                return filter.test( item );
            }
        };
    }

    public static IntIterator deduplicate( IntIterator source )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            private final IntHashSet visited = new IntHashSet();

            @Override
            public boolean test( int testItem )
            {
                return visited.add( testItem );
            }
        };
    }

    public abstract static class PrimitiveIntFilteringIterator extends PrimitiveIntBaseIterator implements IntPredicate
    {
        private final IntIterator source;

        public PrimitiveIntFilteringIterator( IntIterator source )
        {
            this.source = source;
        }

        @Override
        protected boolean fetchNext()
        {
            while ( source.hasNext() )
            {
                int testItem = source.next();
                if ( test( testItem ) )
                {
                    return next( testItem );
                }
            }
            return false;
        }

        @Override
        public abstract boolean test( int testItem );
    }

    public static IntSet asSet( IntIterator iterator )
    {
        final MutableIntSet set = new IntHashSet();
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

    public static long[] asLongArray( IntIterable values )
    {
        long[] array = new long[values.size()];
        final IntIterator iterator = values.intIterator();
        int i = 0;
        while ( iterator.hasNext() )
        {
            array[i++] = iterator.next();
        }
        return array;
    }

    public static IntIterator toPrimitiveIterator( final Iterator<Integer> iterator )
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

    public static void consume( IntIterator source, IntConsumer consumer )
    {
        while ( source.hasNext() )
        {
            consumer.accept( source.next() );
        }
    }

    public static MutableIntSet asSet( long[] values, LongToIntFunction converter )
    {
        MutableIntSet set = new IntHashSet( values.length );
        for ( long value : values )
        {
            set.add( converter.applyAsInt( value ) );
        }
        return set;
    }

    public static boolean contains( int[] values, int candidate )
    {
        for ( int value : values )
        {
            if ( value == candidate )
            {
                return true;
            }
        }
        return false;
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link List}, boxing each int.
     *
     * @param iterator {@link IntIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Integer> toList( IntIterator iterator )
    {
        List<Integer> out = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            out.add( iterator.next() );
        }
        return out;
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link Set}, boxing each int.
     * Any duplicate value will throw {@link IllegalStateException}.
     *
     * @param iterator {@link IntIterator} to pull values from.
     * @return a {@link Set} containing all items.
     * @throws IllegalStateException for the first encountered duplicate.
     */
    public static Set<Integer> toSet( IntIterator iterator )
    {
        return mapToSet( iterator, Integer::new );
    }

    public static <T> Set<T> mapToSet( IntIterator iterator, IntFunction<T> map )
    {
        Set<T> set = new HashSet<>();
        while ( iterator.hasNext() )
        {
            addUnique( set, map.apply( iterator.next() ) );
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
     * Deduplicates values in the {@code values} array.
     *
     * @param values sorted array of int values.
     * @return the provided array if no duplicates were found, otherwise a new shorter array w/o duplicates.
     */
    public static int[] deduplicate( int[] values )
    {
        int unique = 0;
        for ( int i = 0; i < values.length; i++ )
        {
            int value = values[i];
            for ( int j = 0; j < unique; j++ )
            {
                if ( value == values[j] )
                {
                    value = -1; // signal that this value is not unique
                    break; // we will not find more than one conflict
                }
            }
            if ( value != -1 )
            {   // this has to be done outside the inner loop, otherwise we'd never accept a single one...
                values[unique++] = values[i];
            }
        }
        return unique < values.length ? Arrays.copyOf( values, unique ) : values;
    }
}
