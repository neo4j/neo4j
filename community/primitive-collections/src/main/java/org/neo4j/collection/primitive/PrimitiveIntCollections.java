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
package org.neo4j.collection.primitive;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.function.Function;
import java.util.function.IntConsumer;
import java.util.function.IntFunction;
import java.util.function.IntPredicate;
import java.util.function.LongToIntFunction;

import org.neo4j.collection.primitive.base.Empty;

/**
 * Basic and common primitive int collection utils and manipulations.
 *
 * @see PrimitiveLongCollections
 * @see Primitive
 */
public class PrimitiveIntCollections
{
    private PrimitiveIntCollections()
    {
    }

    /**
     * Base iterator for simpler implementations of {@link PrimitiveIntIterator}s.
     */
    public abstract static class PrimitiveIntBaseIterator implements PrimitiveIntIterator
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

    public static PrimitiveIntIterator iterator( final int... items )
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
    public static PrimitiveIntIterator concat( Iterator<PrimitiveIntIterator> iterators )
    {
        return new PrimitiveIntConcatingIterator( iterators );
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
            return (currentIterator != null && currentIterator.hasNext()) && next( currentIterator.next() );
        }

        protected final PrimitiveIntIterator currentIterator()
        {
            return currentIterator;
        }
    }

    public static PrimitiveIntIterator filter( PrimitiveIntIterator source, final IntPredicate filter )
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

    public static PrimitiveIntIterator deduplicate( PrimitiveIntIterator source )
    {
        return new PrimitiveIntFilteringIterator( source )
        {
            private final PrimitiveIntSet visited = Primitive.intSet();

            @Override
            public boolean test( int testItem )
            {
                return visited.add( testItem );
            }
        };
    }

    public abstract static class PrimitiveIntFilteringIterator extends PrimitiveIntBaseIterator implements IntPredicate
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

    public static long[] asLongArray( PrimitiveIntCollection values )
    {
        long[] array = new long[values.size()];
        PrimitiveIntIterator iterator = values.iterator();
        int i = 0;
        while ( iterator.hasNext() )
        {
            array[i++] = iterator.next();
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

    public static <T> Iterator<T> map( final IntFunction<T> mapFunction, final PrimitiveIntIterator source )
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

    public static void consume( PrimitiveIntIterator source, IntConsumer consumer )
    {
        while ( source.hasNext() )
        {
            consumer.accept( source.next() );
        }
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

    public static PrimitiveIntSet asSet( long[] values, LongToIntFunction converter )
    {
        PrimitiveIntSet set = Primitive.intSet( values.length );
        for ( long value : values )
        {
            set.add( converter.applyAsInt( value ) );
        }
        return set;
    }

    public static <T> PrimitiveIntObjectMap<T> copyTransform( PrimitiveIntObjectMap<T> original, Function<T,T> transform )
    {
        PrimitiveIntObjectMap<T> copy = Primitive.intObjectMap( original.size() );
        original.visitEntries( ( key, value ) ->
        {
            copy.put( key, transform.apply( value ) );
            return false;
        } );
        return copy;
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
     * @param iterator {@link PrimitiveIntIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Integer> toList( PrimitiveIntIterator iterator )
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
     * @param iterator {@link PrimitiveIntIterator} to pull values from.
     * @return a {@link Set} containing all items.
     * @throws IllegalStateException for the first encountered duplicate.
     */
    public static Set<Integer> toSet( PrimitiveIntIterator iterator )
    {
        return mapToSet( iterator, Integer::new );
    }

    public static <T> Set<T> mapToSet( PrimitiveIntIterator iterator, IntFunction<T> map )
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
