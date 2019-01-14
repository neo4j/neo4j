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
import java.util.Objects;
import java.util.Set;
import java.util.function.LongFunction;
import java.util.function.LongPredicate;

import org.neo4j.collection.primitive.base.Empty;
import org.neo4j.graphdb.Resource;

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

    private static final PrimitiveLongIterator EMPTY = new PrimitiveLongBaseIterator()
    {
        @Override
        protected boolean fetchNext()
        {
            return false;
        }
    };

    private PrimitiveLongCollections()
    {
        throw new AssertionError( "no instance" );
    }

    public static PrimitiveLongIterator iterator( final long... items )
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
    public static PrimitiveLongIterator concat( PrimitiveLongIterator... primitiveLongIterators )
    {
        return concat( Arrays.asList( primitiveLongIterators ) );
    }

    public static PrimitiveLongIterator concat( Iterable<PrimitiveLongIterator> primitiveLongIterators )
    {
        return new PrimitiveLongConcatingIterator( primitiveLongIterators.iterator() );
    }

    public static PrimitiveLongIterator filter( PrimitiveLongIterator source, final LongPredicate filter )
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

    public static PrimitiveLongResourceIterator filter( PrimitiveLongResourceIterator source, final LongPredicate filter )
    {
        return new PrimitiveLongResourceFilteringIterator( source )
        {
            @Override
            public boolean test( long item )
            {
                return filter.test( item );
            }
        };
    }

    // Range
    public static PrimitiveLongIterator range( long start, long end )
    {
        return new PrimitiveLongRangeIterator( start, end );
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

    public static PrimitiveLongSet asSet( Collection<Long> collection )
    {
        PrimitiveLongSet set = Primitive.longSet( collection.size() );
        for ( Long next : collection )
        {
            set.add( next );
        }
        return set;
    }

    public static PrimitiveLongSet asSet( PrimitiveLongIterator iterator )
    {
        PrimitiveLongSet set = Primitive.longSet();
        while ( iterator.hasNext() )
        {
            set.add( iterator.next() );
        }
        return set;
    }

    public static PrimitiveLongSet asSet( PrimitiveLongSet set )
    {
        PrimitiveLongSet result = Primitive.longSet( set.size() );
        PrimitiveLongIterator iterator = set.iterator();
        while ( iterator.hasNext() )
        {
            result.add( iterator.next() );
        }
        return result;
    }

    public static PrimitiveLongSet asSet( long...values )
    {
        PrimitiveLongSet result = Primitive.longSet( values.length );
        for ( long value : values )
        {
            result.add( value );
        }
        return result;
    }

    public static <T> PrimitiveLongObjectMap<T> copy( PrimitiveLongObjectMap<T> original )
    {
        PrimitiveLongObjectMap<T> copy = Primitive.longObjectMap( original.size() );
        original.visitEntries( ( key, value ) ->
        {
            copy.put( key, value );
            return false;
        } );
        return copy;
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

    public static <T> Iterator<T> map( final LongFunction<T> mapFunction, final PrimitiveLongIterator source )
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
     * @param iterator {@link PrimitiveLongIterator} to pull values from.
     * @return a {@link List} containing all items.
     */
    public static List<Long> asList( PrimitiveLongIterator iterator )
    {
        List<Long> out = new ArrayList<>();
        while ( iterator.hasNext() )
        {
            out.add( iterator.next() );
        }
        return out;
    }

    public static Iterator<Long> toIterator( final PrimitiveLongIterator primIterator )
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
     * Wraps a {@link PrimitiveLongIterator} in a {@link PrimitiveLongResourceIterator} which closes
     * the provided {@code resource} in {@link PrimitiveLongResourceIterator#close()}.
     *
     * @param iterator {@link PrimitiveLongIterator} to convert
     * @param resource {@link Resource} to close in {@link PrimitiveLongResourceIterator#close()}
     * @return Wrapped {@link PrimitiveLongIterator}.
     */
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

    /**
     * Convert primitive set into a plain old java {@link Set}, boxing each long.
     *
     * @param set {@link PrimitiveLongSet} set of primitive values.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet( PrimitiveLongSet set )
    {
        return toSet( set.iterator() );
    }

    /**
     * Pulls all items from the {@code iterator} and puts them into a {@link Set}, boxing each long.
     *
     * @param iterator {@link PrimitiveLongIterator} to pull values from.
     * @return a {@link Set} containing all items.
     */
    public static Set<Long> toSet( PrimitiveLongIterator iterator )
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
        int unique = 0;
        for ( int i = 0; i < values.length; i++ )
        {
            long value = values[i];
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

    /**
     * Base iterator for simpler implementations of {@link PrimitiveLongIterator}s.
     */
    public abstract static class PrimitiveLongBaseIterator implements PrimitiveLongIterator
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
            return (currentIterator != null && currentIterator.hasNext()) && next( currentIterator.next() );
        }

        protected final PrimitiveLongIterator currentIterator()
        {
            return currentIterator;
        }
    }

    public abstract static class PrimitiveLongFilteringIterator extends PrimitiveLongBaseIterator
            implements LongPredicate
    {
        protected final PrimitiveLongIterator source;

        PrimitiveLongFilteringIterator( PrimitiveLongIterator source )
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

    public abstract static class PrimitiveLongResourceFilteringIterator extends PrimitiveLongFilteringIterator
            implements PrimitiveLongResourceIterator
    {
        PrimitiveLongResourceFilteringIterator( PrimitiveLongIterator source )
        {
            super( source );
        }

        @Override
        public void close()
        {
            if ( source instanceof Resource )
            {
                ((Resource) source).close();
            }
        }
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
}
