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
package org.neo4j.kernel.impl.util.collection;

import org.apache.commons.lang3.mutable.MutableInt;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectLongToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.set.MutableSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.SpreadFunctions;
import org.eclipse.collections.impl.primitive.AbstractLongIterable;
import org.eclipse.collections.impl.set.mutable.UnifiedSet;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;

import java.io.IOException;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import org.neo4j.util.VisibleForTesting;

abstract class AbstractLinearProbeLongHashSet extends AbstractLongIterable implements LongSet
{
    private static final long EMPTY = 0;
    static final long REMOVED = 1;

    Memory memory;
    int capacity;
    int elementsInMemory;
    long modCount;
    boolean hasZero;
    boolean hasOne;

    AbstractLinearProbeLongHashSet()
    {
        // nop
    }

    AbstractLinearProbeLongHashSet( AbstractLinearProbeLongHashSet src )
    {
        this.memory = src.memory;
        this.capacity = src.capacity;
        this.hasZero = src.hasZero;
        this.hasOne = src.hasOne;
        this.elementsInMemory = src.elementsInMemory;
    }

    @Override
    public LongIterator longIterator()
    {
        return new FailFastIterator();
    }

    @Override
    public long[] toArray()
    {
        final MutableInt idx = new MutableInt();
        final long[] array = new long[size()];
        each( element -> array[idx.getAndIncrement()] = element );
        return array;
    }

    @Override
    public boolean contains( long element )
    {
        if ( element == 0 )
        {
            return hasZero;
        }
        if ( element == 1 )
        {
            return hasOne;
        }

        int idx = indexOf( element );
        return valueAt( idx ) == element;
    }

    @Override
    public void forEach( LongProcedure procedure )
    {
        each( procedure );
    }

    @Override
    public void each( LongProcedure procedure )
    {
        if ( hasZero )
        {
            procedure.accept( 0 );
        }
        if ( hasOne )
        {
            procedure.accept( 1 );
        }

        int visited = 0;
        for ( int i = 0; i < capacity && visited < elementsInMemory; i++ )
        {
            final long value = valueAt( i );
            if ( isRealValue( value ) )
            {
                procedure.accept( value );
                ++visited;
            }
        }
    }

    @Override
    public boolean anySatisfy( LongPredicate predicate )
    {
        if ( (hasZero && predicate.test( 0 )) || (hasOne && predicate.test( 1 )) )
        {
            return true;
        }

        int visited = 0;
        for ( int i = 0; i < capacity && visited < elementsInMemory; i++ )
        {
            final long value = valueAt( i );
            if ( isRealValue( value ) )
            {
                if ( predicate.test( value ) )
                {
                    return true;
                }
                ++visited;
            }
        }
        return false;
    }

    @Override
    public boolean allSatisfy( LongPredicate predicate )
    {
        if ( (hasZero && !predicate.test( 0 )) || (hasOne && !predicate.test( 1 )) )
        {
            return false;
        }

        int visited = 0;
        for ( int i = 0; i < capacity && visited < elementsInMemory; i++ )
        {
            final long value = valueAt( i );
            if ( isRealValue( value ) )
            {
                if ( !predicate.test( value ) )
                {
                    return false;
                }
                ++visited;
            }
        }
        return true;
    }

    @Override
    public boolean noneSatisfy( LongPredicate predicate )
    {
        return !anySatisfy( predicate );
    }

    @Override
    public long detectIfNone( LongPredicate predicate, long ifNone )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public <T> T injectInto( T injectedValue, ObjectLongToObjectFunction<? super T, ? extends T> function )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int count( LongPredicate predicate )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long sum()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long max()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long min()
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MutableLongSet select( LongPredicate predicate )
    {
        return select( predicate, new LongHashSet() );
    }

    @Override
    public MutableLongSet reject( LongPredicate predicate )
    {
        return reject( predicate, new LongHashSet() );
    }

    @Override
    public <V> MutableSet<V> collect( LongToObjectFunction<? extends V> function )
    {
        final MutableSet<V> result = new UnifiedSet<>( size() );
        each( element ->
        {
            result.add( function.apply( element ) );
        } );
        return result;
    }

    @Override
    public int hashCode()
    {
        final MutableInt h = new MutableInt();
        each( element -> h.add( (int) (element ^ element >>> 32) ) );
        return h.intValue();
    }

    @Override
    public boolean equals( Object obj )
    {
        if ( this == obj )
        {
            return true;
        }
        if ( !(obj instanceof LongSet) )
        {
            return false;
        }
        final LongSet other = (LongSet) obj;
        return size() == other.size() && containsAll( other );
    }

    @Override
    public int size()
    {
        return elementsInMemory + (hasZero ? 1 : 0) + (hasOne ? 1 : 0);
    }

    @Override
    public void appendString( Appendable appendable, String start, String separator, String end )
    {
        try
        {
            appendable.append( start );
            appendable.append( "offheap,size=" ).append( String.valueOf( size() ) ).append( "; " );

            final LongIterator iterator = longIterator();
            for ( int i = 0; i < 100 && iterator.hasNext(); i++ )
            {
                appendable.append( Long.toString( iterator.next() ) );
                if ( iterator.hasNext() )
                {
                    appendable.append( ", " );
                }
            }

            if ( iterator.hasNext() )
            {
                appendable.append( "..." );
            }

            appendable.append( end );
        }
        catch ( IOException e )
        {
            throw new RuntimeException( e );
        }
    }

    static boolean isRealValue( long value )
    {
        return value != REMOVED && value != EMPTY;
    }

    @VisibleForTesting
    int hashAndMask( long element )
    {
        final long h = SpreadFunctions.longSpreadOne( element );
        return Long.hashCode( h ) & (capacity - 1);
    }

    long valueAt( int idx )
    {
        return memory.readLong( (long) idx * Long.BYTES );
    }

    int indexOf( long element )
    {
        int idx = hashAndMask( element );
        int firstRemovedIdx = -1;

        for ( int i = 0; i < capacity; i++ )
        {
            final long valueAtIdx = valueAt( idx );

            if ( valueAtIdx == element )
            {
                return idx;
            }

            if ( valueAtIdx == EMPTY )
            {
                return firstRemovedIdx == -1 ? idx : firstRemovedIdx;
            }

            if ( valueAtIdx == REMOVED && firstRemovedIdx == -1 )
            {
                firstRemovedIdx = idx;
            }

            idx = (idx + 1) & (capacity - 1);
        }

        throw new AssertionError( "Failed to determine index for " + element );
    }

    class FailFastIterator implements MutableLongIterator
    {
        private final long modCount;
        private int visited;
        private int idx;
        private boolean handledZero;
        private boolean handledOne;

        FailFastIterator()
        {
            this.modCount = AbstractLinearProbeLongHashSet.this.modCount;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException( "iterator is exhausted" );
            }

            ++visited;

            if ( !handledZero )
            {
                handledZero = true;
                if ( hasZero )
                {
                    return 0;
                }
            }
            if ( !handledOne )
            {
                handledOne = true;
                if ( hasOne )
                {
                    return 1;
                }
            }

            long value;
            do
            {
                value = valueAt( idx++ );
            }
            while ( !isRealValue( value ) );

            return value;
        }

        @Override
        public boolean hasNext()
        {
            checkState();
            return visited < size();
        }

        private void checkState()
        {
            if ( modCount != AbstractLinearProbeLongHashSet.this.modCount )
            {
                throw new ConcurrentModificationException();
            }
        }
    }
}
