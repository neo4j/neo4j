/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.set.mutable.primitive.SynchronizedLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.UnmodifiableLongSet;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Resource;
import org.neo4j.util.Preconditions;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Integer.bitCount;
import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * Off heap implementation of long hash set.
 * <ul>
 * <li>It is <b>not thread-safe</b>
 * <li>It has to be closed to prevent native memory leakage
 * <li>Iterators returned by this set are fail-fast
 * </ul>
 */
class MutableLinearProbeLongHashSet extends AbstractLinearProbeLongHashSet implements MutableLongSet, Resource
{
    static final int DEFAULT_CAPACITY = 32;
    static final int REMOVALS_RATIO = 4;
    private static final double LOAD_FACTOR = 0.75;

    private final MemoryAllocator allocator;
    private final MutableMultimap<Memory, FrozenCopy> frozenCopies = Multimaps.mutable.list.empty();

    private int resizeOccupancyThreshold;
    private int resizeRemovalsThreshold;
    private int removals;
    private boolean frozen;

    MutableLinearProbeLongHashSet( MemoryAllocator allocator )
    {
        this.allocator = requireNonNull( allocator );
        allocateMemory( DEFAULT_CAPACITY );
    }

    @Override
    public boolean add( long element )
    {
        ++modCount;
        if ( element == 0 )
        {
            final boolean hadZero = hasZero;
            hasZero = true;
            return hadZero != hasZero;
        }
        if ( element == 1 )
        {
            final boolean hadOne = hasOne;
            hasOne = true;
            return hadOne != hasOne;
        }
        return addToMemory( element );
    }

    @Override
    public boolean addAll( long... elements )
    {
        ++modCount;
        final int prevSize = size();
        for ( final long element : elements )
        {
            add( element );
        }
        return prevSize != size();
    }

    @Override
    public boolean addAll( LongIterable elements )
    {
        ++modCount;
        final int prevSize = size();
        elements.forEach( this::add );
        return prevSize != size();
    }

    @Override
    public boolean remove( long element )
    {
        ++modCount;
        if ( element == 0 )
        {
            final boolean hadZero = hasZero;
            hasZero = false;
            return hadZero != hasZero;
        }
        if ( element == 1 )
        {
            final boolean hadOne = hasOne;
            hasOne = false;
            return hadOne != hasOne;
        }
        return removeFromMemory( element );
    }

    @Override
    public boolean removeAll( LongIterable elements )
    {
        ++modCount;
        final int prevSize = size();
        elements.forEach( this::remove );
        return prevSize != size();
    }

    @Override
    public boolean removeAll( long... elements )
    {
        ++modCount;
        final int prevSize = size();
        for ( final long element : elements )
        {
            remove( element );
        }
        return prevSize != size();
    }

    @Override
    public boolean retainAll( LongIterable elements )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll( long... source )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear()
    {
        ++modCount;
        copyIfFrozen();
        memory.clear();
        hasZero = false;
        hasOne = false;
        elementsInMemory = 0;
        removals = 0;
    }

    @Override
    public MutableLongIterator longIterator()
    {
        return new FailFastIterator();
    }

    @Override
    public void close()
    {
        ++modCount;
        if ( memory != null )
        {
            frozenCopies.forEachKeyMultiValues( ( mem, copies ) ->
            {
                mem.free();
                copies.forEach( FrozenCopy::invalidate );
            } );
            if ( !frozenCopies.containsKey( memory ) )
            {
                memory.free();
            }
            memory = null;
            frozenCopies.clear();
        }
    }

    @Override
    public MutableLongSet tap( LongProcedure procedure )
    {
        each( procedure );
        return this;
    }

    @Override
    public MutableLongSet with( long element )
    {
        add( element );
        return this;
    }

    @Override
    public MutableLongSet without( long element )
    {
        remove( element );
        return this;
    }

    @Override
    public MutableLongSet withAll( LongIterable elements )
    {
        addAll( elements );
        return this;
    }

    @Override
    public MutableLongSet withoutAll( LongIterable elements )
    {
        removeAll( elements );
        return this;
    }

    @Override
    public MutableLongSet asUnmodifiable()
    {
        return new UnmodifiableLongSet( this );
    }

    @Override
    public MutableLongSet asSynchronized()
    {
        return new SynchronizedLongSet( this );
    }

    @Override
    public RichLongSet freeze()
    {
        frozen = true;
        final FrozenCopy frozen = new FrozenCopy();
        frozenCopies.put( memory, frozen );
        return frozen;
    }

    @Override
    public ImmutableLongSet toImmutable()
    {
        throw new UnsupportedOperationException();
    }

    private boolean removeFromMemory( long element )
    {
        final int idx = indexOf( element );
        final long valueAtIdx = memory.readLong( (long) idx * Long.BYTES );

        if ( valueAtIdx != element )
        {
            return false;
        }

        copyIfFrozen();

        memory.writeLong( (long) idx * Long.BYTES, REMOVED );
        --elementsInMemory;
        ++removals;

        if ( removals >= resizeRemovalsThreshold )
        {
            rehashWithoutGrow();
        }
        return true;
    }

    private boolean addToMemory( long element )
    {
        final int idx = indexOf( element );
        final long valueAtIdx = valueAt( idx );

        if ( valueAtIdx == element )
        {
            return false;
        }

        if ( valueAtIdx == REMOVED )
        {
            --removals;
        }

        copyIfFrozen();

        memory.writeLong( (long) idx * Long.BYTES, element );
        ++elementsInMemory;

        if ( elementsInMemory >= resizeOccupancyThreshold )
        {
            growAndRehash();
        }

        return true;
    }

    @VisibleForTesting
    void growAndRehash()
    {
        final int newCapacity = capacity * 2;
        if ( newCapacity < capacity )
        {
            throw new RuntimeException( "LongSet reached capacity limit" );
        }
        rehash( newCapacity );
    }

    @VisibleForTesting
    void rehashWithoutGrow()
    {
        rehash( capacity );
    }

    private void allocateMemory( int newCapacity )
    {
        checkArgument( newCapacity > 1 && bitCount( newCapacity ) == 1, "Capacity must be power of 2" );
        capacity = newCapacity;
        resizeOccupancyThreshold = (int) (newCapacity * LOAD_FACTOR);
        resizeRemovalsThreshold = newCapacity / REMOVALS_RATIO;
        memory = allocator.allocate( (long) newCapacity * Long.BYTES, true );
    }

    private void rehash( int newCapacity )
    {
        final int prevCapacity = capacity;
        final Memory prevMemory = memory;
        elementsInMemory = 0;
        removals = 0;
        allocateMemory( newCapacity );

        for ( int i = 0; i < prevCapacity; i++ )
        {
            final long value = prevMemory.readLong( (long) i * Long.BYTES );
            if ( isRealValue( value ) )
            {
                add( value );
            }
        }

        prevMemory.free();
    }

    private void copyIfFrozen()
    {
        if ( frozen )
        {
            frozen = false;
            memory = memory.copy();
        }
    }

    private class RangeIterator implements LongIterator
    {
        private static final int NOT_INITIALIZED = -2;
        private static final int ON_ONE = -1;
        private final int stop;
        //the position referes to the position of the iterator
        private int currentPosition;
        //keeps track of where in the backing memory we are currently looking
        private int internalPosition = NOT_INITIALIZED;
        private long modCount;

        RangeIterator( int start, int stop )
        {
            Preconditions.requireNonNegative( start );
            Preconditions.requireNonNegative( stop );
            Preconditions.requireNonNegative( size() - start );
            this.stop = Math.min( stop, size() );
            this.currentPosition = start;
            this.modCount = MutableLinearProbeLongHashSet.this.modCount;
        }

        /**
         * Finds the n:th item of the iteration and sets the internalPosition so that it is ready to find the next
         * element.
         * <p>
         * Example: Say we have elements (4, 5, 6) in the set and pretend they are layed out in memory as
         * [x, 6, x, 5, x, 4, x, 3, x] where x marks empty slots. Calling findNth(2) would then
         * return 4 and set internalPosition to 6. When the set contains 0 and 1 we need to treat this with some care
         * since they are not stored in the same way. Here it is assumed that if we have 0L in the set it is always
         * stored at position 0 and if the set contains 1L it is either stored at position 0 if 0L is not present in
         * the set or at position 1 if 0L is in the set.
         *
         * @param n the nth element to find
         * @return the n:th element of the set when viewed as an iterator.
         */
        private long findNth( int n )
        {
            int positionNotCountingZerorOrOne = n - (hasOne ? 1 : 0) - (hasZero ? 1 : 0);
            if ( positionNotCountingZerorOrOne == -2 )
            {   //we are looking for the zeroth element, which is always 0 if it is in the set
                //if this set also
                internalPosition = hasOne ? ON_ONE : 0;
                return 0L;
            }
            else if ( positionNotCountingZerorOrOne == -1 )
            {
                //we are either looking for the zeroth element of a set containing either 0L or 1L
                //or we are looking for the first element of a set containing both 0L and 1L
                internalPosition = 0;
                return (hasZero && hasOne) ? 1L : (hasOne ? 1L : 0L);
            }
            int pos = 0;
            for ( int i = 0; i < capacity && pos < elementsInMemory; i++ )
            {
                final long value = valueAt( i );
                if ( isRealValue( value ) )
                {
                    if ( pos == positionNotCountingZerorOrOne )
                    {
                        internalPosition = i + 1;
                        return value;
                    }
                    pos++;
                }
            }
            throw new NoSuchElementException();
        }

        @Override
        public long next()
        {
            switch ( internalPosition )
            {
            case NOT_INITIALIZED:
                return findNth( currentPosition++ );
            case ON_ONE:
                currentPosition++;
                internalPosition = 0;
                return 1L;
            default:
                return findNext();

            }
        }

        private long findNext()
        {
            assert internalPosition >= 0;
            int i = internalPosition;
            while ( i < capacity )
            {
                final long value = valueAt( i++ );
                if ( isRealValue( value ) )
                {
                    internalPosition = i;
                    currentPosition++;
                    return value;
                }
            }

            throw new NoSuchElementException();
        }

        @Override
        public boolean hasNext()
        {
            checkState();
            return currentPosition < stop;
        }

        private void checkState()
        {
            if ( modCount != MutableLinearProbeLongHashSet.this.modCount )
            {
                throw new ConcurrentModificationException();
            }
        }
    }

    @Override
    public LongIterator rangeIterator( int start, int stop )
    {
        return new RangeIterator( start, stop );
    }

    class FrozenCopy extends AbstractLinearProbeLongHashSet
    {
        FrozenCopy()
        {
            super( MutableLinearProbeLongHashSet.this );
        }

        @Override
        public RichLongSet freeze()
        {
            return this;
        }

        @Override
        public ImmutableLongSet toImmutable()
        {
            throw new UnsupportedOperationException();
        }

        void invalidate()
        {
            ++FrozenCopy.this.modCount;
        }

        @Override
        public LongIterator rangeIterator( int start, int stop )
        {
            return new RangeIterator( start, stop );
        }

    }
}
