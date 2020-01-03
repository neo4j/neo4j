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

import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.multimap.MutableMultimap;
import org.eclipse.collections.api.set.primitive.ImmutableLongSet;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.Multimaps;
import org.eclipse.collections.impl.set.mutable.primitive.SynchronizedLongSet;
import org.eclipse.collections.impl.set.mutable.primitive.UnmodifiableLongSet;

import org.neo4j.graphdb.Resource;
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
    public LongSet freeze()
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

    class FrozenCopy extends AbstractLinearProbeLongHashSet
    {

        FrozenCopy()
        {
            super( MutableLinearProbeLongHashSet.this );
        }

        @Override
        public LongSet freeze()
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
    }
}
