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
import org.eclipse.collections.api.LazyLongIterable;
import org.eclipse.collections.api.LongIterable;
import org.eclipse.collections.api.RichIterable;
import org.eclipse.collections.api.bag.MutableBag;
import org.eclipse.collections.api.bag.primitive.MutableLongBag;
import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.api.block.function.primitive.LongFunction0;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.block.function.primitive.LongToObjectFunction;
import org.eclipse.collections.api.block.function.primitive.ObjectLongToObjectFunction;
import org.eclipse.collections.api.block.predicate.primitive.LongLongPredicate;
import org.eclipse.collections.api.block.predicate.primitive.LongPredicate;
import org.eclipse.collections.api.block.procedure.Procedure;
import org.eclipse.collections.api.block.procedure.Procedure2;
import org.eclipse.collections.api.block.procedure.primitive.LongLongProcedure;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.block.procedure.primitive.ObjectIntProcedure;
import org.eclipse.collections.api.collection.primitive.MutableLongCollection;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.map.primitive.ImmutableLongLongMap;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.SpreadFunctions;
import org.eclipse.collections.impl.lazy.AbstractLazyIterable;
import org.eclipse.collections.impl.map.mutable.primitive.SynchronizedLongLongMap;
import org.eclipse.collections.impl.map.mutable.primitive.UnmodifiableLongLongMap;
import org.eclipse.collections.impl.primitive.AbstractLongIterable;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.NoSuchElementException;

import org.neo4j.graphdb.Resource;
import org.neo4j.util.VisibleForTesting;

import static java.lang.Integer.bitCount;
import static java.util.Objects.requireNonNull;
import static org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples.pair;
import static org.neo4j.util.Preconditions.checkArgument;

/**
 * Off heap implementation of long-long hash map.
 * <ul>
 * <li>It is <b>not thread-safe</b>
 * <li>It has to be closed to prevent native memory leakage
 * <li>Iterators returned by this map are fail-fast
 * </ul>
 */
class LinearProbeLongLongHashMap extends AbstractLongIterable implements MutableLongLongMap, Resource
{
    @VisibleForTesting
    static final int DEFAULT_CAPACITY = 32;
    @VisibleForTesting
    static final double REMOVALS_FACTOR =  0.25;
    private static final double LOAD_FACTOR = 0.75;

    private static final long EMPTY_KEY = 0;
    private static final long REMOVED_KEY = 1;
    private static final long EMPTY_VALUE = 0;
    private static final long ENTRY_SIZE = 2 * Long.BYTES;

    private final MemoryAllocator allocator;

    private Memory memory;
    private int capacity;
    private long modCount;
    private int resizeOccupancyThreshold;
    private int resizeRemovalsThreshold;
    private int removals;
    private int entriesInMemory;

    private boolean hasZeroKey;
    private boolean hasOneKey;
    private long zeroValue;
    private long oneValue;

    LinearProbeLongLongHashMap( MemoryAllocator allocator )
    {
        this.allocator = requireNonNull( allocator );
        allocateMemory( DEFAULT_CAPACITY );
    }

    @Override
    public void put( long key, long value )
    {
        ++modCount;

        if ( isSentinelKey( key ) )
        {
            putForSentinelKey( key, value );
            return;
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );

        if ( keyAtIdx == key )
        {
            setValueAt( idx, value );
            return;
        }

        if ( keyAtIdx == REMOVED_KEY )
        {
            --removals;
        }

        setKeyAt( idx, key );
        setValueAt( idx, value );

        ++entriesInMemory;
        if ( entriesInMemory >= resizeOccupancyThreshold )
        {
            growAndRehash();
        }
    }

    @Override
    public void putAll( LongLongMap map )
    {
        ++modCount;
        map.forEachKeyValue( this::put );
    }

    /**
     * @param key
     * @return value associated with the key, or {@code zero} if the map doesn't conain this key
     */
    @Override
    public long get( long key )
    {
        return getIfAbsent( key, EMPTY_VALUE );
    }

    @Override
    public long getIfAbsent( long key, long ifAbsent )
    {
        if ( isSentinelKey( key ) )
        {
            return getForSentinelKey( key, ifAbsent );
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );

        if ( keyAtIdx == key )
        {
            return getValueAt( idx );
        }

        return ifAbsent;
    }

    @Override
    public long getIfAbsentPut( long key, long value )
    {
        return getIfAbsentPut( key, () -> value );
    }

    @Override
    public long getIfAbsentPut( long key, LongFunction0 supplier )
    {
        if ( isSentinelKey( key ) )
        {
            return getIfAbsentPutForSentinelKey( key, supplier );
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );

        if ( keyAtIdx == key )
        {
            return getValueAt( idx );
        }

        ++modCount;
        final long value = supplier.value();

        if ( keyAtIdx == REMOVED_KEY )
        {
            --removals;
        }

        setKeyAt( idx, key );
        setValueAt( idx, value );

        ++entriesInMemory;
        if ( entriesInMemory >= resizeOccupancyThreshold )
        {
            growAndRehash();
        }

        return value;
    }

    @Override
    public long getIfAbsentPutWithKey( long key, LongToLongFunction function )
    {
        return getIfAbsentPut( key, () -> function.valueOf( key ) );
    }

    @Override
    public <P> long getIfAbsentPutWith( long key, LongFunction<? super P> function, P parameter )
    {
        return getIfAbsentPut( key, () -> function.longValueOf( parameter ) );
    }

    @Override
    public long getOrThrow( long key )
    {
        return getIfAbsentPut( key, () ->
        {
            throw new IllegalStateException( "Key not found: " + key );
        } );
    }

    @Override
    public void removeKey( long key )
    {
        removeKeyIfAbsent( key, EMPTY_VALUE );
    }

    @Override
    public void remove( long key )
    {
        removeKeyIfAbsent( key, EMPTY_VALUE );
    }

    @Override
    public long removeKeyIfAbsent( long key, long ifAbsent )
    {
        ++modCount;

        if ( isSentinelKey( key ) )
        {
            return removeForSentinelKey( key, ifAbsent );
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );

        if ( keyAtIdx != key )
        {
            return ifAbsent;
        }

        setKeyAt( idx, REMOVED_KEY );
        --entriesInMemory;
        ++removals;

        final long oldValue = getValueAt( idx );

        if ( removals >= resizeRemovalsThreshold )
        {
            rehashWithoutGrow();
        }

        return oldValue;
    }

    @Override
    public boolean containsKey( long key )
    {
        if ( isSentinelKey( key ) )
        {
            return (key == EMPTY_KEY && hasZeroKey) || (key == REMOVED_KEY && hasOneKey);
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );
        return key == keyAtIdx;
    }

    @Override
    public boolean containsValue( long value )
    {
        if ( hasZeroKey && zeroValue == value )
        {
            return true;
        }
        if ( hasOneKey && oneValue == value )
        {
            return true;
        }

        for ( int i = 0; i < capacity; i++ )
        {
            final long key = getKeyAt( i );
            if ( !isSentinelKey( key ) && getValueAt( i ) == value )
            {
                return true;
            }
        }

        return false;
    }

    @Override
    public long updateValue( long key, long initialValueIfAbsent, LongToLongFunction function )
    {
        ++modCount;

        if ( isSentinelKey( key ) )
        {
            return updateValueForSentinelKey( key, initialValueIfAbsent, function );
        }

        final int idx = indexOf( key );
        final long keyAtIdx = getKeyAt( idx );

        if ( keyAtIdx == key )
        {
            final long newValue = function.applyAsLong( getValueAt( idx ) );
            setValueAt( idx, newValue );
            return newValue;
        }

        if ( keyAtIdx == REMOVED_KEY )
        {
            --removals;
        }

        final long value = function.applyAsLong( initialValueIfAbsent );

        setKeyAt( idx, key );
        setValueAt( idx, value );

        ++entriesInMemory;
        if ( entriesInMemory >= resizeOccupancyThreshold )
        {
            growAndRehash();
        }

        return value;
    }

    @Override
    public long addToValue( long key, long toBeAdded )
    {
        return updateValue( key, 0, v -> v + toBeAdded );
    }

    @Override
    public void forEachKey( LongProcedure procedure )
    {
        if ( hasZeroKey )
        {
            procedure.value( 0 );
        }
        if ( hasOneKey )
        {
            procedure.value( 1 );
        }

        int left = entriesInMemory;
        for ( int i = 0; i < capacity && left > 0; i++ )
        {
            final long key = getKeyAt( i );
            if ( !isSentinelKey( key ) )
            {
                procedure.value( key );
                --left;
            }
        }
    }

    @Override
    public void forEachValue( LongProcedure procedure )
    {
        forEachKeyValue( ( key, value ) -> procedure.value( value ) );
    }

    @Override
    public void forEachKeyValue( LongLongProcedure procedure )
    {
        if ( hasZeroKey )
        {
            procedure.value( 0, zeroValue );
        }
        if ( hasOneKey )
        {
            procedure.value( 1, oneValue );
        }

        int left = entriesInMemory;
        for ( int i = 0; i < capacity && left > 0; i++ )
        {
            final long key = getKeyAt( i );
            if ( !isSentinelKey( key ) )
            {
                final long value = getValueAt( i );
                procedure.value( key, value );
                --left;
            }
        }
    }

    @Override
    public MutableLongCollection values()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void clear()
    {
        ++modCount;
        hasZeroKey = false;
        hasOneKey = false;
        entriesInMemory = 0;
        removals = 0;
        memory.free();
        allocateMemory( DEFAULT_CAPACITY );
    }

    @Override
    public MutableLongLongMap flipUniqueValues()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongLongMap select( LongLongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongLongMap reject( LongLongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongLongMap withKeyValue( long key, long value )
    {
        put( key, value );
        return this;
    }

    @Override
    public MutableLongLongMap withoutKey( long key )
    {
        removeKey( key );
        return this;
    }

    @Override
    public MutableLongLongMap withoutAllKeys( LongIterable keys )
    {
        keys.each( this::removeKey );
        return this;
    }

    @Override
    public MutableLongLongMap asUnmodifiable()
    {
        return new UnmodifiableLongLongMap( this );
    }

    @Override
    public MutableLongLongMap asSynchronized()
    {
        return new SynchronizedLongLongMap( this );
    }

    @Override
    public LazyLongIterable keysView()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public RichIterable<LongLongPair> keyValuesView()
    {
        return new KeyValuesView();
    }

    @Override
    public ImmutableLongLongMap toImmutable()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongSet keySet()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongIterator longIterator()
    {
        return new KeysIterator();
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
    public boolean contains( long value )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void forEach( LongProcedure procedure )
    {
        each( procedure );
    }

    @Override
    public void each( LongProcedure procedure )
    {
        if ( hasZeroKey )
        {
            procedure.value( 0 );
        }
        if ( hasOneKey )
        {
            procedure.value( 1 );
        }

        int left = entriesInMemory;
        for ( int i = 0; i < capacity && left > 0; i++ )
        {
            final long key = getKeyAt( i );
            if ( !isSentinelKey( key ) )
            {
                procedure.value( key );
                --left;
            }
        }
    }

    @Override
    public MutableLongBag select( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public MutableLongBag reject( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public <V> MutableBag<V> collect( LongToObjectFunction<? extends V> function )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long detectIfNone( LongPredicate predicate, long ifNone )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int count( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean anySatisfy( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean allSatisfy( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public boolean noneSatisfy( LongPredicate predicate )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public <T> T injectInto( T injectedValue, ObjectLongToObjectFunction<? super T, ? extends T> function )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long sum()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long max()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public long min()
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public int size()
    {
        return entriesInMemory + (hasOneKey ? 1 : 0) + (hasZeroKey ? 1 : 0);
    }

    @Override
    public void appendString( Appendable appendable, String start, String separator, String end )
    {
        throw new UnsupportedOperationException( "not implemented" );
    }

    @Override
    public void close()
    {
        ++modCount;
        if ( memory != null )
        {
            memory.free();
            memory = null;
        }
    }

    @VisibleForTesting
    void rehashWithoutGrow()
    {
        rehash( capacity );
    }

    @VisibleForTesting
    void growAndRehash()
    {
        final int newCapacity = capacity * 2;
        if ( newCapacity < capacity )
        {
            throw new RuntimeException( "Map reached capacity limit" );
        }
        rehash( newCapacity );
    }

    @VisibleForTesting
    int hashAndMask( long element )
    {
        final long h = SpreadFunctions.longSpreadOne( element );
        return Long.hashCode( h ) & (capacity - 1);
    }

    int indexOf( long element )
    {
        int idx = hashAndMask( element );
        int firstRemovedIdx = -1;

        for ( int i = 0; i < capacity; i++ )
        {
            final long keyAtIdx = getKeyAt( idx );

            if ( keyAtIdx == element )
            {
                return idx;
            }

            if ( keyAtIdx == EMPTY_KEY )
            {
                return firstRemovedIdx == -1 ? idx : firstRemovedIdx;
            }

            if ( keyAtIdx == REMOVED_KEY && firstRemovedIdx == -1 )
            {
                firstRemovedIdx = idx;
            }

            idx = (idx + 1) & (capacity - 1);
        }

        throw new AssertionError( "Failed to determine index for " + element );
    }

    private long updateValueForSentinelKey( long key, long initialValueIfAbsent, LongToLongFunction function )
    {
        if ( key == EMPTY_KEY )
        {
            final long newValue = function.applyAsLong( hasZeroKey ? zeroValue : initialValueIfAbsent );
            hasZeroKey = true;
            zeroValue = newValue;
            return newValue;
        }
        if ( key == REMOVED_KEY )
        {
            final long newValue = function.applyAsLong( hasOneKey ? oneValue : initialValueIfAbsent );
            hasOneKey = true;
            oneValue = newValue;
            return newValue;
        }
        throw new AssertionError( "Invalid sentinel key: " + key );
    }

    private void rehash( int newCapacity )
    {
        final int prevCapacity = capacity;
        final Memory prevMemory = memory;
        entriesInMemory = 0;
        removals = 0;
        allocateMemory( newCapacity );

        for ( int i = 0; i < prevCapacity; i++ )
        {
            final long key = prevMemory.readLong( i * ENTRY_SIZE );
            if ( !isSentinelKey( key ) )
            {
                final long value = prevMemory.readLong( (i * ENTRY_SIZE) + ENTRY_SIZE / 2 );
                put( key, value );
            }
        }

        prevMemory.free();
    }

    private static boolean isSentinelKey( long key )
    {
        return key == EMPTY_KEY || key == REMOVED_KEY;
    }

    private void allocateMemory( int newCapacity )
    {
        checkArgument( newCapacity > 1 && bitCount( newCapacity ) == 1, "Capacity must be power of 2" );
        capacity = newCapacity;
        resizeOccupancyThreshold = (int) (newCapacity * LOAD_FACTOR);
        resizeRemovalsThreshold = (int) (newCapacity * REMOVALS_FACTOR);
        memory = allocator.allocate( newCapacity * ENTRY_SIZE, true );
    }

    private long removeForSentinelKey( long key, long ifAbsent )
    {
        if ( key == EMPTY_KEY )
        {
            final long result = hasZeroKey ? zeroValue : ifAbsent;
            hasZeroKey = false;
            return result;
        }
        if ( key == REMOVED_KEY )
        {
            final long result = hasOneKey ? oneValue : ifAbsent;
            hasOneKey = false;
            return result;
        }
        throw new AssertionError( "Invalid sentinel key: " + key );
    }

    private long getForSentinelKey( long key, long ifAbsent )
    {
        if ( key == EMPTY_KEY )
        {
            return hasZeroKey ? zeroValue : ifAbsent;
        }
        if ( key == REMOVED_KEY )
        {
            return hasOneKey ? oneValue : ifAbsent;
        }
        throw new AssertionError( "Invalid sentinel key: " + key );
    }

    private long getIfAbsentPutForSentinelKey( long key, LongFunction0 supplier )
    {
        if ( key == EMPTY_KEY )
        {
            if ( !hasZeroKey )
            {
                ++modCount;
                hasZeroKey = true;
                zeroValue = supplier.value();
            }
            return zeroValue;
        }
        if ( key == REMOVED_KEY )
        {
            if ( !hasOneKey )
            {
                ++modCount;
                hasOneKey = true;
                oneValue = supplier.value();
            }
            return oneValue;
        }
        throw new AssertionError( "Invalid sentinel key: " + key );
    }

    private void setKeyAt( int idx, long key )
    {
        memory.writeLong( idx * ENTRY_SIZE, key );
    }

    private long getKeyAt( int idx )
    {
        return memory.readLong( idx * ENTRY_SIZE );
    }

    private void setValueAt( int idx, long value )
    {
        memory.writeLong( (idx * ENTRY_SIZE) + ENTRY_SIZE / 2, value );
    }

    private long getValueAt( int idx )
    {
        return memory.readLong( (idx * ENTRY_SIZE) + ENTRY_SIZE / 2 );
    }

    private void putForSentinelKey( long key, long value )
    {
        if ( key == EMPTY_KEY )
        {
            hasZeroKey = true;
            zeroValue = value;
        }
        else if ( key == REMOVED_KEY )
        {
            hasOneKey = true;
            oneValue = value;
        }
        else
        {
            throw new AssertionError( "Invalid sentinel key: " + key );
        }
    }

    private class KeyValuesView extends AbstractLazyIterable<LongLongPair>
    {
        @Override
        public void each( Procedure<? super LongLongPair> procedure )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public void forEachWithIndex( ObjectIntProcedure<? super LongLongPair> objectIntProcedure )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public <P> void forEachWith( Procedure2<? super LongLongPair, ? super P> procedure, P parameter )
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public Iterator<LongLongPair> iterator()
        {
            return new KeyValuesIterator();
        }
    }

    private class KeyValuesIterator implements Iterator<LongLongPair>
    {
        private final long modCount = LinearProbeLongLongHashMap.this.modCount;
        private int visited;
        private int idx;

        private boolean handledZero;
        private boolean handledOne;

        @Override
        public LongLongPair next()
        {
            if ( !hasNext() )
            {
                throw new NoSuchElementException( "iterator is exhausted" );
            }

            ++visited;

            if ( !handledZero )
            {
                handledZero = true;
                if ( hasZeroKey )
                {
                    return pair( 0L, zeroValue );
                }
            }

            if ( !handledOne )
            {
                handledOne = true;
                if ( hasOneKey )
                {
                    return pair( 1L, oneValue );
                }
            }

            long key = getKeyAt( idx );
            while ( isSentinelKey( key ) )
            {
                ++idx;
                key = getKeyAt( idx );
            }

            final long value = getValueAt( idx );
            ++idx;
            return pair( key, value );
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            validateIteratorState( modCount );
            return visited != size();
        }
    }

    private class KeysIterator implements MutableLongIterator
    {
        private final long modCount = LinearProbeLongLongHashMap.this.modCount;
        private int visited;
        private int idx;

        private boolean handledZero;
        private boolean handledOne;

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
                if ( hasZeroKey )
                {
                    return 0L;
                }
            }

            if ( !handledOne )
            {
                handledOne = true;
                if ( hasOneKey )
                {
                    return 1L;
                }
            }

            long key = getKeyAt( idx );
            while ( isSentinelKey( key ) )
            {
                ++idx;
                key = getKeyAt( idx );
            }

            ++idx;
            return key;
        }

        @Override
        public void remove()
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean hasNext()
        {
            validateIteratorState( modCount );
            return visited < size();
        }
    }

    private void validateIteratorState( long iteratorModCount )
    {
        if ( iteratorModCount != modCount )
        {
            throw new ConcurrentModificationException();
        }
    }
}
