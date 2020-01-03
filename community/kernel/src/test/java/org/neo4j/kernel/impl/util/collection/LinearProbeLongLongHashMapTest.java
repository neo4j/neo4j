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

import org.eclipse.collections.api.block.function.primitive.LongFunction;
import org.eclipse.collections.api.block.function.primitive.LongFunction0;
import org.eclipse.collections.api.block.function.primitive.LongToLongFunction;
import org.eclipse.collections.api.block.procedure.primitive.LongLongProcedure;
import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.map.primitive.LongLongMap;
import org.eclipse.collections.api.map.primitive.MutableLongLongMap;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.api.tuple.primitive.LongLongPair;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongLongMaps;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.newListWith;
import static org.eclipse.collections.impl.map.mutable.primitive.LongLongHashMap.newWithKeysValues;
import static org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples.pair;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.util.collection.LinearProbeLongLongHashMap.DEFAULT_CAPACITY;
import static org.neo4j.kernel.impl.util.collection.LinearProbeLongLongHashMap.REMOVALS_FACTOR;

@ExtendWith( RandomExtension.class )
class LinearProbeLongLongHashMapTest
{
    @Inject
    private RandomRule rnd;

    private final CachingOffHeapBlockAllocator blockAllocator = new CachingOffHeapBlockAllocator();
    private final MemoryAllocationTracker memoryTracker = new LocalMemoryTracker();
    private final MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator( memoryTracker, blockAllocator );

    private LinearProbeLongLongHashMap map = newMap();

    private LinearProbeLongLongHashMap newMap()
    {
        return new LinearProbeLongLongHashMap( memoryAllocator );
    }

    @AfterEach
    void tearDown()
    {
        map.close();
        assertEquals( 0, memoryTracker.usedDirectMemory(), "Leaking memory" );
        blockAllocator.release();
    }

    @Test
    void putGetRemove()
    {
        map.put( 0, 10 );
        map.put( 1, 11 );
        map.put( 2, 12 );

        assertEquals( 10, map.get( 0 ) );
        assertEquals( 11, map.get( 1 ) );
        assertEquals( 12, map.get( 2 ) );
        // default empty value
        assertEquals( 0, map.get( 3 ) );

        map.remove( 1 );
        map.remove( 2 );
        map.remove( 0 );

        assertEquals( 0, map.get( 0 ) );
        assertEquals( 0, map.get( 1 ) );
        assertEquals( 0, map.get( 2 ) );
    }

    @Test
    void putAll()
    {
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );
        assertEquals( 3, map.size() );
        assertEquals( 10, map.get( 0 ) );
        assertEquals( 11, map.get( 1 ) );
        assertEquals( 12, map.get( 2 ) );
    }

    @Test
    void getIfAbsent()
    {
        assertEquals( -1, map.getIfAbsent( 0, -1 ) );
        assertEquals( -1, map.getIfAbsent( 1, -1 ) );
        assertEquals( -1, map.getIfAbsent( 2, -1 ) );
        assertEquals( -1, map.getIfAbsent( 3, -1 ) );

        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        assertEquals( 10, map.getIfAbsent( 0, -1 ) );
        assertEquals( 11, map.getIfAbsent( 1, -1 ) );
        assertEquals( 12, map.getIfAbsent( 2, -1 ) );
        assertEquals( -1, map.getIfAbsent( 3, -1 ) );
    }

    @Test
    void getIfAbsentPut()
    {
        assertEquals( 10, map.getIfAbsentPut( 0, 10 ) );
        assertEquals( 10, map.getIfAbsentPut( 0, 100 ) );
        assertEquals( 11, map.getIfAbsentPut( 1, 11 ) );
        assertEquals( 11, map.getIfAbsentPut( 1, 110 ) );
        assertEquals( 12, map.getIfAbsentPut( 2, 12 ) );
        assertEquals( 12, map.getIfAbsentPut( 2, 120 ) );
    }

    @Test
    void getIfAbsentPut_Supplier()
    {
        final LongFunction0 supplier = mock( LongFunction0.class );
        doReturn( 10L, 11L, 12L ).when( supplier ).value();

        assertEquals( 10, map.getIfAbsentPut( 0, supplier ) );
        assertEquals( 11, map.getIfAbsentPut( 1, supplier ) );
        assertEquals( 12, map.getIfAbsentPut( 2, supplier ) );
        verify( supplier, times( 3 ) ).value();

        assertEquals( 10, map.getIfAbsentPut( 0, supplier ) );
        assertEquals( 11, map.getIfAbsentPut( 1, supplier ) );
        assertEquals( 12, map.getIfAbsentPut( 2, supplier ) );
        verifyNoMoreInteractions( supplier );
    }

    @Test
    void getIfAbsentPutWithKey()
    {
        @SuppressWarnings( "Convert2Lambda" )
        final LongToLongFunction function = spy( new LongToLongFunction()
        {
            @Override
            public long valueOf( long x )
            {
                return 10 + x;
            }
        } );

        assertEquals( 10, map.getIfAbsentPutWithKey( 0, function ) );
        assertEquals( 10, map.getIfAbsentPutWithKey( 0, function ) );
        assertEquals( 11, map.getIfAbsentPutWithKey( 1, function ) );
        assertEquals( 11, map.getIfAbsentPutWithKey( 1, function ) );
        assertEquals( 12, map.getIfAbsentPutWithKey( 2, function ) );
        assertEquals( 12, map.getIfAbsentPutWithKey( 2, function ) );

        verify( function ).valueOf( eq( 0L ) );
        verify( function ).valueOf( eq( 1L ) );
        verify( function ).valueOf( eq( 2L ) );
        verifyNoMoreInteractions( function );
    }

    @Test
    void getIfAbsentPutWith()
    {
        @SuppressWarnings( {"Convert2Lambda", "Anonymous2MethodRef"} )
        final LongFunction<String> function = spy( new LongFunction<String>()
        {

            @Override
            public long longValueOf( String s )
            {
                return Long.valueOf( s );
            }
        } );

        assertEquals( 10, map.getIfAbsentPutWith( 0, function, "10" ) );
        assertEquals( 10, map.getIfAbsentPutWith( 0, function, "10" ) );
        assertEquals( 11, map.getIfAbsentPutWith( 1, function, "11" ) );
        assertEquals( 11, map.getIfAbsentPutWith( 1, function, "11" ) );
        assertEquals( 12, map.getIfAbsentPutWith( 2, function, "12" ) );
        assertEquals( 12, map.getIfAbsentPutWith( 2, function, "12" ) );

        verify( function ).longValueOf( eq( "10" ) );
        verify( function ).longValueOf( eq( "11" ) );
        verify( function ).longValueOf( eq( "12" ) );
        verifyNoMoreInteractions( function );
    }

    @Test
    void getOrThrow()
    {
        assertThrows( IllegalStateException.class, () -> map.getOrThrow( 0 ) );
        assertThrows( IllegalStateException.class, () -> map.getOrThrow( 1 ) );
        assertThrows( IllegalStateException.class, () -> map.getOrThrow( 2 ) );

        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        assertEquals( 10, map.getOrThrow( 0 ) );
        assertEquals( 11, map.getOrThrow( 1 ) );
        assertEquals( 12, map.getOrThrow( 2 ) );
    }

    @Test
    void putOverwrite()
    {
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        assertEquals( 10, map.get( 0 ) );
        assertEquals( 11, map.get( 1 ) );
        assertEquals( 12, map.get( 2 ) );

        map.putAll( newWithKeysValues( 0, 20, 1, 21, 2, 22 ) );

        assertEquals( 20, map.get( 0 ) );
        assertEquals( 21, map.get( 1 ) );
        assertEquals( 22, map.get( 2 ) );
    }

    @Test
    void size()
    {
        assertEquals( 0, map.size() );
        map.put( 0, 10 );
        assertEquals( 1, map.size() );
        map.put( 1, 11 );
        assertEquals( 2, map.size() );
        map.put( 2, 12 );
        assertEquals( 3, map.size() );
        map.put( 0, 20 );
        map.put( 1, 20 );
        map.put( 2, 20 );
        assertEquals( 3, map.size() );
        map.remove( 0 );
        assertEquals( 2, map.size() );
        map.remove( 1 );
        assertEquals( 1, map.size() );
        map.remove( 2 );
        assertEquals( 0, map.size() );
    }

    @Test
    void containsKey()
    {
        assertFalse( map.containsKey( 0 ) );
        assertFalse( map.containsKey( 1 ) );
        assertFalse( map.containsKey( 2 ) );

        map.put( 0, 10 );
        assertTrue( map.containsKey( 0 ) );
        map.put( 1, 11 );
        assertTrue( map.containsKey( 1 ) );
        map.put( 2, 12 );
        assertTrue( map.containsKey( 2 ) );

        map.remove( 0 );
        assertFalse( map.containsKey( 0 ) );
        map.remove( 1 );
        assertFalse( map.containsKey( 1 ) );
        map.remove( 2 );
        assertFalse( map.containsKey( 2 ) );
    }

    @Test
    void containsValue()
    {
        assertFalse( map.containsValue( 10 ) );
        assertFalse( map.containsValue( 11 ) );
        assertFalse( map.containsValue( 12 ) );

        map.put( 0, 10 );
        assertTrue( map.containsValue( 10 ) );

        map.put( 1, 11 );
        assertTrue( map.containsValue( 11 ) );

        map.put( 2, 12 );
        assertTrue( map.containsValue( 12 ) );
    }

    @Test
    void removeKeyIfAbsent()
    {
        assertEquals( 10, map.removeKeyIfAbsent( 0, 10 ) );
        assertEquals( 11, map.removeKeyIfAbsent( 1, 11 ) );
        assertEquals( 12, map.removeKeyIfAbsent( 2, 12 ) );

        map.put( 0, 10 );
        map.put( 1, 11 );
        map.put( 2, 12 );

        assertEquals( 10, map.removeKeyIfAbsent( 0, -1 ) );
        assertEquals( 11, map.removeKeyIfAbsent( 1, -1 ) );
        assertEquals( 12, map.removeKeyIfAbsent( 2, -1 ) );

        assertEquals( 0, map.size() );
    }

    @Test
    void updateValue()
    {
        map.updateValue( 0, 10, v -> -v );
        map.updateValue( 1, 11, v -> -v );
        map.updateValue( 2, 12, v -> -v );

        assertEquals( -10, map.get( 0 ) );
        assertEquals( -11, map.get( 1 ) );
        assertEquals( -12, map.get( 2 ) );

        map.updateValue( 0, 0, v -> -v );
        map.updateValue( 1, 0, v -> -v );
        map.updateValue( 2, 0, v -> -v );

        assertEquals( 10, map.get( 0 ) );
        assertEquals( 11, map.get( 1 ) );
        assertEquals( 12, map.get( 2 ) );

        assertEquals( 3, map.size() );
    }

    @Test
    void addToValue()
    {
        assertEquals( 10, map.addToValue( 0, 10 ) );
        assertEquals( 11, map.addToValue( 1, 11 ) );
        assertEquals( 12, map.addToValue( 2, 12 ) );

        assertEquals( 110, map.addToValue( 0, 100 ) );
        assertEquals( 111, map.addToValue( 1, 100 ) );
        assertEquals( 112, map.addToValue( 2, 100 ) );

        assertEquals( 3, map.size() );
    }

    @Test
    void forEachKey()
    {
        final LongProcedure consumer = mock( LongProcedure.class );
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        map.forEachKey( consumer );

        verify( consumer ).value( eq( 0L ) );
        verify( consumer ).value( eq( 1L ) );
        verify( consumer ).value( eq( 2L ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void forEachValue()
    {
        final LongProcedure consumer = mock( LongProcedure.class );
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        map.forEachValue( consumer );

        verify( consumer ).value( eq( 10L ) );
        verify( consumer ).value( eq( 11L ) );
        verify( consumer ).value( eq( 12L ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void forEachKeyValue()
    {
        final LongLongProcedure consumer = mock( LongLongProcedure.class );
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        map.forEachKeyValue( consumer );

        verify( consumer ).value( eq( 0L ), eq( 10L ) );
        verify( consumer ).value( eq( 1L ), eq( 11L ) );
        verify( consumer ).value( eq( 2L ), eq( 12L ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void clear()
    {
        map.clear();
        assertEquals( 0, map.size() );

        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );
        assertEquals( 3, map.size() );

        map.clear();
        assertEquals( 0, map.size() );

        map.clear();
        assertEquals( 0, map.size() );
    }

    @Test
    void toList()
    {
        assertEquals( 0, map.toList().size() );

        map.putAll( toMap( 0, 1, 2, 3, 4, 5 ) );
        assertEquals( newListWith( 0, 1, 2, 3, 4, 5 ), map.toList().sortThis() );
    }

    @Test
    void toArray()
    {
        assertEquals( 0, map.toArray().length );

        map.putAll( toMap( 0, 1, 2, 3, 4, 5 ) );

        assertArrayEquals( new long[]{0, 1, 2, 3, 4, 5}, map.toSortedArray() );
    }

    @Test
    void keysIterator()
    {
        final LongSet keys = LongSets.immutable.of( 0L, 1L, 2L, 42L );
        keys.forEach( k -> map.put( k, k * 10 ) );

        final MutableLongIterator iter = map.longIterator();
        final MutableLongSet found = new LongHashSet();
        while ( iter.hasNext() )
        {
            found.add( iter.next() );
        }

        assertEquals( keys, found );
    }

    @Test
    void keysIteratorFailsWhenMapIsClosed()
    {
        map.putAll( newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        final MutableLongIterator iter = map.longIterator();

        assertTrue( iter.hasNext() );
        assertEquals( 0, iter.next() );

        map.close();

        assertThrows( ConcurrentModificationException.class, iter::hasNext );
        assertThrows( ConcurrentModificationException.class, iter::next );
    }

    @Test
    void grow()
    {
        map = spy( map );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            map.put( 100 + i, i );
        }
        verify( map ).growAndRehash();
    }

    @Test
    void rehashWhenTooManyRemovals()
    {
        map = spy( map );

        final int numOfElements = DEFAULT_CAPACITY / 2;
        final int removalsToTriggerRehashing = (int) (DEFAULT_CAPACITY * REMOVALS_FACTOR);

        for ( int i = 0; i < numOfElements; i++ )
        {
            map.put( 100 + i, i );
        }

        assertEquals( numOfElements, map.size() );
        verify( map, never() ).rehashWithoutGrow();
        verify( map, never() ).growAndRehash();

        for ( int i = 0; i < removalsToTriggerRehashing; i++ )
        {
            map.remove( 100 + i );
        }

        assertEquals( numOfElements - removalsToTriggerRehashing, map.size() );
        verify( map ).rehashWithoutGrow();
        verify( map, never() ).growAndRehash();
    }

    @Test
    void randomizedTest()
    {
        final int count = 10000 + rnd.nextInt( 1000 );

        final MutableLongLongMap m = new LongLongHashMap();
        while ( m.size() < count )
        {
            m.put( rnd.nextLong(), rnd.nextLong() );
        }

        m.forEachKeyValue( ( k, v ) ->
        {
            assertFalse( map.containsKey( k ) );
            map.put( k, v );
            assertTrue( map.containsKey( k ) );
            assertEquals( v, map.get( k ) );
            assertEquals( v, map.getOrThrow( k ) );
            assertEquals( v, map.getIfAbsent( k, v * 2 ) );
            assertEquals( v, map.getIfAbsentPut( k, v * 2 ) );
            assertEquals( v, map.getIfAbsentPut( k, () -> v * 2 ) );
        } );

        assertEquals( m.size(), map.size() );
        assertTrue( m.keySet().allSatisfy( map::containsKey ) );

        final List<LongLongPair> toRemove = m.keyValuesView().select( p -> rnd.nextInt( 100 ) < 75 ).toList().shuffleThis( rnd.random() );

        toRemove.forEach( p ->
        {
            final long k = p.getOne();
            final long v = p.getTwo();

            map.updateValue( k, v + 1, x -> -x );
            assertEquals( -v, map.get( k ) );

            map.remove( k );
            assertEquals( v * 2, map.removeKeyIfAbsent( k, v * 2 ) );
            assertEquals( v * 2, map.getIfAbsent( k, v * 2 ) );
            assertFalse( map.containsKey( k ) );
            assertThrows( IllegalStateException.class, () -> map.getOrThrow( k ) );

            map.updateValue( k, v + 42, x -> -x );
            assertEquals( -v - 42, map.get( k ) );
        } );

        toRemove.forEach( p -> map.removeKey( p.getOne() ) );

        assertEquals( count - toRemove.size(), map.size() );
    }

    @Nested
    class Collisions
    {
        private final ImmutableLongList collisions = generateKeyCollisions( 5 );
        private final long a = collisions.get( 0 );
        private final long b = collisions.get( 1 );
        private final long c = collisions.get( 2 );
        private final long d = collisions.get( 3 );
        private final long e = collisions.get( 4 );

        private ImmutableLongList generateKeyCollisions( int n )
        {
            final long seed = rnd.nextLong();
            final MutableLongList elements;
            try ( LinearProbeLongLongHashMap s = new LinearProbeLongLongHashMap( memoryAllocator ) )
            {
                long v = s.hashAndMask( seed );
                while ( s.hashAndMask( v ) != 0 || v == 0 || v == 1 )
                {
                    ++v;
                }

                final int h = s.hashAndMask( v );
                elements = LongLists.mutable.with( v );

                while ( elements.size() < n )
                {
                    ++v;
                    if ( s.hashAndMask( v ) == h )
                    {
                        elements.add( v );
                    }
                }
            }
            return elements.toImmutable();
        }

        @Test
        void addAll()
        {
            fill( map, collisions.toArray() );
            assertEquals( collisions, map.toSortedList() );
        }

        @Test
        void addAllReversed()
        {
            fill( map, collisions.toReversed().toArray() );
            assertEquals( collisions.toReversed(), map.toList() );
        }

        @Test
        void addAllRemoveLast()
        {
            fill( map, collisions.toArray() );
            map.remove( e );
            assertEquals( newListWith( a, b, c, d ), map.toList() );
        }

        @Test
        void addAllRemoveFirst()
        {
            fill( map, collisions.toArray() );
            map.remove( a );
            assertEquals( newListWith( b, c, d, e ), map.toList() );
        }

        @Test
        void addAllRemoveMiddle()
        {
            fill( map, collisions.toArray() );
            map.remove( b );
            map.remove( d );
            assertEquals( newListWith( a, c, e ), map.toList() );
        }

        @Test
        void addAllRemoveMiddle2()
        {
            fill( map, collisions.toArray() );
            map.remove( a );
            map.remove( c );
            map.remove( e );
            assertEquals( newListWith( b, d ), map.toList() );
        }

        @Test
        void addReusesRemovedHead()
        {
            fill( map, a, b, c );

            map.remove( a );
            assertEquals( newListWith( b, c ), map.toList() );

            map.put( d, 42 );
            assertEquals( newListWith( d, b, c ), map.toList() );
        }

        @Test
        void addReusesRemovedTail()
        {
            fill( map, a, b, c );

            map.remove( c );
            assertEquals( newListWith( a, b ), map.toList() );

            map.put( d, 42 );
            assertEquals( newListWith( a, b, d ), map.toList() );
        }

        @Test
        void addReusesRemovedMiddle()
        {
            fill( map, a, b, c );

            map.remove( b );
            assertEquals( newListWith( a, c ), map.toList() );

            map.put( d, 42 );
            assertEquals( newListWith( a, d, c ), map.toList() );
        }

        @Test
        void addReusesRemovedMiddle2()
        {
            fill( map, a, b, c, d, e );

            map.remove( b );
            map.remove( c );
            assertEquals( newListWith( a, d, e ), map.toList() );

            map.put( c, 1 );
            map.put( b, 2 );
            assertEquals( newListWith( a, c, b, d, e ), map.toList() );
        }

        @Test
        void rehashingCompactsSparseSentinels()
        {
            fill( map, a, b, c, d, e );

            map.remove( b );
            map.remove( d );
            map.remove( e );
            assertEquals( newListWith( a, c ), map.toList() );

            fill( map, b, d, e );
            assertEquals( newListWith( a, b, c, d, e ), map.toList() );

            map.remove( b );
            map.remove( d );
            map.remove( e );
            assertEquals( newListWith( a, c ), map.toList() );

            map.rehashWithoutGrow();
            fill( map, e, d, b );
            assertEquals( newListWith( a, c, e, d, b ), map.toList() );
        }
    }

    @Nested
    class IterationConcurrentModification
    {
        @Test
        void put()
        {
            testIteratorsFail( m -> m.put( 0, 0 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.put( 1, 1 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.put( 0, 0 ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.put( 1, 1 ), pair( 0L, 10L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.put( 2, 2 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.put( 4, 14 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
        }

        @Test
        void getIfAbsentPut_put()
        {
            testIteratorsFail( m -> m.getIfAbsentPut( 0, 0 ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.getIfAbsentPut( 1, 1 ), pair( 0L, 10L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.getIfAbsentPut( 4, 4 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
        }

        @Test
        void getIfAbsentPut_onlyGetNoPut()
        {
            fill( map, 0L, 1L, 2L, 3L );

            final MutableLongIterator keyIter = map.longIterator();
            final Iterator<LongLongPair> keyValueIter = map.keyValuesView().iterator();

            map.getIfAbsentPut( 0, 0 );
            map.getIfAbsentPut( 1, 1 );
            map.getIfAbsentPut( 2, 2 );

            assertDoesNotThrow( keyIter::hasNext );
            assertDoesNotThrow( keyIter::next );
            assertDoesNotThrow( keyValueIter::hasNext );
            assertDoesNotThrow( keyValueIter::next );
        }

        @Test
        void remove()
        {
            testIteratorsFail( m -> m.remove( 0 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.remove( 1 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.remove( 0 ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.remove( 1 ), pair( 0L, 10L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.remove( 2 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.remove( 4 ), pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
        }

        @Test
        void putAll()
        {
            testIteratorsFail( m -> m.putAll( newWithKeysValues( 0, 0 ) ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.putAll( newWithKeysValues( 4, 4 ) ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.putAll( LongLongMaps.immutable.empty() ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
        }

        @Test
        void updateValue()
        {
            testIteratorsFail( m -> m.updateValue( 0, 0, x -> x * 2 ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.updateValue( 2, 2, x -> x * 2 ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
            testIteratorsFail( m -> m.updateValue( 4, 4, x -> x * 2 ),
                    pair( 0L, 10L ), pair( 1L, 11L ), pair( 2L, 12L ), pair( 3L, 13L ) );
        }

        @Test
        void close()
        {
            testIteratorsFail( LinearProbeLongLongHashMap::close, pair( 0L, 10L ), pair( 2L, 12L ) );
        }

        private void testIteratorsFail( Consumer<LinearProbeLongLongHashMap> mutator, LongLongPair... initialValues )
        {
            map.clear();
            for ( LongLongPair pair : initialValues )
            {
                map.putPair( pair );
            }

            final MutableLongIterator keysIterator = map.longIterator();
            final Iterator<LongLongPair> keyValueIterator = map.keyValuesView().iterator();

            assertTrue( keysIterator.hasNext() );
            assertDoesNotThrow( keysIterator::next );
            assertTrue( keyValueIterator.hasNext() );
            assertDoesNotThrow( keyValueIterator::next );

            mutator.accept( map );

            assertThrows( ConcurrentModificationException.class, keysIterator::hasNext );
            assertThrows( ConcurrentModificationException.class, keysIterator::next );
            assertThrows( ConcurrentModificationException.class, keyValueIterator::hasNext );
            assertThrows( ConcurrentModificationException.class, keyValueIterator::next );
        }
    }

    private static void fill( MutableLongLongMap m, long... keys )
    {
        for ( long key : keys )
        {
            m.put( key, System.nanoTime() );
        }
    }

    private static LongLongMap toMap( long... keys )
    {
        final MutableLongLongMap m = new LongLongHashMap();
        fill( m, keys );
        return m;
    }
}
