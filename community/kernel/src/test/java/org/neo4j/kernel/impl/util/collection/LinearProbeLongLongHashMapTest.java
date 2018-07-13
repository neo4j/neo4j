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
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.function.Consumer;

import static java.util.Arrays.asList;
import static org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.newListWith;
import static org.eclipse.collections.impl.tuple.primitive.PrimitiveTuples.pair;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
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

class LinearProbeLongLongHashMapTest
{
    private final TestMemoryAllocator allocator = new TestMemoryAllocator();
    private LinearProbeLongLongHashMap map = newMap();

    private LinearProbeLongLongHashMap newMap()
    {
        return new LinearProbeLongLongHashMap( allocator );
    }

    @AfterEach
    void tearDown()
    {
        map.close();
        assertEquals( 0, allocator.tracker.usedDirectMemory(), "Leaking memory" );
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
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );
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

        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

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

        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        assertEquals( 10, map.getOrThrow( 0 ) );
        assertEquals( 11, map.getOrThrow( 1 ) );
        assertEquals( 12, map.getOrThrow( 2 ) );
    }

    @Test
    void putOverwrite()
    {
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        assertEquals( 10, map.get( 0 ) );
        assertEquals( 11, map.get( 1 ) );
        assertEquals( 12, map.get( 2 ) );

        map.putAll( LongLongHashMap.newWithKeysValues( 0, 20, 1, 21, 2, 22 ) );

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
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

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
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

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
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

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

        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );
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
        map.putAll( LongLongHashMap.newWithKeysValues( 0, 10, 1, 11, 2, 12 ) );

        final MutableLongIterator iter = map.longIterator();

        assertTrue( iter.hasNext() );
        assertEquals( 0, iter.next() );

        map.close();

        assertThrows( ConcurrentModificationException.class, iter::hasNext );
        assertThrows( ConcurrentModificationException.class, iter::next );
    }

    @TestFactory
    Collection<DynamicTest> failFastIterator()
    {
        return asList(
                testIteratorsFail( "put sentinel", m -> m.put( 0, 42 ), pair( 1L, 10L ) ),
                testIteratorsFail( "put", m -> m.put( 4, 40 ), pair( 1L, 10L ) ),
                testIteratorsFail( "put all; emtpy source", m -> m.putAll( LongLongMaps.immutable.empty() ), pair( 1L, 10L ) ),
                testIteratorsFail( "overwrite sentinel", m -> m.put( 0, 0 ), pair( 0L, 1L ) ),
                testIteratorsFail( "overwrite", m -> m.put( 4, 40 ), pair( 4L, 40L ) ),
                testIteratorsFail( "remove sentinel", m -> m.remove( 1 ), pair( 1L, 10L ) ),
                testIteratorsFail( "remove", m -> m.remove( 4 ), pair( 4L, 40L ) ),
                testIteratorsFail( "remove nonexisting", m -> m.remove( 13 ), pair( 4L, 40L ) ),
                testIteratorsFail( "getIfAbsentPut", m -> m.getIfAbsentPut( 10, 100 ), pair( 4L, 40L ) ),
                testIteratorsFail( "close", LinearProbeLongLongHashMap::close, pair( 1L, 10L ) )
        );
    }

    @Test
    void grow()
    {
        map = Mockito.spy( map );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            map.put( 100 + i, i );
        }
        verify( map ).growAndRehash();
    }

    @Test
    void rehashWhenTooManyRemovals()
    {
        map = Mockito.spy( map );

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

    @TestFactory
    Collection<DynamicTest> collisions()
    {
        final ImmutableLongList collisions = generateKeyCollisions( 5 );
        final long a = collisions.get( 0 );
        final long b = collisions.get( 1 );
        final long c = collisions.get( 2 );
        final long d = collisions.get( 3 );
        final long e = collisions.get( 4 );

        return asList(
                dynamicTest( "add all", withNewMap( m ->
                {
                    putAll( m, collisions.toArray() );
                    assertEquals( collisions, m.toSortedList() );
                } ) ),
                dynamicTest( "add all reversed", withNewMap( m ->
                {
                    putAll( m, collisions.toReversed().toArray() );
                    assertEquals( collisions.toReversed(), m.toList() );
                } ) ),
                dynamicTest( "add all, remove last", withNewMap( m ->
                {
                    putAll( m, collisions.toArray() );
                    m.remove( e );
                    assertEquals( newListWith( a, b, c, d ), m.toList() );
                } ) ),
                dynamicTest( "add all, remove first", withNewMap( m ->
                {
                    putAll( m, collisions.toArray() );
                    m.remove( a );
                    assertEquals( newListWith( b, c, d, e ), m.toList() );
                } ) ),
                dynamicTest( "add all, remove middle", withNewMap( m ->
                {
                    putAll( m, collisions.toArray() );
                    m.remove( b );
                    m.remove( d );
                    assertEquals( newListWith( a, c, e ), m.toList() );
                } ) ),
                dynamicTest( "add all, remove middle 2", withNewMap( m ->
                {
                    putAll( m, collisions.toArray() );
                    m.remove( a );
                    m.remove( c );
                    m.remove( e );
                    assertEquals( newListWith( b, d ), m.toList() );
                } ) ),
                dynamicTest( "add reuses removed head", withNewMap( m ->
                {
                    putAll( m, a, b, c );

                    m.remove( a );
                    assertEquals( newListWith( b, c ), m.toList() );

                    m.put( d, 42 );
                    assertEquals( newListWith( d, b, c ), m.toList() );
                } ) ),
                dynamicTest( "add reuses removed tail", withNewMap( m ->
                {
                    putAll( m, a, b, c );

                    m.remove( c );
                    assertEquals( newListWith( a, b ), m.toList() );

                    m.put( d, 42 );
                    assertEquals( newListWith( a, b, d ), m.toList() );
                } ) ),
                dynamicTest( "add reuses removed middle", withNewMap( m ->
                {
                    putAll( m, a, b, c );

                    m.remove( b );
                    assertEquals( newListWith( a, c ), m.toList() );

                    m.put( d, 42 );
                    assertEquals( newListWith( a, d, c ), m.toList() );
                } ) ),
                dynamicTest( "add reuses removed middle 2", withNewMap( m ->
                {
                    putAll( m, a, b, c, d, e );

                    m.remove( b );
                    m.remove( c );
                    assertEquals( newListWith( a, d, e ), m.toList() );

                    m.putAll( toMap( c, b ) );
                    assertEquals( newListWith( a, c, b, d, e ), m.toList() );
                } ) ),
                dynamicTest( "rehashing compacts sparse sentinels", withNewMap( m ->
                {
                    putAll( m, a, b, c, d, e );

                    m.remove( b );
                    m.remove( d );
                    m.remove( e );
                    assertEquals( newListWith( a, c ), m.toList() );

                    putAll( m, b, d, e );
                    assertEquals( newListWith( a, b, c, d, e ), m.toList() );

                    m.remove( b );
                    m.remove( d );
                    m.remove( e );
                    assertEquals( newListWith( a, c ), m.toList() );

                    m.rehashWithoutGrow();
                    putAll( m, e, d, b );
                    assertEquals( newListWith( a, c, e, d, b ), m.toList() );
                } ) )
        );
    }

    private static void putAll( LinearProbeLongLongHashMap m, long... keys )
    {
        for ( long key: keys )
        {
            m.put( key, System.nanoTime() );
        }
    }

    private static LongLongMap toMap( long... keys )
    {
        final MutableLongLongMap m = new LongLongHashMap();
        for ( long key: keys )
        {
            m.put( key, System.nanoTime() );
        }
        return m;
    }

    private DynamicTest testIteratorsFail( String name, Consumer<LinearProbeLongLongHashMap> mutator, LongLongPair... initialValues )
    {
        return dynamicTest( name, withNewMap( m ->
        {
            for ( LongLongPair pair: initialValues )
            {
                m.putPair( pair );
            }

            final MutableLongIterator keysIterator = m.longIterator();
            final Iterator<LongLongPair> keyValueIterator = m.keyValuesView().iterator();

            assertTrue( keysIterator.hasNext() );
            assertDoesNotThrow( keysIterator::next );
            assertTrue( keyValueIterator.hasNext() );
            assertDoesNotThrow( keyValueIterator::next );

            mutator.accept( m );

            assertThrows( ConcurrentModificationException.class, keysIterator::hasNext );
            assertThrows( ConcurrentModificationException.class, keysIterator::next );
            assertThrows( ConcurrentModificationException.class, keyValueIterator::hasNext );
            assertThrows( ConcurrentModificationException.class, keyValueIterator::next );
        } ) );
    }

    private Executable withNewMap( Consumer<LinearProbeLongLongHashMap> test )
    {
        return () ->
        {
            try ( LinearProbeLongLongHashMap m = newMap() )
            {
                test.accept( m );
            }
        };
    }

    private ImmutableLongList generateKeyCollisions( int n )
    {
        long v = 1984;
        final MutableLongList elements;
        try ( LinearProbeLongLongHashMap m = newMap() )
        {
            final int h = m.hashAndMask( v );
            elements = LongLists.mutable.with( v );

            while ( elements.size() < n )
            {
                ++v;
                if ( m.hashAndMask( v ) == h )
                {
                    elements.add( v );
                }
            }
        }
        return elements.toImmutable();
    }
}
