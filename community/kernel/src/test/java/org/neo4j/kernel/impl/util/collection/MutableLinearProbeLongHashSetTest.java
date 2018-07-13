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

import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.function.Executable;
import org.mockito.Mockito;

import java.util.Collection;
import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.neo4j.memory.MemoryAllocationTracker;

import static java.util.Arrays.asList;
import static org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.newListWith;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.DynamicTest.dynamicTest;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.util.collection.MutableLinearProbeLongHashSet.DEFAULT_CAPACITY;
import static org.neo4j.kernel.impl.util.collection.MutableLinearProbeLongHashSet.REMOVALS_RATIO;

class MutableLinearProbeLongHashSetTest
{
    private final TestMemoryAllocator allocator = new TestMemoryAllocator();
    private MutableLinearProbeLongHashSet set = newSet();

    @AfterEach
    void tearDown()
    {
        set.close();
        assertEquals( 0, allocator.tracker.usedDirectMemory(), "Leaking memory" );
    }

    @Test
    void addRemoveContains()
    {
        set = Mockito.spy( set );

        assertFalse( set.contains( 0 ) );
        assertTrue( set.add( 0 ) );
        assertTrue( set.contains( 0 ) );
        assertFalse( set.add( 0 ) );
        assertEquals( 1, set.size() );

        assertFalse( set.contains( 1 ) );
        assertTrue( set.add( 1 ) );
        assertTrue( set.contains( 1 ) );
        assertFalse( set.add( 1 ) );
        assertEquals( 2, set.size() );

        assertFalse( set.contains( 2 ) );
        assertTrue( set.add( 2 ) );
        assertTrue( set.contains( 2 ) );
        assertFalse( set.add( 2 ) );
        assertEquals( 3, set.size() );

        assertFalse( set.contains( 3 ) );
        assertFalse( set.remove( 3 ) );
        assertEquals( 3, set.size() );

        assertEquals( newSetWith( 0, 1, 2 ), set );

        assertTrue( set.remove( 0 ) );
        assertFalse( set.contains( 0 ) );
        assertEquals( 2, set.size() );

        assertTrue( set.remove( 1 ) );
        assertFalse( set.contains( 1 ) );
        assertEquals( 1, set.size() );

        assertTrue( set.remove( 2 ) );
        assertFalse( set.contains( 2 ) );
        assertEquals( 0, set.size() );
    }

    @Test
    void addRemoveAll()
    {
        set.addAll( 1, 2, 3, 1, 2, 3, 100, 200, 300 );
        assertEquals( 6, set.size() );
        assertTrue( set.containsAll( 100, 200, 300, 1, 2, 3 ) );

        set.removeAll( 1, 2, 100, 200 );
        assertEquals( 2, set.size() );
        assertTrue( set.containsAll( 300, 3 ) );

        set.removeAll( 3, 300 );
        assertEquals( 0, set.size() );
    }

    @Test
    void clear()
    {
        set.addAll( 1, 2, 3 );
        assertEquals( 3, set.size() );

        set.clear();
        assertEquals( 0, set.size() );

        set.clear();
        assertEquals( 0, set.size() );

        set.addAll( 4, 5, 6 );
        assertEquals( 3, set.size() );

        set.clear();
        assertEquals( 0, set.size() );
    }

    @Test
    void grow()
    {
        set = Mockito.spy( set );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            assertTrue( set.add( 100 + i ) );
        }
        verify( set ).growAndRehash();
    }

    @Test
    void rehashWhenTooManyRemovals()
    {
        set = Mockito.spy( set );

        final int numOfElements = DEFAULT_CAPACITY / 2;
        final int removalsToTriggerRehashing = DEFAULT_CAPACITY / REMOVALS_RATIO;

        for ( int i = 0; i < numOfElements; i++ )
        {
            assertTrue( set.add( 100 + i ) );
        }

        assertEquals( numOfElements, set.size() );
        verify( set, never() ).rehashWithoutGrow();
        verify( set, never() ).growAndRehash();

        for ( int i = 0; i < removalsToTriggerRehashing; i++ )
        {
            assertTrue( set.remove( 100 + i ) );
        }

        assertEquals( numOfElements - removalsToTriggerRehashing, set.size() );
        verify( set ).rehashWithoutGrow();
        verify( set, never() ).growAndRehash();
    }

    @Test
    void forEach()
    {
        final LongProcedure consumer = mock( LongProcedure.class );

        set.addAll( 1, 2, 100, 200 );
        set.forEach( consumer );

        verify( consumer ).accept( eq( 1L ) );
        verify( consumer ).accept( eq( 2L ) );
        verify( consumer ).accept( eq( 100L ) );
        verify( consumer ).accept( eq( 200L ) );
        verifyNoMoreInteractions( consumer );
    }

    @Test
    void allocateFreeMemory()
    {
        final MemoryAllocationTracker tracker = mock( MemoryAllocationTracker.class );
        final MutableLinearProbeLongHashSet set2 = new MutableLinearProbeLongHashSet( new TestMemoryAllocator( tracker ) );

        verify( tracker ).allocated( anyLong() );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            set2.add( 100 + i );
        }
        verify( tracker ).deallocated( anyLong() );
        verify( tracker, times( 2 ) ).allocated( anyLong() );

        set2.close();
        verify( tracker, times( 2 ) ).deallocated( anyLong() );
    }

    @Test
    void freeFrozenMemory()
    {
        final MemoryAllocationTracker tracker = mock( MemoryAllocationTracker.class );
        final MutableLinearProbeLongHashSet set2 = new MutableLinearProbeLongHashSet( new TestMemoryAllocator( tracker ) );
        verify( tracker ).allocated( anyLong() );

        set2.addAll( 100, 200, 300 );
        set2.freeze();
        set2.remove( 100 );
        set2.freeze();
        set2.clear();
        set2.close();
        verify( tracker, times( 3 ) ).deallocated( anyLong() );
    }

    @Test
    void toList()
    {
        assertEquals( 0, set.toList().size() );

        set.addAll( 1, 2, 3, 100, 200, 300 );
        assertEquals( newListWith( 1, 2, 3, 100, 200, 300 ), set.toList().sortThis() );
    }

    @Test
    void toArray()
    {
        assertEquals( 0, set.toArray().length );

        set.addAll( 1, 2, 3, 100, 200, 300 );
        assertArrayEquals( new long[]{1, 2, 3, 100, 200, 300}, set.toSortedArray() );
    }

    @Test
    void emptyIterator()
    {
        final MutableLongIterator iterator = set.longIterator();
        assertFalse( iterator::hasNext );
        assertThrows( NoSuchElementException.class, iterator::next );
    }

    @Test
    void iterator()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );

        MutableLongIterator iterator = set.longIterator();
        final LongHashSet visited = new LongHashSet();
        while ( iterator.hasNext() )
        {
            visited.add( iterator.next() );
        }

        assertEquals( 6, visited.size() );
        assertEquals( set, visited );

        assertThrows( NoSuchElementException.class, iterator::next );
    }

    @TestFactory
    Collection<DynamicTest> failFastIterator()
    {
        return asList(
                testIteratorFails( "add duplicate to bitmap", s -> s.add( 1 ), 1, 2, 3 ),
                testIteratorFails( "add unique to bitmap", s -> s.add( 4 ), 1, 2, 3 ),
                testIteratorFails( "add duplicate to memory", s -> s.add( 100 ), 100, 200, 300 ),
                testIteratorFails( "add unique to memory", s -> s.add( 400 ), 100, 200, 300 ),
                testIteratorFails( "remove duplicate to bitmap", s -> s.remove( 1 ), 1, 2, 3 ),
                testIteratorFails( "remove unique to bitmap", s -> s.remove( 4 ), 1, 2, 3 ),
                testIteratorFails( "remove duplicate to memory", s -> s.remove( 100 ), 100, 200, 300 ),
                testIteratorFails( "remove unique to memory", s -> s.remove( 400 ), 100, 200, 300 ),
                testIteratorFails( "removeAll empty source", s -> s.removeAll(), 100, 200, 300 ),
                testIteratorFails( "addAll empty source", s -> s.addAll(), 100, 200, 300 ),
                testIteratorFails( "close", MutableLinearProbeLongHashSet::close, 1, 2, 3 )
        );
    }

    @Test
    void freeze()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );

        final LongSet frozen = set.freeze();
        assertEquals( set, frozen );
        assertEquals( 6, set.size() );
        assertEquals( 6, frozen.size() );

        set.removeAll( 1, 100 );
        assertNotEquals( set, frozen );
        assertEquals( 4, set.size() );
        assertEquals( 6, frozen.size() );
    }

    @Test
    void testEquals()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );
        final MutableLinearProbeLongHashSet set2 = newSet( 300, 200, 100, 3, 2, 1 );
        final LongHashSet set3 = newSetWith( 300, 200, 100, 3, 2, 1 );
        assertEquals( set, set2 );
        assertEquals( set, set3 );

        set.removeAll( 1, 100 );
        assertNotEquals( set, set2 );
        assertNotEquals( set, set3 );

        set2.close();
    }

    @Test
    void frozenIterator()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );

        final LongIterator iter1 = set.freeze().longIterator();
        set.removeAll( 1, 100 );
        final LongIterator iter2 = set.freeze().longIterator();
        set.removeAll( 2, 200 );

        final LongSet values1 = drain( iter1 );
        final LongSet values2 = drain( iter2 );

        assertEquals( newSetWith( 1, 2, 3, 100, 200, 300 ), values1 );
        assertEquals( newSetWith( 2, 3, 200, 300 ), values2 );
    }

    @Test
    void frozenIteratorFailsWhenParentSetIsClosed()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );

        final LongIterator iter = set.freeze().longIterator();

        set.close();

        assertThrows( ConcurrentModificationException.class, iter::hasNext );
        assertThrows( ConcurrentModificationException.class, iter::next );
    }

    @Test
    void allSatisfy()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );
        assertTrue( set.allSatisfy( x -> x < 1000 ) );
        assertFalse( set.allSatisfy( x -> x % 2 == 0 ) );
    }

    @Test
    void noneSatisfy()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );
        assertTrue( set.noneSatisfy( x -> x < 0 ) );
        assertFalse( set.noneSatisfy( x -> x % 2 == 0 ) );
    }

    @Test
    void anySatisfy()
    {
        set.addAll( 1, 2, 3, 100, 200, 300 );
        assertTrue( set.anySatisfy( x -> x % 3 == 1 ) );
        assertFalse( set.anySatisfy( x -> x < 0 ) );
    }

    @TestFactory
    Collection<DynamicTest> collisions()
    {
        final ImmutableLongList collisions = generateCollisions( 5 );
        final long a = collisions.get( 0 );
        final long b = collisions.get( 1 );
        final long c = collisions.get( 2 );
        final long d = collisions.get( 3 );
        final long e = collisions.get( 4 );

        return asList(
                dynamicTest( "add all", withNewSet( set ->
                {
                    set.addAll( collisions );
                    assertEquals( collisions, set.toList() );
                } ) ),
                dynamicTest( "add all reversed", withNewSet( set ->
                {
                    set.addAll( collisions.toReversed() );
                    assertEquals( collisions.toReversed(), set.toList() );
                } ) ),
                dynamicTest( "add all, remove last", withNewSet( set ->
                {
                    set.addAll( collisions );
                    set.remove( e );
                    assertEquals( newListWith( a, b, c, d ), set.toList() );
                } ) ),
                dynamicTest( "add all, remove first", withNewSet( set ->
                {
                    set.addAll( collisions );
                    set.remove( a );
                    assertEquals( newListWith( b, c, d, e ), set.toList() );
                } ) ),
                dynamicTest( "add all, remove middle", withNewSet( set ->
                {
                    set.addAll( collisions );
                    set.removeAll( b, d );
                    assertEquals( newListWith( a, c, e ), set.toList() );
                } ) ),
                dynamicTest( "add all, remove middle 2", withNewSet( set ->
                {
                    set.addAll( collisions );
                    set.removeAll( a, c, e );
                    assertEquals( newListWith( b, d ), set.toList() );
                } ) ),
                dynamicTest( "add reuses removed head", withNewSet( set ->
                {
                    set.addAll( a, b, c );

                    set.remove( a );
                    assertEquals( newListWith( b, c ), set.toList() );

                    set.add( d );
                    assertEquals( newListWith( d, b, c ), set.toList() );
                } ) ),
                dynamicTest( "add reuses removed tail", withNewSet( set ->
                {
                    set.addAll( a, b, c );

                    set.remove( c );
                    assertEquals( newListWith( a, b ), set.toList() );

                    set.add( d );
                    assertEquals( newListWith( a, b, d ), set.toList() );
                } ) ),
                dynamicTest( "add reuses removed middle", withNewSet( set ->
                {
                    set.addAll( a, b, c );

                    set.remove( b );
                    assertEquals( newListWith( a, c ), set.toList() );

                    set.add( d );
                    assertEquals( newListWith( a, d, c ), set.toList() );
                } ) ),
                dynamicTest( "add reuses removed middle 2", withNewSet( set ->
                {
                    set.addAll( a, b, c, d, e );

                    set.removeAll( b, c );
                    assertEquals( newListWith( a, d, e ), set.toList() );

                    set.addAll( c, b );
                    assertEquals( newListWith( a, c, b, d, e ), set.toList() );
                } ) ),
                dynamicTest( "rehashing compacts sparse sentinels", withNewSet( set ->
                {
                    set.addAll( a, b, c, d, e );

                    set.removeAll( b, d, e );
                    assertEquals( newListWith( a, c ), set.toList() );

                    set.addAll( b, d, e );
                    assertEquals( newListWith( a, b, c, d, e ), set.toList() );

                    set.removeAll( b, d, e );
                    assertEquals( newListWith( a, c ), set.toList() );

                    set.rehashWithoutGrow();
                    set.addAll( e, d, b );
                    assertEquals( newListWith( a, c, e, d, b ), set.toList() );
                } ) )
        );
    }

    private static LongSet drain( LongIterator iter )
    {
        final MutableLongSet result = new LongHashSet();
        while ( iter.hasNext() )
        {
            result.add( iter.next() );
        }
        return result;
    }

    private DynamicTest testIteratorFails( String name, Consumer<MutableLinearProbeLongHashSet> mutator, long... initialValues )
    {
        return dynamicTest( name, withNewSet( s ->
        {
            s.addAll( initialValues );

            final MutableLongIterator iterator = s.longIterator();
            assertTrue( iterator.hasNext() );
            assertDoesNotThrow( iterator::next );

            mutator.accept( s );
            assertThrows( ConcurrentModificationException.class, iterator::hasNext );
            assertThrows( ConcurrentModificationException.class, iterator::next );
        } ) );
    }

    private Executable withNewSet( Consumer<MutableLinearProbeLongHashSet> test )
    {
        return () ->
        {
            try ( MutableLinearProbeLongHashSet set = newSet() )
            {
                    test.accept( set );
            }
        };
    }

    private ImmutableLongList generateCollisions( int n )
    {
        long v = 1984;
        final MutableLongList elements;
        try ( MutableLinearProbeLongHashSet s = newSet() )
        {
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

    private MutableLinearProbeLongHashSet newSet( long... elements )
    {
        MutableLinearProbeLongHashSet result = new MutableLinearProbeLongHashSet( allocator );
        result.addAll( elements );
        return result;
    }
}
