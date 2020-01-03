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

import org.eclipse.collections.api.block.procedure.primitive.LongProcedure;
import org.eclipse.collections.api.iterator.LongIterator;
import org.eclipse.collections.api.iterator.MutableLongIterator;
import org.eclipse.collections.api.list.primitive.ImmutableLongList;
import org.eclipse.collections.api.list.primitive.MutableLongList;
import org.eclipse.collections.api.set.primitive.LongSet;
import org.eclipse.collections.api.set.primitive.MutableLongSet;
import org.eclipse.collections.impl.factory.primitive.LongLists;
import org.eclipse.collections.impl.factory.primitive.LongSets;
import org.eclipse.collections.impl.set.mutable.primitive.LongHashSet;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

import java.util.ConcurrentModificationException;
import java.util.NoSuchElementException;
import java.util.function.Consumer;

import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryAllocationTracker;
import org.neo4j.test.extension.Inject;
import org.neo4j.test.extension.RandomExtension;
import org.neo4j.test.rule.RandomRule;

import static org.apache.commons.lang3.ArrayUtils.EMPTY_LONG_ARRAY;
import static org.apache.commons.lang3.ArrayUtils.shuffle;
import static org.eclipse.collections.impl.list.mutable.primitive.LongArrayList.newListWith;
import static org.eclipse.collections.impl.set.mutable.primitive.LongHashSet.newSetWith;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.util.collection.MutableLinearProbeLongHashSet.DEFAULT_CAPACITY;
import static org.neo4j.kernel.impl.util.collection.MutableLinearProbeLongHashSet.REMOVALS_RATIO;

@ExtendWith( RandomExtension.class )
class MutableLinearProbeLongHashSetTest
{
    @Inject
    private RandomRule rnd;

    private final CachingOffHeapBlockAllocator blockAllocator = new CachingOffHeapBlockAllocator();
    private final MemoryAllocationTracker memoryTracker = new LocalMemoryTracker();
    private final MemoryAllocator memoryAllocator = new OffHeapMemoryAllocator( memoryTracker, blockAllocator );

    private MutableLinearProbeLongHashSet set = new MutableLinearProbeLongHashSet( memoryAllocator );

    @AfterEach
    void afterEach()
    {
        set.close();
        assertEquals( 0, memoryTracker.usedDirectMemory(), "Leaking memory" );
        blockAllocator.release();
    }

    @Test
    void addRemoveContains()
    {
        set = spy( set );

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
        set = spy( set );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            assertTrue( set.add( 100 + i ) );
        }
        verify( set ).growAndRehash();
    }

    @Test
    void rehashWhenTooManyRemovals()
    {
        set = spy( set );

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
        final MemoryAllocationTracker memoryTrackerSpy = spy( new LocalMemoryTracker() );
        final MutableLinearProbeLongHashSet set2 = new MutableLinearProbeLongHashSet( new OffHeapMemoryAllocator( memoryTrackerSpy, blockAllocator ) );

        verify( memoryTrackerSpy ).allocated( anyLong() );

        for ( int i = 0; i < DEFAULT_CAPACITY; i++ )
        {
            set2.add( 100 + i );
        }
        verify( memoryTrackerSpy ).deallocated( anyLong() );
        verify( memoryTrackerSpy, times( 2 ) ).allocated( anyLong() );

        set2.close();
        verify( memoryTrackerSpy, times( 2 ) ).deallocated( anyLong() );
    }

    @Test
    void freeFrozenMemory()
    {
        final MemoryAllocationTracker memoryTrackerSpy = spy( new LocalMemoryTracker() );
        final MutableLinearProbeLongHashSet set2 = new MutableLinearProbeLongHashSet( new OffHeapMemoryAllocator( memoryTrackerSpy, blockAllocator ) );

        verify( memoryTrackerSpy ).allocated( anyLong() );

        set2.addAll( 100, 200, 300 );
        set2.freeze();
        set2.remove( 100 );
        set2.freeze();
        set2.clear();
        set2.close();
        verify( memoryTrackerSpy, times( 3 ) ).deallocated( anyLong() );
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
        final MutableLinearProbeLongHashSet set2 = new MutableLinearProbeLongHashSet( memoryAllocator );
        set2.addAll( 300, 200, 100, 3, 2, 1 );
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

    @Test
    void randomizedTest()
    {
        final int count = 10000 + rnd.nextInt( 1000 );

        final MutableLongSet uniqueValues = new LongHashSet();
        while ( uniqueValues.size() < count )
        {
            uniqueValues.add( rnd.nextLong() );
        }

        final long[] values = uniqueValues.toArray();

        for ( long v : values )
        {
            assertTrue( set.add( v ) );
        }
        shuffle( values );
        for ( long v : values )
        {
            assertTrue( set.contains( v ) );
            assertFalse( set.add( v ) );
        }
        assertTrue( set.containsAll( values ) );

        final long[] toRemove = uniqueValues.select( v -> rnd.nextInt( 100 ) < 75 ).toArray();
        shuffle( toRemove );

        for ( long v : toRemove )
        {
            assertTrue( set.remove( v ) );
            assertFalse( set.contains( v ) );
        }

        assertEquals( count - toRemove.length, set.size() );
    }

    @Nested
    class Collisions
    {
        private final ImmutableLongList collisions = generateCollisions( 5 );
        private final long a = collisions.get( 0 );
        private final long b = collisions.get( 1 );
        private final long c = collisions.get( 2 );
        private final long d = collisions.get( 3 );
        private final long e = collisions.get( 4 );

        private ImmutableLongList generateCollisions( int n )
        {
            final long seed = rnd.nextLong();
            final MutableLongList elements;
            try ( MutableLinearProbeLongHashSet s = new MutableLinearProbeLongHashSet( memoryAllocator ) )
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
            set.addAll( collisions );
            assertEquals( collisions, set.toList() );
        }

        @Test
        void addAllReversed()
        {
            set.addAll( collisions.toReversed() );
            assertEquals( collisions.toReversed(), set.toList() );
        }

        @Test
        void addAllRemoveLast()

        {
            set.addAll( collisions );
            set.remove( e );
            assertEquals( newListWith( a, b, c, d ), set.toList() );
        }

        @Test
        void addAllRemoveFirst()

        {
            set.addAll( collisions );
            set.remove( a );
            assertEquals( newListWith( b, c, d, e ), set.toList() );
        }

        @Test
        void addAllRemoveMiddle()

        {
            set.addAll( collisions );
            set.removeAll( b, d );
            assertEquals( newListWith( a, c, e ), set.toList() );
        }

        @Test
        void addAllRemoveMiddle2()
        {
            set.addAll( collisions );
            set.removeAll( a, c, e );
            assertEquals( newListWith( b, d ), set.toList() );
        }

        @Test
        void addReusesRemovedHead()
        {
            set.addAll( a, b, c );

            set.remove( a );
            assertEquals( newListWith( b, c ), set.toList() );

            set.add( d );
            assertEquals( newListWith( d, b, c ), set.toList() );
        }

        @Test
        void addReusesRemovedTail()
        {
            set.addAll( a, b, c );

            set.remove( c );
            assertEquals( newListWith( a, b ), set.toList() );

            set.add( d );
            assertEquals( newListWith( a, b, d ), set.toList() );
        }

        @Test
        void addReusesRemovedMiddle()

        {
            set.addAll( a, b, c );

            set.remove( b );
            assertEquals( newListWith( a, c ), set.toList() );

            set.add( d );
            assertEquals( newListWith( a, d, c ), set.toList() );
        }

        @Test
        void addReusesRemovedMiddle2()
        {
            set.addAll( a, b, c, d, e );

            set.removeAll( b, c );
            assertEquals( newListWith( a, d, e ), set.toList() );

            set.addAll( c, b );
            assertEquals( newListWith( a, c, b, d, e ), set.toList() );
        }

        @Test
        void rehashingCompactsSparseSentinels()
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
        }
    }

    @Nested
    class IterationConcurrentModification
    {
        @Test
        void add()
        {
            testIteratorFails( s -> s.add( 0 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.add( 1 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.add( 0 ), 1, 2, 3 );
            testIteratorFails( s -> s.add( 1 ), 0, 2, 3 );
            testIteratorFails( s -> s.add( 2 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.add( 4 ), 0, 1, 2, 3 );
        }

        @Test
        void remove()
        {
            testIteratorFails( s -> s.remove( 0 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.remove( 1 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.remove( 2 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.remove( 4 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( LongSets.immutable.empty() ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( EMPTY_LONG_ARRAY ), 0, 1, 2, 3 );
        }

        @Test
        void addAll()
        {
            testIteratorFails( s -> s.addAll( 0, 2 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.addAll( 4, 5 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.addAll( LongSets.immutable.of( 0, 2 ) ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.addAll( LongSets.immutable.of( 4, 5 ) ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.addAll( LongSets.immutable.empty() ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.addAll( EMPTY_LONG_ARRAY ), 0, 1, 2, 3 );
        }

        @Test
        void removeAll()
        {
            testIteratorFails( s -> s.removeAll( 0, 2 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( 4, 5 ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( LongSets.immutable.of( 0, 2 ) ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( LongSets.immutable.of( 4, 5 ) ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( LongSets.immutable.empty() ), 0, 1, 2, 3 );
            testIteratorFails( s -> s.removeAll( EMPTY_LONG_ARRAY ), 0, 1, 2, 3 );
        }

        @Test
        void clear()
        {
            testIteratorFails( MutableLinearProbeLongHashSet::clear, 1, 2, 3 );
        }

        @Test
        void close()
        {
            testIteratorFails( MutableLinearProbeLongHashSet::close, 1, 2, 3 );
        }

        private void testIteratorFails( Consumer<MutableLinearProbeLongHashSet> mutator, long... initialValues )
        {
            set.addAll( initialValues );

            final MutableLongIterator iterator = set.longIterator();
            assertTrue( iterator.hasNext() );
            assertDoesNotThrow( iterator::next );

            mutator.accept( set );
            assertThrows( ConcurrentModificationException.class, iterator::hasNext );
            assertThrows( ConcurrentModificationException.class, iterator::next );
        }
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
}
