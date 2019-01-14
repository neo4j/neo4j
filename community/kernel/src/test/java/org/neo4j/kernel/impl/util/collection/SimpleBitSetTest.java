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
package org.neo4j.kernel.impl.util.collection;

import org.junit.Test;

import java.util.List;

import org.neo4j.collection.primitive.PrimitiveIntCollections;

import static java.util.Arrays.asList;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class SimpleBitSetTest
{

    @Test
    public void put()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 16 );

        // When
        set.put( 2 );
        set.put( 7 );
        set.put( 15 );

        // Then
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 6 ) );
        assertFalse( set.contains( 14 ) );

        assertTrue( set.contains( 2 ) );
        assertTrue( set.contains( 7 ) );
        assertTrue( set.contains( 15 ) );
    }

    @Test
    public void putAndRemove()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 16 );

        // When
        set.put( 2 );
        set.put( 7 );
        set.remove( 2 );

        // Then
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 6 ) );
        assertFalse( set.contains( 14 ) );
        assertFalse( set.contains( 2 ) );

        assertTrue( set.contains( 7 ) );
    }

    @Test
    public void putOtherBitSet()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 16 );
        SimpleBitSet otherSet = new SimpleBitSet( 16 );

        otherSet.put( 4 );
        otherSet.put( 14 );

        set.put( 3 );
        set.put( 4 );

        // When
        set.put( otherSet );

        // Then
        assertFalse( set.contains( 0 ) );
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 15 ) );
        assertFalse( set.contains( 7 ) );

        assertTrue( set.contains( 3 ) );
        assertTrue( set.contains( 4 ) );
        assertTrue( set.contains( 14 ) );
    }

    @Test
    public void removeOtherBitSet()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 16 );
        SimpleBitSet otherSet = new SimpleBitSet( 16 );

        otherSet.put( 4 );
        otherSet.put( 12 );
        otherSet.put( 14 );

        set.put( 3 );
        set.put( 4 );
        set.put( 12 );

        // When
        set.remove( otherSet );

        // Then
        assertFalse( set.contains( 0 ) );
        assertFalse( set.contains( 1 ) );
        assertFalse( set.contains( 4 ) );
        assertFalse( set.contains( 14 ) );

        assertTrue( set.contains( 3 ) );
    }

    @Test
    public void resize()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 8 );

        // When
        set.put( 128 );

        // Then
        assertTrue( set.contains( 128 ) );

        assertFalse( set.contains( 126 ));
        assertFalse( set.contains( 129 ));
    }

    @Test
    public void shouldAllowIterating()
    {
        // Given
        SimpleBitSet set = new SimpleBitSet( 64 );
        set.put( 4 );
        set.put( 7 );
        set.put( 63 );
        set.put( 78 );

        // When
        List<Integer> found = PrimitiveIntCollections.toList( set.iterator() );

        // Then
        assertThat( found, equalTo( asList( 4, 7, 63, 78 ) ));
    }

    @Test
    public void checkPointOnUnchangedSetMustDoNothing()
    {
        SimpleBitSet set = new SimpleBitSet( 16 );
        int key = 10;
        set.put( key );
        long checkpoint = 0;
        checkpoint = set.checkPointAndPut( checkpoint, key );
        assertThat( set.checkPointAndPut( checkpoint, key ), is( checkpoint ) );
        assertTrue( set.contains( key ) );
    }

    @Test
    public void checkPointOnUnchangedSetButWithDifferentKeyMustUpdateSet()
    {
        SimpleBitSet set = new SimpleBitSet( 16 );
        int key = 10;
        set.put( key );
        long checkpoint = 0;
        checkpoint = set.checkPointAndPut( checkpoint, key );
        assertThat( set.checkPointAndPut( checkpoint, key + 1 ), is( not( checkpoint ) ) );
        assertTrue( set.contains( key + 1 ) );
        assertFalse( set.contains( key ) );
    }

    @Test
    public void checkPointOnChangedSetMustClearState()
    {
        SimpleBitSet set = new SimpleBitSet( 16 );
        int key = 10;
        set.put( key );
        long checkpoint = 0;
        checkpoint = set.checkPointAndPut( checkpoint, key );
        set.put( key + 1 );
        assertThat( set.checkPointAndPut( checkpoint, key ), is( not( checkpoint ) ) );
        assertTrue( set.contains( key ) );
        assertFalse( set.contains( key + 1 ) );
    }

    @Test
    public void checkPointMustBeAbleToExpandCapacity()
    {
        SimpleBitSet set = new SimpleBitSet( 16 );
        int key = 10;
        int key2 = 255;
        set.put( key );
        long checkpoint = 0;
        checkpoint = set.checkPointAndPut( checkpoint, key );
        assertThat( set.checkPointAndPut( checkpoint, key2 ), is( not( checkpoint ) ) );
        assertTrue( set.contains( key2 ) );
        assertFalse( set.contains( key ) );
    }

    @Test
    public void modificationsMustTakeWriteLocks()
    {
        // We can observe that a write lock was taken, by seeing that an optimistic read lock was invalidated.
        SimpleBitSet set = new SimpleBitSet( 16 );
        long stamp = set.tryOptimisticRead();

        set.put( 8 );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        set.put( 8 );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        SimpleBitSet other = new SimpleBitSet( 16 );
        other.put( 3 );
        set.put( other );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        set.remove( 3 );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        set.remove( 3 );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        other.put( 8 );
        set.remove( other );
        assertFalse( set.validate( stamp ) );
        stamp = set.tryOptimisticRead();

        other.put( 8 );
        set.remove( other );
        assertFalse( set.validate( stamp ) );
    }
}
