/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.collection.primitive.versioned;

import org.junit.Test;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

import org.neo4j.collection.primitive.PrimitiveLongIterator;
import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.collection.primitive.hopscotch.Table;

import static org.hamcrest.Matchers.contains;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toIterator;
import static org.neo4j.collection.primitive.PrimitiveLongCollections.toSet;
import static org.neo4j.collection.primitive.PrimitiveLongVisitor.EMPTY;

public class VersionedPrimitiveLongObjectMapTest
{
    public static final Consumer<Object> NOP_CONSUMER = ignore ->
    {
    };
    private final VersionedPrimitiveLongObjectMap<Object> map = new VersionedPrimitiveLongObjectMap<>();
    private final PrimitiveLongObjectMap<Object> currentView = map.currentView();
    private final PrimitiveLongObjectMap<Object> stableView = map.stableView();

    @Test
    public void addAndGetValueFromActiveMap()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        currentView.put( 4L, "d" );

        assertEquals( "a", currentView.get( 1L ) );
        assertEquals( "b", currentView.get( 2L ) );
        assertEquals( "c", currentView.get( 3L ) );
        assertEquals( "d", currentView.get( 4L ) );
    }

    @Test
    public void versionedMapActiveMapSize()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        currentView.put( 4L, "d" );

        assertEquals( 4, currentView.size() );
    }

    @Test
    public void versionedMapActiveMapWithOverrideSize()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 2L, "b2" );
        currentView.put( 3L, "c" );
        currentView.put( 3L, "c2" );
        currentView.put( 4L, "d" );

        assertEquals( 4, currentView.size() );
    }

    @Test
    public void updateCurrentVersionEntry()
    {
        currentView.put( 1L, "a" );
        currentView.put( 1L, "b" );

        assertEquals( "b", currentView.get( 1L ) );
    }

    @Test
    public void activeMapEmptiness()
    {
        assertTrue( currentView.isEmpty() );

        currentView.put( 1L, "a" );

        assertFalse( currentView.isEmpty() );
    }

    @Test
    public void iterateOverActiveMapKeys()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        currentView.put( 4L, "d" );

        Set<Long> keys = toSet( currentView.iterator() );
        assertThat( keys, contains( 1L, 2L, 3L, 4L ) );
    }

    @Test
    public void iterateOverActiveMapValues()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        currentView.put( 4L, "d" );

        assertThat( currentView.values(), contains( "a", "b", "c", "d" ) );
    }

    @Test
    public void addAndGetValuesFromStableView()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        currentView.put( 4L, "d" );
        map.markStable();

        assertEquals( "a", stableView.get( 1L ) );
        assertEquals( "b", stableView.get( 2L ) );
        assertEquals( "c", stableView.get( 3L ) );
        assertEquals( "d", stableView.get( 4L ) );
    }

    @Test
    public void observeDeletedItemInAStableView()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        map.markStable();
        currentView.remove( 2L );

        assertNull( currentView.get( 2L ) );
        assertEquals( "a", stableView.get( 1L ) );
        assertEquals( "b", stableView.get( 2L ) );
        assertEquals( "c", stableView.get( 3L ) );
    }

    @Test
    public void observeNotUpdatedItemInAStableView()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        map.markStable();
        currentView.put( 2L, "poison" );

        assertEquals( "a", stableView.get( 1L ) );
        assertEquals( "b", stableView.get( 2L ) );
        assertEquals( "c", stableView.get( 3L ) );
        assertEquals( "a", currentView.get( 1L ) );
        assertEquals( "poison", currentView.get( 2L ) );
        assertEquals( "c", currentView.get( 3L ) );
    }

    @Test
    public void doNotObserveNewItemInAStableView()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        map.markStable();

        currentView.put( 3L, "c" );
        assertEquals( "a", stableView.get( 1L ) );
        assertEquals( "b", stableView.get( 2L ) );
        assertNull( stableView.get( 3L ) );
    }

    @Test
    public void iterateOverStableViewItems()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );

        map.markStable();
        assertThat( stableView.values(), contains( "a", "b", "c" ) );
    }

    @Test
    public void iterateOverStableViewItemsWithInvisibleNewActiveItem()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        map.markStable();
        currentView.put( 4L, "d" );

        assertThat( stableView.values(), contains( "a", "b", "c" ) );
        assertThat( currentView.values(), contains( "a", "b", "c", "d" ) );
    }

    @Test
    public void removedValuesFromOldStableVersionAreNotVisible()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        map.markStable();
        currentView.remove( 2L );
        map.markStable();
        currentView.remove( 1L );

        assertFalse( "Stable view is empty", stableView.isEmpty() );
        assertFalse( "Active view is empty", currentView.isEmpty() );
        assertEquals( "Stable view size does not match, actual content: " + toString( stableView ), 2, stableView.size() );
        assertEquals( "Active view size does not match, actual content: " + toString( currentView ), 1, currentView.size() );
        assertThat( stableView.values(), contains( "a", "c" ) );
        assertThat( currentView.values(), contains( "c" ) );
    }

    @Test
    public void updatedValuesFromOldStableVersionAreNotVisible()
    {
        currentView.put( 1L, "a" );
        currentView.put( 2L, "b" );
        currentView.put( 3L, "c" );
        map.markStable();
        currentView.put( 2L, "X" );
        map.markStable();
        currentView.put( 1L, "Y" );

        assertFalse( "Stable view is empty", stableView.isEmpty() );
        assertFalse( "Active view is empty", currentView.isEmpty() );
        assertEquals( "Stable view size does not match, actual content: " + toString( stableView ), 3, stableView.size() );
        assertEquals( "Active view size does not match, actual content: " + toString( currentView ), 3, currentView.size() );
        assertThat( stableView.values(), contains( "a", "X", "c" ) );
        assertThat( currentView.values(), contains( "Y", "X", "c" ) );
    }

    @Test
    public void activeViewValuesIteratorInvalidatedWhenVersionChanges()
    {
        map.currentView().put( 1L, "a" );
        map.currentView().put( 2L, "b" );
        map.currentView().put( 3L, "c" );

        final Iterator<Object> iterator = currentView.values().iterator();
        iterator.next();
        map.markStable();
        try
        {
            iterator.next();
            fail( "Iterator must fail" );
        }
        catch ( ConcurrentModificationException ignore )
        {
            // expected
        }
    }

    @Test
    public void stableViewValuesIteratorInvalidatedWhenVersionChanges()
    {
        map.currentView().put( 1L, "a" );
        map.currentView().put( 2L, "b" );
        map.currentView().put( 3L, "c" );
        map.markStable();

        final Iterator<Object> iterator = stableView.values().iterator();
        iterator.next();
        map.markStable();
        try
        {
            iterator.next();
            fail( "Iterator must fail" );
        }
        catch ( ConcurrentModificationException ignore )
        {
            // expected
        }
    }

    @Test
    public void activeViewIteratorsInvalidatedWhenVersionChanges()
    {
        map.currentView().put( 1L, "a" );
        map.currentView().put( 2L, "b" );
        map.currentView().put( 3L, "c" );

        final PrimitiveLongIterator iterator = currentView.iterator();
        iterator.next();
        map.markStable();
        try
        {
            iterator.next();
            fail( "Iterator must fail" );
        }
        catch ( ConcurrentModificationException ignore )
        {
            // expected
        }
    }

    @Test
    public void stableViewIteratorInvalidatedWhenVersionChanges()
    {
        map.currentView().put( 1L, "a" );
        map.currentView().put( 2L, "b" );
        map.currentView().put( 3L, "c" );
        map.markStable();

        final PrimitiveLongIterator iterator = stableView.iterator();
        iterator.next();
        map.markStable();
        try
        {
            iterator.next();
            fail( "Iterator must fail" );
        }
        catch ( ConcurrentModificationException ignore )
        {
            // expected
        }
    }

    @Test
    public void valuesIteratorPurgesOrphanEntriesOnStableViewIteration()
    {
        verifyOrphanPurgingByIterator( stableView, v -> v.values().iterator() );
    }

    @Test
    public void valuesIteratorPurgesOrphanEntriesOnCurrentViewIteration()
    {
        verifyOrphanPurgingByIterator( currentView, v -> v.values().iterator() );
    }

    @Test
    public void mapIteratorPurgesOrphanEntriesOnStableViewIteration()
    {
        verifyOrphanPurgingByIterator( stableView, v -> toIterator( v.iterator() ) );
    }

    @Test
    public void mapIteratorPurgesOrphanEntriesOnCurrentViewIteration()
    {
        verifyOrphanPurgingByIterator( currentView, v -> toIterator( v.iterator() ) );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void activeViewVisitKeysUnsupported()
    {
        map.currentView().visitKeys( EMPTY );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void stableViewVisitKeysUnsupported()
    {
        map.stableView().visitKeys( EMPTY );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void activeViewVisitEntriesUnsupported()
    {
        map.currentView().visitEntries( ( k, v ) -> false );
    }

    @Test( expected = UnsupportedOperationException.class )
    public void stableViewVisitEntriesUnsupported()
    {
        map.stableView().visitEntries( ( k, v ) -> false );
    }

    @SuppressWarnings( "unchecked" )
    private void verifyOrphanPurgingByIterator( PrimitiveLongObjectMap iteratingView, Function<PrimitiveLongObjectMap, Iterator> iteratorGetter )
    {
        final Table<VersionedEntry<Object>> table = map.getLastTable();
        assertEquals( 0, table.size() );

        currentView.put( 1L, "foo" );

        assertEquals( 1, table.size() );

        map.markStable();
        iteratorGetter.apply( iteratingView ).forEachRemaining( NOP_CONSUMER );

        assertEquals( 1, table.size() );

        currentView.remove( 1L );

        assertEquals( 1, table.size() );

        iteratorGetter.apply( iteratingView ).forEachRemaining( NOP_CONSUMER );

        assertEquals( 1, table.size() );

        map.markStable();

        assertEquals( 1, table.size() );

        iteratorGetter.apply( iteratingView ).forEachRemaining( NOP_CONSUMER );

        assertEquals( "orphan entry was not purged", 0, table.size() );
    }

    private void verifyIteratorInvalidationOnVersionChange( Supplier<Iterator> iteratorSupplier )
    {
        final Iterator iterator = iteratorSupplier.get();
        iterator.next();
        map.markStable();
        try
        {
            iterator.next();
            fail( "Iterator must fail" );
        }
        catch ( ConcurrentModificationException ignore )
        {
            // expected
        }
    }

    private static String toString( PrimitiveLongObjectMap<?> map )
    {
        final StringBuilder b = new StringBuilder();
        b.append( '[' );

        final PrimitiveLongIterator iter = map.iterator();
        while ( iter.hasNext() )
        {
            final long key = iter.next();
            b.append( key ).append( '=' ).append( map.get( key ) ).append( ';' );
        }

        b.append( ']' );
        return b.toString();
    }
}
