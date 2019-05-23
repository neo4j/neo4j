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
package org.neo4j.kernel.impl.api;

import org.junit.jupiter.api.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.kernel.impl.util.diffsets.MutableDiffSetsImpl;

import static java.util.Arrays.asList;
import static java.util.Collections.singletonList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsIterableContaining.hasItems;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.neo4j.internal.helpers.collection.Iterators.asCollection;
import static org.neo4j.internal.helpers.collection.Iterators.asSet;
import static org.neo4j.internal.helpers.collection.Iterators.iterator;

class MutableDiffSetsImplTest
{
    private static final Predicate<Long> ODD_FILTER = item -> item % 2 != 0;

    private final MutableDiffSetsImpl<Long> diffSets = new MutableDiffSetsImpl<>();

    @Test
    void testAdd()
    {
        // WHEN
        diffSets.add( 1L );
        diffSets.add( 2L );

        // THEN
        assertEquals( asSet( 1L, 2L ), diffSets.getAdded() );
        assertTrue( diffSets.getRemoved().isEmpty() );
    }

    @Test
    void testRemove()
    {
        // WHEN
        diffSets.add( 1L );
        diffSets.remove( 2L );

        // THEN
        assertEquals( asSet( 1L ), diffSets.getAdded() );
        assertEquals( asSet( 2L ), diffSets.getRemoved() );
    }

    @Test
    void testAddRemove()
    {
        // WHEN
        diffSets.add( 1L );
        diffSets.remove( 1L );

        // THEN
        assertTrue( diffSets.getAdded().isEmpty() );
        assertTrue( diffSets.getRemoved().isEmpty() );
    }

    @Test
    void testRemoveAdd()
    {
        // WHEN
        diffSets.remove( 1L );
        diffSets.add( 1L );

        // THEN
        assertTrue( diffSets.getAdded().isEmpty() );
        assertTrue( diffSets.getRemoved().isEmpty() );
    }

    @Test
    void testIsAddedOrRemoved()
    {
        // WHEN
        diffSets.add( 1L );
        diffSets.remove( 10L );

        // THEN
        assertTrue( diffSets.isAdded( 1L ) );
        assertFalse( diffSets.isAdded( 2L ) );
        assertTrue( diffSets.isRemoved( 10L ) );
        assertFalse( diffSets.isRemoved( 2L ) );
    }

    @Test
    void testAddRemoveAll()
    {
        // WHEN
        diffSets.addAll( iterator( 1L, 2L ) );
        diffSets.removeAll( iterator( 2L, 3L ) );

        // THEN
        assertEquals( asSet( 1L ), diffSets.getAdded() );
        assertEquals( asSet( 3L ), diffSets.getRemoved() );
    }

    @Test
    void testFilterAdded()
    {
        // GIVEN
        diffSets.addAll( iterator( 1L, 2L ) );
        diffSets.removeAll( iterator( 3L, 4L ) );

        // WHEN
        MutableDiffSetsImpl<Long> filtered = diffSets.filterAdded( ODD_FILTER );

        // THEN
        assertEquals( asSet( 1L ), filtered.getAdded() );
        assertEquals( asSet( 3L, 4L ), filtered.getRemoved() );
    }

    @Test
    void testReturnSourceFromApplyWithEmptyDiffSets()
    {
        // WHEN
        Iterator<Long> result = diffSets.apply( singletonList( 18L ).iterator() );

        // THEN
        assertEquals( singletonList( 18L ), asCollection( result ) );

    }

    @Test
    void testAppendAddedToSourceInApply()
    {
        // GIVEN
        diffSets.add( 52L );
        diffSets.remove( 43L );

        // WHEN
        Iterator<Long> result = diffSets.apply( singletonList( 18L ).iterator() );

        // THEN
        assertEquals( asList( 18L, 52L ), asCollection( result ) );

    }

    @Test
    void testFilterRemovedFromSourceInApply()
    {
        // GIVEN
        diffSets.remove( 43L );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42L, 43L, 44L ).iterator() );

        // THEN
        assertEquals( asList( 42L, 44L ), asCollection( result ) );

    }

    @Test
    void testFilterAddedFromSourceInApply()
    {
        // GIVEN
        diffSets.add( 42L );
        diffSets.add( 44L );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42L, 43L ).iterator() );

        // THEN
        Collection<Long> collectedResult = asCollection( result );
        assertEquals( 3, collectedResult.size() );
        assertThat( collectedResult, hasItems( 43L, 42L, 44L ) );
    }
}
