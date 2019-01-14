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

import org.junit.Test;

import java.util.Collection;
import java.util.Iterator;
import java.util.function.Predicate;

import org.neo4j.kernel.impl.util.diffsets.DiffSets;

import static java.util.Arrays.asList;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.Iterators.asCollection;
import static org.neo4j.helpers.collection.Iterators.asSet;
import static org.neo4j.helpers.collection.Iterators.iterator;

public class DiffSetsTest
{
    private static final Predicate<Long> ODD_FILTER = item -> item % 2 == 1L;

    @Test
    public void testAdd()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.add( 1L );
        actual.add( 2L );

        // THEN
        assertEquals( asSet( 1L, 2L ), actual.getAdded() );
        assertTrue( actual.getRemoved().isEmpty() );
    }

    @Test
    public void testRemove()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.add( 1L );
        actual.remove( 2L );

        // THEN
        assertEquals( asSet( 1L ), actual.getAdded() );
        assertEquals( asSet( 2L ), actual.getRemoved() );
    }

    @Test
    public void testAddRemove()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.add( 1L );
        actual.remove( 1L );

        // THEN
        assertTrue( actual.getAdded().isEmpty() );
        assertTrue( actual.getRemoved().isEmpty() );
    }

    @Test
    public void testRemoveAdd()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.remove( 1L );
        actual.add( 1L );

        // THEN
        assertTrue( actual.getAdded().isEmpty() );
        assertTrue( actual.getRemoved().isEmpty() );
    }

    @Test
    public void testIsAddedOrRemoved()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.add( 1L );
        actual.remove( 10L );

        // THEN
        assertTrue( actual.isAdded( 1L ) );
        assertTrue( !actual.isAdded( 2L ) );
        assertTrue( actual.isRemoved( 10L ) );
        assertTrue( !actual.isRemoved( 2L ) );
    }

    @Test
    public void testAddRemoveAll()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();

        // WHEN
        actual.addAll( iterator( 1L, 2L ) );
        actual.removeAll( iterator( 2L, 3L ) );

        // THEN
        assertEquals( asSet( 1L ), actual.getAdded() );
        assertEquals( asSet( 3L ), actual.getRemoved() );
    }

    @Test
    public void testFilterAdded()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<>();
        actual.addAll( iterator( 1L, 2L ) );
        actual.removeAll( iterator( 3L, 4L ) );

        // WHEN
        DiffSets<Long> filtered = actual.filterAdded( ODD_FILTER );

        // THEN
        assertEquals( asSet( 1L ), filtered.getAdded() );
        assertEquals( asSet( 3L, 4L ), filtered.getRemoved() );
    }

    @Test
    public void testReturnSourceFromApplyWithEmptyDiffSets()
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets();

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 18L ).iterator() );

        // THEN
        assertEquals( asList( 18L ), asCollection( result ) );

    }

    @Test
    public void testAppendAddedToSourceInApply()
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.add( 52L );
        diffSets.remove( 43L );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 18L ).iterator() );

        // THEN
        assertEquals( asList( 18L, 52L ), asCollection( result ) );

    }

    @Test
    public void testFilterRemovedFromSourceInApply()
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.remove( 43L );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42L, 43L, 44L ).iterator() );

        // THEN
        assertEquals( asList( 42L, 44L ), asCollection( result ) );

    }

    @Test
    public void testFilterAddedFromSourceInApply()
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.add( 42L );
        diffSets.add( 44L );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42L, 43L ).iterator() );

        // THEN
        Collection<Long> collectedResult = asCollection( result );
        assertEquals( 3, collectedResult.size() );
        assertThat( collectedResult, hasItems( 43L, 42L, 44L ) );
    }

    @Test
    public void replaceMultipleTimesWithAnInitialValue()
    {
        // GIVEN
        // an initial value, meaning an added value in "this transaction"
        DiffSets<Integer> diff = new DiffSets<>();
        diff.add( 0 );

        // WHEN
        // replacing that value two times
        diff.replace( 0, 1 );
        diff.replace( 1, 2 );

        // THEN
        // there should not be any removed value, only the last one added
        assertEquals( asSet( 2 ), diff.getAdded() );
        assertEquals( asSet(), diff.getRemoved() );
    }

    @Test
    public void replaceMultipleTimesWithNoInitialValue()
    {
        // GIVEN
        // no initial value, meaning a value existing before "this transaction"
        DiffSets<Integer> diff = new DiffSets<>();

        // WHEN
        // replacing that value two times
        diff.replace( 0, 1 );
        diff.replace( 1, 2 );

        // THEN
        // the initial value should show up as removed and the last one as added
        assertEquals( asSet( 2 ), diff.getAdded() );
        assertEquals( asSet( 0 ), diff.getRemoved() );
    }
}
