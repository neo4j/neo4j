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
package org.neo4j.kernel.impl.api;

import java.util.Collection;
import java.util.Iterator;

import org.junit.Test;

import org.neo4j.function.Predicate;
import org.neo4j.kernel.impl.util.diffsets.DiffSets;

import static java.util.Arrays.asList;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.IsCollectionContaining.hasItems;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import static org.neo4j.helpers.collection.IteratorUtil.asCollection;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;

public class DiffSetsTest
{
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
    public void testReturnSourceFromApplyWithEmptyDiffSets() throws Exception
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets();

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 18l ).iterator() );

        // THEN
        assertEquals( asList( 18l ), asCollection( result ) );

    }

    @Test
    public void testAppendAddedToSourceInApply() throws Exception
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.add( 52l );
        diffSets.remove( 43l );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 18l ).iterator() );

        // THEN
        assertEquals( asList( 18l, 52l ), asCollection( result ) );

    }

    @Test
    public void testFilterRemovedFromSourceInApply() throws Exception
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.remove( 43l );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42l, 43l, 44l ).iterator() );

        // THEN
        assertEquals( asList( 42l, 44l ), asCollection( result ) );

    }

    @Test
    public void testFilterAddedFromSourceInApply() throws Exception
    {
        // GIVEN
        DiffSets<Long> diffSets = new DiffSets<>();
        diffSets.add( 42l );
        diffSets.add( 44l );

        // WHEN
        Iterator<Long> result = diffSets.apply( asList( 42l, 43l ).iterator() );

        // THEN
        Collection<Long> collectedResult = asCollection( result );
        assertEquals( 3, collectedResult.size() );
        assertThat( collectedResult, hasItems( 43l, 42l, 44l ) );
    }

    @Test
    public void replaceMultipleTimesWithAnInitialValue() throws Exception
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
    public void replaceMultipleTimesWithNoInitialValue() throws Exception
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

    private static final Predicate<Long> ODD_FILTER = new Predicate<Long>()
    {
        @Override
        public boolean test( Long item )
        {
            return item % 2 == 1l;
        }
    };
}
