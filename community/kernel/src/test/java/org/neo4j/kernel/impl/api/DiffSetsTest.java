/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.neo4j.helpers.collection.IteratorUtil.asSet;
import static org.neo4j.helpers.collection.IteratorUtil.iterator;

import org.junit.Test;
import org.neo4j.helpers.Predicate;

public class DiffSetsTest
{
    @Test
    public void testAdd()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<Long>();

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
        DiffSets<Long> actual = new DiffSets<Long>();

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
        DiffSets<Long> actual = new DiffSets<Long>();

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
        DiffSets<Long> actual = new DiffSets<Long>();

        // WHEN
        actual.remove( 1L );
        actual.add( 1L );

        // THEN
        assertEquals( asSet( 1L ), actual.getAdded() );
        assertTrue( actual.getRemoved().isEmpty() );
    }

    @Test
    public void testIsAddedOrRemoved()
    {
        // GIVEN
        DiffSets<Long> actual = new DiffSets<Long>();

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
        DiffSets<Long> actual = new DiffSets<Long>();

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
        DiffSets<Long> actual = new DiffSets<Long>();
        actual.addAll( iterator( 1L, 2L ) );
        actual.removeAll( iterator( 3L, 4L ) );

        // WHEN
        DiffSets<Long> filtered = actual.filterAdded( ODD_FILTER );

        // THEN
        assertEquals( asSet( 1L ), filtered.getAdded() );
        assertEquals( asSet( 3L, 4L ), filtered.getRemoved() );
    }

    private static final Predicate<Long> ODD_FILTER = new Predicate<Long>()
    {
        @Override
        public boolean accept( Long item )
        {
            return item % 2 == 1l;
        }
    };
}
