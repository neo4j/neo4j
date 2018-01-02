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
package org.neo4j.graphalgo.impl.util;

import org.junit.Test;

import org.neo4j.graphalgo.impl.util.PriorityMap.Entry;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

public class TestPriorityMap
{
    @Test
    public void testIt()
    {
        PriorityMap<Integer, Integer, Double> map =
            PriorityMap.<Integer, Double>withSelfKeyNaturalOrder();
        map.put( 0, 5d );
        map.put( 1, 4d );
        map.put( 1, 4d );
        map.put( 1, 3d );
        assertEntry( map.pop(), 1, 3d );
        assertEntry( map.pop(), 0, 5d );
        assertNull( map.pop() );

        int start = 0, a = 1, b = 2, c = 3, d = 4, e = 6, f = 7, y = 8, x = 9;
        map.put( start, 0d );
        map.put( a, 1d );
        // get start
        // get a
        map.put( x, 10d );
        map.put( b, 2d );
        // get b
        map.put( x, 9d );
        map.put( c, 3d );
        // get c
        map.put( x, 8d );
        map.put( x, 6d );
        map.put( d, 4d );
        // get d
        map.put( x, 7d );
        map.put( e, 5d );
        // get e
        map.put( x, 6d );
        map.put( f, 7d );
        // get x
        map.put( y, 8d );
        // get x
//        map.put(
    }

    @Test
    public void shouldReplaceIfBetter() throws Exception
    {
        // GIVEN
        PriorityMap<Integer, Integer, Double> map = PriorityMap.<Integer, Double>withSelfKeyNaturalOrder();
        map.put( 1, 2d );

        // WHEN
        boolean putResult = map.put( 1, 1.5d );

        // THEN
        assertTrue( putResult );
        Entry<Integer, Double> top = map.pop();
        assertNull( map.peek() );
        assertEquals( 1, top.getEntity().intValue() );
        assertEquals( 1.5d, top.getPriority().doubleValue(), 0d );
    }

    @Test
    public void shouldKeepAllPrioritiesIfToldTo() throws Exception
    {
        // GIVEN
        int entity = 5;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.<Integer, Double>withSelfKeyNaturalOrder( false, false );
        assertTrue( map.put( entity, 3d ) );
        assertTrue( map.put( entity, 2d ) );

        // WHEN
        assertTrue( map.put( entity, 5d ) );
        assertTrue( map.put( entity, 4d ) );

        // THEN
        assertEntry( map.pop(), entity, 2d );
        assertEntry( map.pop(), entity, 3d );
        assertEntry( map.pop(), entity, 4d );
        assertEntry( map.pop(), entity, 5d );
    }

    @Test
    public void inCaseSaveAllPrioritiesShouldHandleNewEntryWithWorsePrio()
    {
        // GIVEN
        int first = 1;
        int second = 2;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.<Integer, Double>withSelfKeyNaturalOrder( false, false);

        // WHEN
        assertTrue( map.put( first, 1d) );
        assertTrue( map.put( second, 2d) );
        assertTrue( map.put( first, 3d ) );

        // THEN
        assertEntry( map.pop(), first, 1d );
        assertEntry( map.pop(), second, 2d );
        assertEntry( map.pop(), first, 3d );
        assertNull( map.peek() );
    }

    @Test
    public void inCaseSaveAllPrioritiesShouldHandleNewEntryWithBetterPrio()
    {
        // GIVEN
        int first = 1;
        int second = 2;
        PriorityMap<Integer, Integer, Double> map = PriorityMap.<Integer, Double>withSelfKeyNaturalOrder( false, false);

        // WHEN
        assertTrue( map.put( first, 3d) );
        assertTrue( map.put( second, 2d) );
        assertTrue( map.put( first, 1d ) );

        // THEN
        assertEntry( map.pop(), first, 1d );
        assertEntry( map.pop(), second, 2d );
        assertEntry( map.pop(), first, 3d );
        assertNull( map.peek() );
    }

    private void assertEntry( Entry<Integer, Double> entry, Integer entity, Double priority )
    {
        assertNotNull( entry );
        assertEquals( entity, entry.getEntity() );
        assertEquals( priority, entry.getPriority() );
    }
}
