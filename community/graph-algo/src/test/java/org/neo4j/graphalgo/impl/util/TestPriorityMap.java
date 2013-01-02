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
package org.neo4j.graphalgo.impl.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;

import org.junit.Test;
import org.neo4j.graphalgo.impl.util.PriorityMap.Entry;

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

    private void assertEntry( Entry<Integer, Double> entry, Integer entity,
            Double priority )
    {
        assertNotNull( entry );
        assertEquals( entity, entry.getEntity() );
        assertEquals( priority, entry.getPriority() );
    }
}
