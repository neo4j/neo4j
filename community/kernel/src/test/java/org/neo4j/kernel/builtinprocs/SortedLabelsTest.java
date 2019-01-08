/*
 * Copyright (c) 2002-2019 "Neo Technology,"
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
package org.neo4j.kernel.builtinprocs;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;

public class SortedLabelsTest
{
    @Test
    public void testEquals()
    {
        int[] intsA = new int[]{1, 2, 3};
        int[] intsB = new int[]{3, 2, 1};
        int[] intsC = new int[]{1, 2, 3, 4};
        SortedLabels a = SortedLabels.from( intsA );
        SortedLabels b = SortedLabels.from( intsB );
        SortedLabels c = SortedLabels.from( intsC );

        // self
        //noinspection EqualsWithItself
        assertEquals( a, a );

        // unordered self
        assertEquals( a, b );
        assertEquals( b, a );

        // other
        assertNotEquals( a, c );
        assertNotEquals( c, a );
    }

    @Test
    public void testHashCodeOfLabelSet()
    {
        int[] intsA = new int[]{1, 2, 3};
        int[] intsB = new int[]{3, 2, 1};
        int[] intsC = new int[]{1, 2, 3, 4};
        SortedLabels a = SortedLabels.from( intsA );
        SortedLabels b = SortedLabels.from( intsB );
        SortedLabels c = SortedLabels.from( intsC );

        assertEquals( a.hashCode(), b.hashCode() );
        assertNotEquals( a.hashCode(), c.hashCode() );
    }
}
