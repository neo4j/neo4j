/**
 * Copyright (c) 2002-2014 "Neo Technology,"
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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import static org.neo4j.unsafe.impl.batchimport.cache.IntArray.intArray;
import static org.neo4j.unsafe.impl.batchimport.cache.LongArrayFactory.HEAP;

public class IntArrayTest
{
    @Test
    public void shouldBeAbleToKeepTwoIntValuesInSameLong() throws Exception
    {
        // GIVEN
        int defaultValue = 5;
        IntArray array = intArray( HEAP.newDynamicLongArray( 1000 ) );
        array.setAll( defaultValue );

        // WHEN/THEN
        for ( int i = 0; i < 100; i++ )
        {
            assertEquals( defaultValue, array.get( i ) );
        }

        array.set( 2, Integer.MAX_VALUE-8 );
        assertEquals( defaultValue, array.get( 3 ) );
        assertEquals( Integer.MAX_VALUE-8, array.get( 2 ) );

        array.set( defaultValue, Integer.MAX_VALUE-12 );
        assertEquals( Integer.MAX_VALUE-12, array.get( defaultValue ) );
        assertEquals( defaultValue, array.get( 4 ) );
    }
}
