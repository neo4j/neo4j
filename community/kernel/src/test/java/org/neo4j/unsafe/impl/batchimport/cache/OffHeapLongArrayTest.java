/**
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import org.neo4j.unsafe.impl.batchimport.cache.LongArray;
import org.neo4j.unsafe.impl.batchimport.cache.OffHeapLongArray;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

public class OffHeapLongArrayTest
{
    @Test
    public void shouldPutAndGetValues() throws Exception
    {
        // GIVEN
        int size = 100;
        LongArray array = new OffHeapLongArray( size );
        array.setAll( -1 );
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( -1L, array.get( i ) );
        }

        // WHEN
        array.set( 10, 100 );
        array.set( 0, 21 );
        array.set( 99, 349389 );

        // THEN
        assertEquals( 100L, array.get( 10 ) );
        assertEquals( 21L, array.get( 0 ) );
        assertEquals( 349389L, array.get( 99 ) );
    }
}
