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
package org.neo4j.unsafe.impl.batchimport.cache;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class DynamicIntArrayTest
{
    @Test
    public void shouldWorkOnSingleChunk() throws Exception
    {
        // GIVEN
        int defaultValue = 0;
        IntArray array = NumberArrayFactory.AUTO.newDynamicIntArray( 10, defaultValue );
        array.set( 4, 5 );

        // WHEN
        assertEquals( 5L, array.get( 4 ) );
        assertEquals( defaultValue, array.get( 12 ) );
        array.set( 7, 1324 );
        assertEquals( 1324L, array.get( 7 ) );
    }

    @Test
    public void shouldChunksAsNeeded() throws Exception
    {
        // GIVEN
        IntArray array = NumberArrayFactory.AUTO.newDynamicIntArray( 10, 0 );

        // WHEN
        long index = 243;
        int value = 5485748;
        array.set( index, value );

        // THEN
        assertEquals( value, array.get( index ) );
    }


    @Test
    public void shouldFixate() throws Exception
    {
        // GIVEN
        IntArray array = NumberArrayFactory.AUTO.newDynamicIntArray( 100, 0 );
        array.set( 50, 1 );
        array.set( 500, 1 );

        // WHEN
        array = array.fixate();
        assertEquals( 1, array.get( 50 ) );
        assertEquals( 1, array.get( 500 ) );
        array.set( 499, 10 );
        assertEquals( 10, array.get( 499 ) );
        assertEquals( 0, array.get( 50_000 ) );
        try
        {
            array.set( 650, 9 );
            fail( "Should have been fixated at this point" );
        }
        catch ( ArrayIndexOutOfBoundsException e )
        {
            // THEN good
        }
    }
}
