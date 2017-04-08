/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyInt;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Matchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NumberArrayFactoryTest
{
    private static final long KILO = 1024;

    @Test
    public void shouldPickFirstAvailableCandidateLongArray() throws Exception
    {
        // GIVEN
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NumberArrayFactory.HEAP );

        // WHEN
        LongArray array = factory.newLongArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        assertTrue( array instanceof HeapLongArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldPickFirstAvailableCandidateLongArrayWhenSomeDontHaveEnoughMemory() throws Exception
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newLongArray( anyLong(), anyLong(), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( lowMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        LongArray array = factory.newLongArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        verify( lowMemoryFactory, times( 1 ) ).newLongArray( KILO, -1, 0 );
        assertTrue( array instanceof HeapLongArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldThrowOomOnNotEnoughMemory() throws Exception
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newLongArray( anyLong(), anyLong(), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( lowMemoryFactory );

        // WHEN
        try
        {
            factory.newLongArray( KILO, -1 );
            fail( "Should have thrown" );
        }
        catch ( OutOfMemoryError e )
        {
            // THEN OK
        }
    }

    @Test
    public void shouldPickFirstAvailableCandidateIntArray() throws Exception
    {
        // GIVEN
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NumberArrayFactory.HEAP );

        // WHEN
        IntArray array = factory.newIntArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        assertTrue( array instanceof HeapIntArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldPickFirstAvailableCandidateIntArrayWhenSomeDontHaveEnoughMemory() throws Exception
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newIntArray( anyLong(), anyInt(), anyInt() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( lowMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        IntArray array = factory.newIntArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        verify( lowMemoryFactory, times( 1 ) ).newIntArray( KILO, -1, 0 );
        assertTrue( array instanceof HeapIntArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldEvenCatchOtherExceptionsAndTryNext() throws Exception
    {
        // GIVEN
        NumberArrayFactory throwingMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( ArithmeticException.class ).when( throwingMemoryFactory )
                .newByteArray( anyLong(), any( byte[].class ), anyInt() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( throwingMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        ByteArray array = factory.newByteArray( KILO, new byte[4], 0 );
        array.setInt( KILO - 10, 0, 12345 );

        // THEN
        verify( throwingMemoryFactory, times( 1 ) ).newByteArray( eq( KILO ), any( byte[].class ), eq( 0L ) );
        assertTrue( array instanceof HeapByteArray );
        assertEquals( 12345, array.getInt( KILO - 10, 0 ) );
    }
}
