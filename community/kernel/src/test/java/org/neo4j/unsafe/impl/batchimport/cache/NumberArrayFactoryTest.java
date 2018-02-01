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

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;

public class NumberArrayFactoryTest
{
    private static final long KILO = 1024;

    @Test
    public void shouldPickFirstAvailableCandidateLongArray()
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
    public void shouldPickFirstAvailableCandidateLongArrayWhenSomeDontHaveEnoughMemory()
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
    public void shouldThrowOomOnNotEnoughMemory()
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
    public void shouldPickFirstAvailableCandidateIntArray()
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
    public void shouldPickFirstAvailableCandidateIntArrayWhenSomeDontHaveEnoughMemory()
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newIntArray( anyLong(), anyInt(), anyLong() );
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
    public void shouldEvenCatchOtherExceptionsAndTryNext()
    {
        // GIVEN
        NumberArrayFactory throwingMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( ArithmeticException.class ).when( throwingMemoryFactory )
                .newByteArray( anyLong(), any( byte[].class ), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( throwingMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        ByteArray array = factory.newByteArray( KILO, new byte[4], 0 );
        array.setInt( KILO - 10, 0, 12345 );

        // THEN
        verify( throwingMemoryFactory, times( 1 ) ).newByteArray( eq( KILO ), any( byte[].class ), eq( 0L ) );
        assertTrue( array instanceof HeapByteArray );
        assertEquals( 12345, array.getInt( KILO - 10, 0 ) );
    }

    @Test
    public void heapArrayShouldAllowVeryLargeBases()
    {
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NumberArrayFactory.HEAP );
        verifyVeryLargeBaseSupport( factory );
    }

    @Test
    public void offHeapArrayShouldAllowVeryLargeBases()
    {
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NumberArrayFactory.OFF_HEAP );
        verifyVeryLargeBaseSupport( factory );
    }

    private void verifyVeryLargeBaseSupport( NumberArrayFactory factory )
    {
        long base = Integer.MAX_VALUE * 1337L;
        byte[] into = new byte[1];
        into[0] = 1;
        factory.newByteArray( 10, new byte[1], base ).get( base + 1, into );
        assertThat( into[0], is( (byte) 0 ) );
        assertThat( factory.newIntArray( 10, 1, base ).get( base + 1 ), is( 1 ) );
        assertThat( factory.newLongArray( 10, 1, base ).get( base + 1 ), is( 1L ) );
    }
}
