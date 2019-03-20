/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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

import org.neo4j.unsafe.impl.internal.dragons.NativeMemoryAllocationRefusedError;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
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
import static org.neo4j.helpers.collection.Iterables.single;
import static org.neo4j.unsafe.impl.batchimport.cache.NumberArrayFactory.NO_MONITOR;

public class NumberArrayFactoryTest
{
    private static final long KILO = 1024;

    @Test
    public void shouldPickFirstAvailableCandidateLongArray()
    {
        // GIVEN
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, NumberArrayFactory.HEAP );

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
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, lowMemoryFactory, NumberArrayFactory.HEAP );

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
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newLongArray( anyLong(), anyLong(), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( monitor, lowMemoryFactory );

        // WHEN
        try
        {
            factory.newLongArray( KILO, -1 );
            fail( "Should have thrown" );
        }
        catch ( OutOfMemoryError e )
        {
            // THEN OK
            assertFalse( monitor.called );
        }
    }

    @Test
    public void shouldPickFirstAvailableCandidateIntArray()
    {
        // GIVEN
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory factory = new NumberArrayFactory.Auto( monitor, NumberArrayFactory.HEAP );

        // WHEN
        IntArray array = factory.newIntArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        assertTrue( array instanceof HeapIntArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
        assertEquals( NumberArrayFactory.HEAP, monitor.successfulFactory );
        assertFalse( monitor.attemptedAllocationFailures.iterator().hasNext() );
    }

    @Test
    public void shouldPickFirstAvailableCandidateIntArrayWhenSomeThrowOutOfMemoryError()
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( OutOfMemoryError.class ).when( lowMemoryFactory ).newIntArray( anyLong(), anyInt(), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, lowMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        IntArray array = factory.newIntArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        verify( lowMemoryFactory, times( 1 ) ).newIntArray( KILO, -1, 0 );
        assertTrue( array instanceof HeapIntArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldPickFirstAvailableCandidateIntArrayWhenSomeThrowNativeMemoryAllocationRefusedError()
    {
        // GIVEN
        NumberArrayFactory lowMemoryFactory = mock( NumberArrayFactory.class );
        doThrow( NativeMemoryAllocationRefusedError.class ).when( lowMemoryFactory ).newIntArray( anyLong(), anyInt(), anyLong() );
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, lowMemoryFactory, NumberArrayFactory.HEAP );

        // WHEN
        IntArray array = factory.newIntArray( KILO, -1 );
        array.set( KILO - 10, 12345 );

        // THEN
        verify( lowMemoryFactory, times( 1 ) ).newIntArray( KILO, -1, 0 );
        assertTrue( array instanceof HeapIntArray );
        assertEquals( 12345, array.get( KILO - 10 ) );
    }

    @Test
    public void shouldCatchArithmeticExceptionsAndTryNext()
    {
        // GIVEN
        NumberArrayFactory throwingMemoryFactory = mock( NumberArrayFactory.class );
        ArithmeticException failure = new ArithmeticException( "This is an artificial failure" );
        doThrow( failure ).when( throwingMemoryFactory ).newByteArray( anyLong(), any( byte[].class ), anyLong() );
        FailureMonitor monitor = new FailureMonitor();
        NumberArrayFactory factory = new NumberArrayFactory.Auto( monitor, throwingMemoryFactory, NumberArrayFactory.HEAP );
        int itemSize = 4;

        // WHEN
        ByteArray array = factory.newByteArray( KILO, new byte[itemSize], 0 );
        array.setInt( KILO - 10, 0, 12345 );

        // THEN
        verify( throwingMemoryFactory, times( 1 ) ).newByteArray( eq( KILO ), any( byte[].class ), eq( 0L ) );
        assertTrue( array instanceof HeapByteArray );
        assertEquals( 12345, array.getInt( KILO - 10, 0 ) );
        assertEquals( KILO * itemSize, monitor.memory );
        assertEquals( NumberArrayFactory.HEAP, monitor.successfulFactory );
        assertEquals( throwingMemoryFactory, single( monitor.attemptedAllocationFailures ).getFactory() );
        assertThat( single( monitor.attemptedAllocationFailures ).getFailure().getMessage(), containsString( failure.getMessage() ) );
    }

    @Test
    public void heapArrayShouldAllowVeryLargeBases()
    {
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, NumberArrayFactory.HEAP );
        verifyVeryLargeBaseSupport( factory );
    }

    @Test
    public void offHeapArrayShouldAllowVeryLargeBases()
    {
        NumberArrayFactory factory = new NumberArrayFactory.Auto( NO_MONITOR, NumberArrayFactory.OFF_HEAP );
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

    private static class FailureMonitor implements NumberArrayFactory.Monitor
    {
        private boolean called;
        private long memory;
        private NumberArrayFactory successfulFactory;
        private Iterable<NumberArrayFactory.AllocationFailure> attemptedAllocationFailures;

        @Override
        public void allocationSuccessful( long memory, NumberArrayFactory successfulFactory,
                Iterable<NumberArrayFactory.AllocationFailure> attemptedAllocationFailures )
        {
            this.memory = memory;
            this.successfulFactory = successfulFactory;
            this.attemptedAllocationFailures = attemptedAllocationFailures;
            this.called = true;
        }
    }
}
