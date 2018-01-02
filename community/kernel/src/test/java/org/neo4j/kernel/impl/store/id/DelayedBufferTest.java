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
package org.neo4j.kernel.impl.store.id;

import org.junit.Test;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.function.Consumer;
import org.neo4j.function.Predicate;
import org.neo4j.function.Predicates;
import org.neo4j.function.Supplier;
import org.neo4j.helpers.Clock;
import org.neo4j.test.Race;
import static java.util.concurrent.ThreadLocalRandom.current;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.locks.LockSupport.parkNanos;

import static org.neo4j.function.Suppliers.singleton;
import static org.neo4j.unsafe.impl.batchimport.Utils.safeCastLongToInt;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verifyNoMoreInteractions;

public class DelayedBufferTest
{
    @Test
    public void shouldHandleTheWholeWorkloadShebang() throws Throwable
    {
        // GIVEN
        final int size = 1_000;
        final long bufferTime = 3;
        VerifyingConsumer consumer = new VerifyingConsumer( size );
        final Clock clock = Clock.SYSTEM_CLOCK;
        Supplier<Long> chunkThreshold = new Supplier<Long>()
        {
            @Override
            public Long get()
            {
                return clock.currentTimeMillis();
            }
        };
        Predicate<Long> safeThreshold = new Predicate<Long>()
        {
            @Override
            public boolean test( Long time )
            {
                return clock.currentTimeMillis() - bufferTime >= time;
            }
        };
        final DelayedBuffer<Long> buffer = new DelayedBuffer<>( chunkThreshold, safeThreshold, 10, consumer );
        MaintenanceThread maintenance = new MaintenanceThread( buffer, 5 );
        Race adders = new Race();
        final int numberOfAdders = 20;
        final byte[] offeredIds = new byte[size];
        for ( int i = 0; i < numberOfAdders; i++ )
        {
            final int finalI = i;
            adders.addContestant( new Runnable()
            {
                @Override
                public void run()
                {
                    for ( int j = 0; j < size; j++ )
                    {
                        if ( j % numberOfAdders == finalI )
                        {
                            buffer.offer( j );
                            offeredIds[j] = 1;
                            parkNanos( MILLISECONDS.toNanos( current().nextInt( 2 ) ) );
                        }
                    }
                }
            } );
        }

        // WHEN (multi-threadded) offering of ids
        adders.go();
        // ... ensuring the test is sane itself (did we really offer all these IDs?)
        for ( int i = 0; i < size; i++ )
        {
            assertEquals( "ID " + i, (byte) 1, offeredIds[i] );
        }
        maintenance.halt();
        buffer.close();

        // THEN
        consumer.assertHaveOnlySeenRange( 0, size-1 );
    }

    @Test
    public void shouldNotReleaseValuesUntilCrossedThreshold() throws Exception
    {
        // GIVEN
        VerifyingConsumer consumer = new VerifyingConsumer( 30 );
        final AtomicLong txOpened = new AtomicLong();
        final AtomicLong txClosed = new AtomicLong();
        Supplier<Long> chunkThreshold = new Supplier<Long>()
        {
            @Override
            public Long get()
            {
                return txOpened.get();
            }
        };
        Predicate<Long> safeThreshold = new Predicate<Long>()
        {
            @Override
            public boolean test( Long value )
            {
                return txClosed.get() >= value;
            }
        };
        DelayedBuffer<Long> buffer = new DelayedBuffer<>( chunkThreshold, safeThreshold, 100, consumer );

        // Transaction spans like these:
        //    1 |-1--------2-------3---|
        //    2   |4--5---------|
        //    3       |---------6----|
        //    4        |7--8-|
        //    5          |--------9-------10-|
        //    6                  |--11----|
        //    7                    |-12---13---14--|
        // TIME|1-2-3-4-5-6-7-8-9-a-b-c-d-e-f-g-h-i-j|
        //  POI     ^   ^     ^         ^     ^     ^
        //          A   B     C         D     E     F

        // A
        txOpened.incrementAndGet(); // <-- TX 1
        buffer.offer( 1 );
        txOpened.incrementAndGet(); // <-- TX 2
        buffer.offer( 4 );
        buffer.maintenance();
        assertEquals( 0, consumer.chunksAccepted() );

        // B
        buffer.offer( 5 );
        txOpened.incrementAndGet(); // <-- TX 3
        txOpened.incrementAndGet(); // <-- TX 4
        buffer.offer( 7 );
        buffer.maintenance();
        assertEquals( 0, consumer.chunksAccepted() );

        // C
        txOpened.incrementAndGet(); // <-- TX 5
        buffer.offer( 2 );
        buffer.offer( 8 );
        // TX 4 closes, but TXs with lower ids are still open
        buffer.maintenance();
        assertEquals( 0, consumer.chunksAccepted() );

        // D
        // TX 2 closes, but TXs with lower ids are still open
        buffer.offer( 6 );
        txOpened.incrementAndGet(); // <-- TX 6
        buffer.offer( 9 );
        buffer.offer( 3 );
        txOpened.incrementAndGet(); // <-- TX 7
        buffer.offer( 11 );
        // TX 3 closes, but TXs with lower ids are still open
        buffer.offer( 12 );
        txClosed.set( 4 ); // since 1-4 have now all closed
        buffer.maintenance();
        consumer.assertHaveOnlySeen( 1, 4, 5, 7 );

        // E
        buffer.offer( 10 );
        // TX 6 closes, but TXs with lower ids are still open
        buffer.offer( 13 );
        txClosed.set( 6 ); // since 1-6 have now all closed
        buffer.maintenance();
        consumer.assertHaveOnlySeen( 1, 2, 4, 5, 7, 8 );

        // F
        buffer.offer( 14 );
        txClosed.set( 7 ); // since 1-7 have now all closed
        buffer.maintenance();
        consumer.assertHaveOnlySeen( 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 );
    }

    @Test
    public void shouldClearCurrentChunk() throws Exception
    {
        // GIVEN
        Consumer<long[]> consumer = mock( Consumer.class );
        DelayedBuffer<Long> buffer = new DelayedBuffer<>( singleton( 0L ), Predicates.<Long>alwaysTrue(),
                10, consumer );
        buffer.offer( 0 );
        buffer.offer( 1 );
        buffer.offer( 2 );

        // WHEN
        buffer.clear();
        buffer.maintenance();

        // THEN
        verifyNoMoreInteractions( consumer );
    }

    @Test
    public void shouldClearPreviousChunks() throws Exception
    {
        // GIVEN
        Consumer<long[]> consumer = mock( Consumer.class );
        final AtomicBoolean safeThreshold = new AtomicBoolean( false );
        DelayedBuffer<Long> buffer = new DelayedBuffer<>( singleton( 0L ), new Predicate<Long>()
        {
            @Override
            public boolean test( Long t )
            {
                return safeThreshold.get();
            }
        }, 10, consumer );
        // three chunks
        buffer.offer( 0 );
        buffer.maintenance();
        buffer.offer( 1 );
        buffer.maintenance();
        buffer.offer( 2 );
        buffer.maintenance();

        // WHEN
        safeThreshold.set( true );
        buffer.clear();
        buffer.maintenance();

        // THEN
        verifyNoMoreInteractions( consumer );
    }

    private static class MaintenanceThread extends Thread
    {
        private final DelayedBuffer buffer;
        private final long nanoInterval;
        private volatile boolean end;

        MaintenanceThread( DelayedBuffer buffer, long nanoInterval )
        {
            this.buffer = buffer;
            this.nanoInterval = nanoInterval;
            start();
        }

        @Override
        public void run()
        {
            while ( !end )
            {
                buffer.maintenance();
                LockSupport.parkNanos( nanoInterval );
            }
        }

        void halt() throws InterruptedException
        {
            end = true;
            while ( isAlive() )
            {
                Thread.sleep( 1 );
            }
        }
    }

    private static class VerifyingConsumer implements Consumer<long[]>
    {
        private final boolean[] seenIds;
        private int chunkCount;

        public VerifyingConsumer( int size )
        {
            seenIds = new boolean[size];
        }

        void assertHaveOnlySeenRange( long low, long high )
        {
            long[] values = new long[(int) (high - low + 1)];
            for ( long id = low, i = 0; id <= high; id++, i++ )
            {
                values[(int) i] = id;
            }
            assertHaveOnlySeen( values );
        }

        @Override
        public void accept( long[] chunk )
        {
            chunkCount++;
            for ( long id : chunk )
            {
                assertFalse( seenIds[safeCastLongToInt( id )] );
                seenIds[safeCastLongToInt( id )] = true;
            }
        }

        void assertHaveOnlySeen( long... values )
        {
            for ( int i = 0, vi = 0; i < seenIds.length && vi < values.length; i++ )
            {
                boolean expectedToBeSeen = values[vi] == i;
                if ( expectedToBeSeen && !seenIds[i] )
                {
                    fail( "Expected to have seen " + i + ", but hasn't" );
                }
                else if ( !expectedToBeSeen && seenIds[i] )
                {
                    fail( "Expected to NOT have seen " + i + ", but have" );
                }

                if ( expectedToBeSeen )
                {
                    vi++;
                }
            }
        }

        int chunksAccepted()
        {
            return chunkCount;
        }
    }
}
