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
package org.neo4j.kernel.impl.transaction.log;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.function.Factory;
import org.neo4j.kernel.impl.util.Counter;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class BatchingPhysicalTransactionAppenderTest
{
    @Test
    public void shouldWaitOnCorrectTicket() throws Exception
    {
        // GIVEN
        long highestValueBeforeWrappingAround = 3;
        LogFile logFile = mock( LogFile.class );
        when( logFile.getWriter() ).thenReturn( new InMemoryLogChannel() );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        LimitedCounterFactory counters = new LimitedCounterFactory( highestValueBeforeWrappingAround, 0 );
        ControlledParkStrategy forceThreadControl = new ControlledParkStrategy();
        BatchingPhysicalTransactionAppender appender = new BatchingPhysicalTransactionAppender( logFile, cache,
                transactionIdStore, BYPASS, counters, forceThreadControl );
        Counter appendCounter = counters.createdCounters.get( 0 );
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );

        // WHEN setting the counter to its highest value before wrapping around
        assertForceAfterAppendAwaitsCorrectForceTicket( t2, appender, forceThreadControl, appendCounter );
    }

    @Test
    public void shouldHandleTicketsWrappingAround() throws Exception
    {
        // GIVEN
        long highestValueBeforeWrappingAround = 3;
        LogFile logFile = mock( LogFile.class );
        when( logFile.getWriter() ).thenReturn( new InMemoryLogChannel() );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        LimitedCounterFactory counters = new LimitedCounterFactory(
                highestValueBeforeWrappingAround, highestValueBeforeWrappingAround );
        ControlledParkStrategy forceThreadControl = new ControlledParkStrategy();
        BatchingPhysicalTransactionAppender appender = new BatchingPhysicalTransactionAppender( logFile, cache,
                transactionIdStore, BYPASS, counters, forceThreadControl );
        Counter appendCounter = counters.createdCounters.get( 0 );
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );

        // and even WHEN wrapping around ticket the force must be awaited correctly
        assertForceAfterAppendAwaitsCorrectForceTicket( t2, appender, forceThreadControl, appendCounter );
    }

    private void assertForceAfterAppendAwaitsCorrectForceTicket( OtherThreadExecutor<Void> t2,
            BatchingPhysicalTransactionAppender appender, ControlledParkStrategy forceThreadControl, Counter appendCounter ) throws Exception
    {
        forceThreadControl.awaitIdle();
        long ticket = appendCounter.incrementAndGet();
        // THEN forcing as part of append (forceAfterAppend) should await that ticket
        Future<Object> forceFuture = t2.executeDontWait( forceAfterAppend( appender, ticket ) );
        forceThreadControl.unpark( Thread.currentThread() );
        forceFuture.get();
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();

    private WorkerCommand<Void, Object> forceAfterAppend( final BatchingPhysicalTransactionAppender appender,
            final long ticket )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                appender.forceAfterAppend( ticket );
                return null;
            }
        };
    }

    private static class LimitedCounterFactory implements Factory<Counter>
    {
        private final List<Counter> createdCounters = new ArrayList<>();
        private final long highestValue;
        private final long initialValue;

        public LimitedCounterFactory( long highestValue, long initialValue )
        {
            this.highestValue = highestValue;
            this.initialValue = initialValue;
        }

        @Override
        public Counter newInstance()
        {
            Counter counter = new Counter()
            {
                private long value = initialValue;

                @Override
                public void set( long value )
                {
                    assert value <= highestValue && value >= (-highestValue-1);
                    this.value = value;
                }

                @Override
                public long incrementAndGet()
                {
                    value++;
                    if ( value > highestValue )
                    {
                        value = -highestValue-1;
                    }
                    return value;
                }

                @Override
                public long get()
                {
                    return value;
                }
            };
            createdCounters.add( counter );
            return counter;
        }
    }
}
