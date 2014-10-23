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

import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import static org.neo4j.kernel.impl.util.Counter.ATOMIC_LONG;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import static org.neo4j.kernel.impl.util.IdOrderingQueue.BYPASS;

public class BatchingPhysicalTransactionAppenderTest
{
    @Test
    public void shouldWaitOnForce() throws Exception
    {
        // GIVEN
        LogFile logFile = mock( LogFile.class );
        when( logFile.getWriter() ).thenReturn( new InMemoryLogChannel() );
        TransactionMetadataCache cache = new TransactionMetadataCache( 10, 10 );
        TransactionIdStore transactionIdStore = mock( TransactionIdStore.class );
        ControlledIdler forceThreadControl = new ControlledIdler();
        BatchingPhysicalTransactionAppender appender = new BatchingPhysicalTransactionAppender( logFile, cache,
                transactionIdStore, BYPASS, ATOMIC_LONG, forceThreadControl );
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );

        // WHEN setting the counter to its highest value before wrapping around
        assertForceAfterAppendAwaitsCorrectForceTicket( t2, appender, forceThreadControl );
    }

    private void assertForceAfterAppendAwaitsCorrectForceTicket( OtherThreadExecutor<Void> t2,
            BatchingPhysicalTransactionAppender appender, ControlledIdler forceThreadControl ) throws Exception
    {
        forceThreadControl.awaitIdle();

        // THEN forcing as part of append (forceAfterAppend) should await that ticket
        Future<Object> forceFuture = t2.executeDontWait( forceAfterAppend( appender ) );
        Thread.sleep( 1 );
        assertFalse( forceFuture.isDone() );

        forceThreadControl.letLoose();
        forceFuture.get();
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();

    private WorkerCommand<Void, Object> forceAfterAppend( final BatchingPhysicalTransactionAppender appender )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                appender.forceAfterAppend( );
                return null;
            }
        };
    }

    public class ControlledIdler implements WaitStrategy
    {
        private volatile boolean idle;

        @Override
        public void wait( Thread thread )
        {
            idle = true;
            await( false );
        }

        public void letLoose()
        {
            idle = false;
        }

        public void awaitIdle()
        {
            await( true );
        }

        private void await( boolean idle )
        {
            while ( this.idle != idle )
            {
                try
                {
                    Thread.sleep( 1 );
                }
                catch ( InterruptedException e )
                {
                    throw new RuntimeException( e );
                }
            }
        }
    }
}
