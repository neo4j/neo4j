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
package org.neo4j.kernel.impl.transaction;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.impl.util.IdOrderingQueue;
import org.neo4j.kernel.impl.util.SynchronizedArrayIdOrderingQueue;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.OtherThreadExecutor.WorkerCommand;

public class SynchronizedArrayIdOrderingQueueTest
{
    @Test
    public void shouldOfferQueueABunchOfIds() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = new SynchronizedArrayIdOrderingQueue( 5 );

        // WHEN
        for ( int i = 0; i < 7; i++ )
        {
            queue.offer( i );
        }

        // THEN
        for ( int i = 0; i < 7; i++ )
        {
            assertFalse( queue.isEmpty() );
            queue.waitFor( i );
            queue.removeChecked( i );
        }
        assertTrue( queue.isEmpty() );
    }

    @Test
    public void shouldOfferAwaitAndRemoveRoundAndRound() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = new SynchronizedArrayIdOrderingQueue( 5 );
        long offeredId = 0, awaitedId = 0;
        queue.offer( offeredId++ );
        queue.offer( offeredId++ );

        // WHEN
        for ( int i = 0; i < 20; i++ )
        {
            queue.waitFor( awaitedId );
            queue.removeChecked( awaitedId++ );
            queue.offer( offeredId++ );
            assertFalse( queue.isEmpty() );
        }

        // THEN
        queue.removeChecked( awaitedId++ );
        queue.removeChecked( awaitedId++ );
        assertTrue( queue.isEmpty() );
    }

    @Test
    public void shouldHaveOneThreadWaitForARemoval() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = new SynchronizedArrayIdOrderingQueue( 5 );
        queue.offer( 3 );
        queue.offer( 5 );

        // WHEN another thread comes in and awaits 5
        OtherThreadExecutor<Void> t2 = cleanup.add( new OtherThreadExecutor<Void>( "T2", null ) );
        Future<Object> await5 = t2.executeDontWait( awaitHead( queue, 5 ) );
        t2.waitUntilWaiting();
        // ... and head (3) gets removed
        queue.removeChecked( 3 );

        // THEN the other thread should be OK to continue
        await5.get();
    }

    @Test
    public void shouldExtendArrayWhenIdsAreWrappingAround() throws Exception
    {
        // GIVEN
        IdOrderingQueue queue = new SynchronizedArrayIdOrderingQueue( 5 );
        for ( int i = 0; i < 3; i++ )
        {
            queue.offer( i );
            queue.removeChecked( i );
        }
        // Now we're at [0,1,2,0,0]
        //                     ^-- headIndex and offerIndex
        for ( int i = 3; i < 8; i++ )
        {
            queue.offer( i );
        }
        // Now we're at [5,6,2,3,4]
        //                     ^-- headIndex and offerIndex%length

        // WHEN offering one more, so that the queue is forced to resize
        queue.offer( 8 );

        // THEN it should have been offered as well as all the previous ids should be intact
        for ( int i = 3; i <= 8; i++ )
        {
            assertFalse( queue.isEmpty() );
            queue.removeChecked( i );
        }
        assertTrue( queue.isEmpty() );
    }

    private WorkerCommand<Void, Object> awaitHead( final IdOrderingQueue queue, final long id )
    {
        return new WorkerCommand<Void, Object>()
        {
            @Override
            public Object doWork( Void state ) throws Exception
            {
                queue.waitFor( id );
                return null;
            }
        };
    }

    public final @Rule CleanupRule cleanup = new CleanupRule();
}
