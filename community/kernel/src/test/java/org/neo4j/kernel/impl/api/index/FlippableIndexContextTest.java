/**
 * Copyright (c) 2002-2013 "Neo Technology,"
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
package org.neo4j.kernel.impl.api.index;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.mockito.Mockito.*;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.Test;
import org.neo4j.test.OtherThreadExecutor;

public class FlippableIndexContextTest
{
    @Test
    public void shouldBeAbleToSwitchDelegate() throws Exception
    {
        // GIVEN
        IndexContext actual = mock( IndexContext.class );
        IndexContext other = mock( IndexContext.class );
        FlippableIndexContext delegate = new FlippableIndexContext(actual);
        delegate.setFlipTarget( IndexingService.singleContext( other ) );

        // WHEN
        delegate.flip();
        delegate.drop();

        // THEN
        verify( other ).drop();
    }


    @Test
    public void shouldBlockAccessDuringFlipAndThenDelegateToCorrectContext() throws Exception
    {
        // GIVEN
        final IndexContext contextBeforeFlip = mock( IndexContext.class );
        final IndexContext contextAfterFlip = mock( IndexContext.class );
        final FlippableIndexContext flippable = new FlippableIndexContext( contextBeforeFlip );
        flippable.setFlipTarget( IndexingService.singleContext( contextAfterFlip ) );

        // And given complicated thread race condition tools
        final CountDownLatch triggerFinishFlip = new CountDownLatch( 1 );
        final CountDownLatch triggerExternalAccess = new CountDownLatch( 1 );

        OtherThreadExecutor<Void> flippingThread = new OtherThreadExecutor<Void>( "Flipping thread", null );
        OtherThreadExecutor<Void> dropIndexThread = new OtherThreadExecutor<Void>( "Drop index thread", null );


        // WHEN one thread starts flipping to another context
        Future<Void> flipContextFuture = flippingThread.executeDontWait( startFlipAndWaitForLatchBeforeFinishing(
                flippable,
                triggerFinishFlip, triggerExternalAccess ) );

        // And I wait until the flipping thread is in the middle of "the flip"
        triggerExternalAccess.await( 10, SECONDS );

        // And another thread comes along and drops the index
        Future<Void> dropIndexFuture = dropIndexThread.executeDontWait( dropTheIndex( flippable ) );
        dropIndexThread.waitUntilWaiting();

        // And the flipping thread finishes the flip
        triggerFinishFlip.countDown();

        // And both threads get to finish up and return
        dropIndexFuture.get( 10, SECONDS );
        flipContextFuture.get( 10, SECONDS );


        // THEN the thread wanting to drop the index should not have interacted with the original context
        // eg. it should have waited for the flip to finish
        verifyNoMoreInteractions( contextBeforeFlip );

        // But it should have gotten to drop the new index context, after the flip happened.
        verify( contextAfterFlip ).drop();
    }

    private OtherThreadExecutor.WorkerCommand<Void, Void> dropTheIndex( final FlippableIndexContext flippable )
    {
        return new OtherThreadExecutor.WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state )
            {
                flippable.drop();
                return null;
            }
        };
    }

    private OtherThreadExecutor.WorkerCommand<Void, Void> startFlipAndWaitForLatchBeforeFinishing(
            final FlippableIndexContext flippable, final CountDownLatch triggerFinishFlip,
            final CountDownLatch triggerExternalAccess )
    {
        return new OtherThreadExecutor.WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state )
            {
                flippable.flip( new Runnable()
                {
                    @Override
                    public void run()
                    {
                        triggerExternalAccess.countDown();
                        try
                        {
                            triggerFinishFlip.await( 10, SECONDS );
                        }
                        catch ( InterruptedException e )
                        {
                            throw new RuntimeException( e );
                        }
                    }
                } );
                return null;
            }
        };
    }

}
