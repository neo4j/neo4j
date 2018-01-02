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
package org.neo4j.kernel.impl.api.index;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.junit.Rule;
import org.junit.Test;

import org.neo4j.kernel.api.exceptions.index.FlipFailedKernelException;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.test.CleanupRule;
import org.neo4j.test.OtherThreadExecutor;

import static java.util.concurrent.TimeUnit.SECONDS;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;

import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.awaitFuture;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.awaitLatch;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

public class FlippableIndexProxyTest
{
    @Test
    public void shouldBeAbleToSwitchDelegate() throws Exception
    {
        // GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxy other = mockIndexProxy();
        FlippableIndexProxy delegate = new FlippableIndexProxy(actual);
        delegate.setFlipTarget( singleProxy( other ) );

        // WHEN
        delegate.flip( noOp(), null );
        delegate.drop().get();

        // THEN
        verify( other ).drop();
    }

    @Test
    public void shouldNotBeAbleToFlipAfterClosed() throws Exception
    {
        //GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxyFactory indexContextFactory = mock( IndexProxyFactory.class );

        FlippableIndexProxy delegate = new FlippableIndexProxy( actual );

        //WHEN
        delegate.close().get();

        delegate.setFlipTarget( indexContextFactory );

        //THEN
        try
        {
            delegate.flip( noOp(), null );
            fail("Expected IndexProxyAlreadyClosedKernelException to be thrown");
        }
        catch ( IndexProxyAlreadyClosedKernelException e )
        {
            // expected
        }
    }

    @Test
    public void shouldNotBeAbleToFlipAfterDrop() throws Exception
    {
        //GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxy failed = mockIndexProxy();
        IndexProxyFactory indexContextFactory = mock( IndexProxyFactory.class );

        FlippableIndexProxy delegate = new FlippableIndexProxy( actual );
        delegate.setFlipTarget( indexContextFactory );

        //WHEN
        delegate.drop().get();

        //THEN
        try
        {
            delegate.flip( noOp(), singleFailedDelegate( failed ) );
            fail("Expected IndexProxyAlreadyClosedKernelException to be thrown");
        }
        catch ( IndexProxyAlreadyClosedKernelException e )
        {
            // expected
        }
    }

    @Test
    public void shouldBlockAccessDuringFlipAndThenDelegateToCorrectContext() throws Exception
    {
        // GIVEN
        final IndexProxy contextBeforeFlip = mockIndexProxy();
        final IndexProxy contextAfterFlip = mockIndexProxy();
        final FlippableIndexProxy flippable = new FlippableIndexProxy( contextBeforeFlip );
        flippable.setFlipTarget( singleProxy( contextAfterFlip ) );

        // And given complicated thread race condition tools
        final CountDownLatch triggerFinishFlip = new CountDownLatch( 1 );
        final CountDownLatch triggerExternalAccess = new CountDownLatch( 1 );

        OtherThreadExecutor<Void> flippingThread = cleanup.add( new OtherThreadExecutor<Void>( "Flipping thread", null ) );
        OtherThreadExecutor<Void> dropIndexThread = cleanup.add( new OtherThreadExecutor<Void>( "Drop index thread", null ) );


        // WHEN one thread starts flipping to another context
        Future<Void> flipContextFuture = flippingThread.executeDontWait( startFlipAndWaitForLatchBeforeFinishing(
                flippable,
                triggerFinishFlip, triggerExternalAccess ) );

        // And I wait until the flipping thread is in the middle of "the flip"
        assertTrue( triggerExternalAccess.await( 10, SECONDS ) );

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

    public final @Rule CleanupRule cleanup = new CleanupRule();

    private OtherThreadExecutor.WorkerCommand<Void, Void> dropTheIndex( final FlippableIndexProxy flippable )
    {
        return new OtherThreadExecutor.WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state ) throws IOException
            {
                awaitFuture( flippable.drop() );
                return null;
            }
        };
    }

    private OtherThreadExecutor.WorkerCommand<Void, Void> startFlipAndWaitForLatchBeforeFinishing(
            final FlippableIndexProxy flippable, final CountDownLatch triggerFinishFlip,
            final CountDownLatch triggerExternalAccess )
    {
        return new OtherThreadExecutor.WorkerCommand<Void, Void>()
        {
            @Override
            public Void doWork( Void state ) throws FlipFailedKernelException
            {
                flippable.flip( new Callable<Void>()
                {
                    @Override
                    public Void call()
                    {
                        triggerExternalAccess.countDown();
                        assertTrue( awaitLatch( triggerFinishFlip ) );
                        return null;
                    }
                }, null );
                return null;
            }
        };
    }

    private Callable<Void> noOp()
    {
        return new Callable<Void>()
        {
            @Override
            public Void call() throws Exception
            {
                return null;
            }
        };
    }

    public static IndexProxyFactory singleProxy( final IndexProxy proxy )
    {
        return new IndexProxyFactory()
        {
            @Override
            public IndexProxy create()
            {
                return proxy;
            }
        };
    }

    private FailedIndexProxyFactory singleFailedDelegate( final IndexProxy failed )
    {
        return new FailedIndexProxyFactory()
        {
            @Override
            public IndexProxy create( Throwable failure )
            {
                return failed;
            }
        };
    }
}
