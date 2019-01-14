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
package org.neo4j.kernel.impl.api.index;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.io.IOException;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;

import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.test.OtherThreadExecutor;
import org.neo4j.test.rule.CleanupRule;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.awaitLatch;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

public class FlippableIndexProxyTest
{
    @Rule
    public final CleanupRule cleanup = new CleanupRule();
    @Rule
    public ExpectedException expectedException = ExpectedException.none();

    @Test
    public void shouldBeAbleToSwitchDelegate() throws Exception
    {
        // GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxy other = mockIndexProxy();
        FlippableIndexProxy delegate = new FlippableIndexProxy( actual );
        delegate.setFlipTarget( singleProxy( other ) );

        // WHEN
        delegate.flip( noOp(), null );
        delegate.drop();

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
        delegate.close();

        delegate.setFlipTarget( indexContextFactory );

        //THEN
        expectedException.expect( IndexProxyAlreadyClosedKernelException.class );

        delegate.flip( noOp(), null );
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
        delegate.drop();

        //THEN
        expectedException.expect( IndexProxyAlreadyClosedKernelException.class );
        delegate.flip( noOp(), singleFailedDelegate( failed ) );
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

        OtherThreadExecutor<Void> flippingThread = cleanup.add( new OtherThreadExecutor<>( "Flipping thread", null ) );
        OtherThreadExecutor<Void> dropIndexThread = cleanup.add( new OtherThreadExecutor<>( "Drop index thread", null ) );

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

    @Test
    public void shouldAbortStoreScanWaitOnDrop() throws Exception
    {
        // given the proxy structure
        FakePopulatingIndexProxy delegate = new FakePopulatingIndexProxy();
        FlippableIndexProxy flipper = new FlippableIndexProxy( delegate );
        OtherThreadExecutor<Void> waiter = cleanup.add( new OtherThreadExecutor<>( "Waiter", null ) );

        // and a thread stuck in the awaitStoreScanCompletion loop
        Future<Object> waiting = waiter.executeDontWait( state -> flipper.awaitStoreScanCompleted() );
        while ( !delegate.awaitCalled )
        {
            Thread.sleep( 10 );
        }

        // when
        flipper.drop();

        // then the waiting should quickly be over
        waiting.get( 10, SECONDS );
    }

    private OtherThreadExecutor.WorkerCommand<Void, Void> dropTheIndex( final FlippableIndexProxy flippable )
            throws IOException
    {
        return state ->
        {
            flippable.drop();
            return null;
        };
    }

    private OtherThreadExecutor.WorkerCommand<Void, Void> startFlipAndWaitForLatchBeforeFinishing(
            final FlippableIndexProxy flippable, final CountDownLatch triggerFinishFlip,
            final CountDownLatch triggerExternalAccess )
    {
        return state ->
        {
            flippable.flip( () ->
            {
                triggerExternalAccess.countDown();
                assertTrue( awaitLatch( triggerFinishFlip ) );
                return Boolean.TRUE;
            }, null );
            return null;
        };
    }

    private Callable<Boolean> noOp()
    {
        return () -> Boolean.TRUE;
    }

    public static IndexProxyFactory singleProxy( final IndexProxy proxy )
    {
        return () -> proxy;
    }

    private FailedIndexProxyFactory singleFailedDelegate( final IndexProxy failed )
    {
        return failure -> failed;
    }

    private static class FakePopulatingIndexProxy extends IndexProxyAdapter
    {
        private volatile boolean awaitCalled;

        @Override
        public boolean awaitStoreScanCompleted()
        {
            awaitCalled = true;
            return true;
        }
    }
}
