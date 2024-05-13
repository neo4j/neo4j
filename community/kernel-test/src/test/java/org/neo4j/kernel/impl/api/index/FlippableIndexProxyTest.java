/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.api.index;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.awaitLatch;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.neo4j.kernel.api.exceptions.index.IndexProxyAlreadyClosedKernelException;
import org.neo4j.test.OtherThreadExecutor;

class FlippableIndexProxyTest {
    @Test
    void shouldBeAbleToSwitchDelegate() throws Exception {
        // GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxy other = mockIndexProxy();
        FlippableIndexProxy delegate = new FlippableIndexProxy(actual);
        delegate.setFlipTarget(singleProxy(other));

        // WHEN
        delegate.flip(noOp());
        delegate.drop();

        // THEN
        verify(other).drop();
    }

    @Test
    void shouldNotBeAbleToFlipAfterClosed() throws Exception {
        // GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxyFactory indexContextFactory = mock(IndexProxyFactory.class);

        FlippableIndexProxy delegate = new FlippableIndexProxy(actual);

        // WHEN
        delegate.close(NULL_CONTEXT);

        delegate.setFlipTarget(indexContextFactory);

        // THEN
        assertThrows(IndexProxyAlreadyClosedKernelException.class, () -> delegate.flip(noOp()));
    }

    @Test
    void shouldNotBeAbleToFlipAfterDrop() {
        // GIVEN
        IndexProxy actual = mockIndexProxy();
        IndexProxyFactory indexContextFactory = mock(IndexProxyFactory.class);

        FlippableIndexProxy delegate = new FlippableIndexProxy(actual);
        delegate.setFlipTarget(indexContextFactory);

        // WHEN
        delegate.drop();

        // THEN
        assertThrows(IndexProxyAlreadyClosedKernelException.class, () -> delegate.flip(noOp()));
    }

    @Test
    void shouldBlockAccessDuringFlipAndThenDelegateToCorrectContext() throws Exception {
        // GIVEN
        final IndexProxy contextBeforeFlip = mockIndexProxy();
        final IndexProxy contextAfterFlip = mockIndexProxy();
        final FlippableIndexProxy flippable = new FlippableIndexProxy(contextBeforeFlip);
        flippable.setFlipTarget(singleProxy(contextAfterFlip));

        // And given complicated thread race condition tools
        final CountDownLatch triggerFinishFlip = new CountDownLatch(1);
        final CountDownLatch triggerExternalAccess = new CountDownLatch(1);

        try (OtherThreadExecutor flippingThread = new OtherThreadExecutor("Flipping thread");
                OtherThreadExecutor dropIndexThread = new OtherThreadExecutor("Drop index thread")) {
            // WHEN one thread starts flipping to another context
            Future<Void> flipContextFuture = flippingThread.executeDontWait(
                    startFlipAndWaitForLatchBeforeFinishing(flippable, triggerFinishFlip, triggerExternalAccess));

            // And I wait until the flipping thread is in the middle of "the flip"
            assertTrue(triggerExternalAccess.await(10, SECONDS));

            // And another thread comes along and drops the index
            Future<Void> dropIndexFuture = dropIndexThread.executeDontWait(dropTheIndex(flippable));
            dropIndexThread.waitUntilWaiting();

            // And the flipping thread finishes the flip
            triggerFinishFlip.countDown();

            // And both threads get to finish up and return
            dropIndexFuture.get(10, SECONDS);
            flipContextFuture.get(10, SECONDS);

            // THEN the thread wanting to drop the index should not have interacted with the original context
            // eg. it should have waited for the flip to finish
            verifyNoMoreInteractions(contextBeforeFlip);

            // But it should have gotten to drop the new index context, after the flip happened.
            verify(contextAfterFlip).drop();
        }
    }

    @Test
    void shouldAbortStoreScanWaitOnDrop() throws Exception {
        // given the proxy structure
        FakePopulatingIndexProxy delegate = new FakePopulatingIndexProxy();
        FlippableIndexProxy flipper = new FlippableIndexProxy(delegate);
        try (OtherThreadExecutor waiter = new OtherThreadExecutor("Waiter")) {
            // and a thread stuck in the awaitStoreScanCompletion loop
            Future<Object> waiting = waiter.executeDontWait(() -> flipper.awaitStoreScanCompleted(0, MILLISECONDS));
            while (!delegate.awaitCalled) {
                Thread.sleep(10);
            }

            // when
            flipper.drop();

            // then the waiting should quickly be over
            waiting.get(10, SECONDS);
        }
    }

    private static Callable<Void> dropTheIndex(final FlippableIndexProxy flippable) {
        return () -> {
            flippable.drop();
            return null;
        };
    }

    private static Callable<Void> startFlipAndWaitForLatchBeforeFinishing(
            final FlippableIndexProxy flippable,
            final CountDownLatch triggerFinishFlip,
            final CountDownLatch triggerExternalAccess) {
        return () -> {
            flippable.flip(() -> {
                triggerExternalAccess.countDown();
                assertTrue(awaitLatch(triggerFinishFlip));
                return Boolean.TRUE;
            });
            return null;
        };
    }

    private static Callable<Boolean> noOp() {
        return () -> Boolean.TRUE;
    }

    private static IndexProxyFactory singleProxy(final IndexProxy proxy) {
        return () -> proxy;
    }

    private static class FakePopulatingIndexProxy extends IndexProxyAdapter {
        private volatile boolean awaitCalled;

        @Override
        public boolean awaitStoreScanCompleted(long time, TimeUnit unit) {
            awaitCalled = true;
            return true;
        }
    }
}
