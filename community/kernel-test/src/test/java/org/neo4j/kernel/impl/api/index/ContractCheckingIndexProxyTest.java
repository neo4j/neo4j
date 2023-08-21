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

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoMoreInteractions;
import static org.neo4j.io.pagecache.context.CursorContext.NULL_CONTEXT;
import static org.neo4j.kernel.impl.api.index.SchemaIndexTestHelper.mockIndexProxy;

import java.io.IOException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.tracing.FileFlushEvent;
import org.neo4j.kernel.api.exceptions.index.IndexEntryConflictException;
import org.neo4j.kernel.api.index.IndexUpdater;
import org.neo4j.test.DoubleLatch;
import org.neo4j.test.ThreadTestUtils;

class ContractCheckingIndexProxyTest {
    private static final long TEST_TIMEOUT = 20_000;

    @Test
    void shouldNotCreateIndexTwice() {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.start();
        assertThrows(IllegalStateException.class, outer::start);
    }

    @Test
    void shouldNotCloseIndexTwice() throws IOException {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.close(NULL_CONTEXT);
        assertThrows(IllegalStateException.class, () -> outer.close(NULL_CONTEXT));
    }

    @Test
    void shouldNotDropIndexTwice() {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.drop();
        assertThrows(IllegalStateException.class, outer::drop);
    }

    @Test
    void shouldNotDropAfterClose() throws IOException {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.close(NULL_CONTEXT);
        assertThrows(IllegalStateException.class, outer::drop);
    }

    @Test
    void shouldDropAfterCreate() {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.start();

        // PASS
        outer.drop();
    }

    @Test
    void shouldCloseAfterCreate() throws IOException {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.start();

        // PASS
        outer.close(NULL_CONTEXT);
    }

    @Test
    void shouldNotUpdateBeforeCreate() {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        assertThrows(IllegalStateException.class, () -> {
            try (IndexUpdater updater = outer.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                updater.process(null);
            }
        });
    }

    @Test
    void shouldNotUpdateAfterClose() throws Exception {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.start();
        outer.close(NULL_CONTEXT);

        assertThrows(IllegalStateException.class, () -> {
            try (IndexUpdater updater = outer.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                updater.process(null);
            }
        });
    }

    @Test
    void shouldNotForceBeforeCreate() throws IOException {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.force(FileFlushEvent.NULL, NULL_CONTEXT);
        verifyNoMoreInteractions(inner);
    }

    @Test
    void shouldNotForceAfterClose() throws IOException {
        // GIVEN
        IndexProxy inner = mockIndexProxy();
        IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        outer.start();
        outer.close(NULL_CONTEXT);

        outer.force(FileFlushEvent.NULL, NULL_CONTEXT);
        verify(inner).start();
        verify(inner).close(any());
        verifyNoMoreInteractions(inner);
    }

    @Test
    void shouldNotCloseWhileCreating() {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter() {
            @Override
            public void start() {
                latch.startAndWaitForAllToStartAndFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        runInSeparateThread(outer::start);

        try {
            latch.waitForAllToStart();
            assertThrows(IllegalStateException.class, () -> outer.close(NULL_CONTEXT));
        } finally {
            latch.finish();
        }
    }

    @Test
    void shouldNotDropWhileCreating() {
        // GIVEN
        final DoubleLatch latch = new DoubleLatch();
        final IndexProxy inner = new IndexProxyAdapter() {
            @Override
            public void start() {
                latch.startAndWaitForAllToStartAndFinish();
            }
        };
        final IndexProxy outer = newContractCheckingIndexProxy(inner);

        // WHEN
        runInSeparateThread(outer::start);

        try {
            latch.waitForAllToStart();
            assertThrows(IllegalStateException.class, outer::drop);
        } finally {
            latch.finish();
        }
    }

    @Test
    void closeWaitForUpdateToFinish() throws InterruptedException {
        // GIVEN
        CountDownLatch latch = new CountDownLatch(1);
        final IndexProxy inner = new IndexProxyAdapter() {};
        final IndexProxy outer = newContractCheckingIndexProxy(inner);
        Thread actionThread = createActionThread(() -> outer.close(NULL_CONTEXT));
        outer.start();

        // WHEN
        Thread updaterThread = runInSeparateThread(() -> {
            try (IndexUpdater updater = outer.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                updater.process(null);
                try {
                    actionThread.start();
                    latch.await();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            } catch (IndexEntryConflictException e) {
                throw new RuntimeException(e);
            }
        });

        ThreadTestUtils.awaitThreadState(actionThread, TEST_TIMEOUT, Thread.State.TIMED_WAITING);
        latch.countDown();
        updaterThread.join();
        actionThread.join();
    }

    @Test
    void closeWaitForForceToComplete() throws Exception {
        // GIVEN
        CountDownLatch latch = new CountDownLatch(1);
        AtomicReference<Thread> actionThreadReference = new AtomicReference<>();
        final IndexProxy inner = new IndexProxyAdapter() {
            @Override
            public void force(FileFlushEvent flushEvent, CursorContext cursorContext) {
                try {
                    actionThreadReference.get().start();
                    latch.await();
                } catch (Exception e) {
                    throw new RuntimeException(e);
                }
            }
        };
        IndexProxy outer = newContractCheckingIndexProxy(inner);
        Thread actionThread = createActionThread(() -> outer.close(NULL_CONTEXT));
        actionThreadReference.set(actionThread);

        outer.start();
        Thread thread = runInSeparateThread(() -> outer.force(FileFlushEvent.NULL, NULL_CONTEXT));

        ThreadTestUtils.awaitThreadState(actionThread, TEST_TIMEOUT, Thread.State.TIMED_WAITING);
        latch.countDown();

        thread.join();
        actionThread.join();
    }

    @Test
    void exceptionFromNewUpdaterDoesNotAddOpenCalls() {
        var outer = newContractCheckingIndexProxy(new IndexProxyAdapter() {
            @Override
            public IndexUpdater newUpdater(IndexUpdateMode mode, CursorContext cursorContext, boolean parallel) {
                throw new IllegalStateException("Can't create updater");
            }
        });
        outer.start();

        assertThatThrownBy(() -> {
                    try (IndexUpdater updater = outer.newUpdater(IndexUpdateMode.ONLINE, NULL_CONTEXT, false)) {
                        // nothing
                    }
                })
                .isInstanceOf(IllegalStateException.class);

        assertThat(outer.getOpenCalls())
                .as("Failure to create updater should result in zero open calls")
                .isZero();
    }

    private interface ThrowingRunnable {
        void run() throws IOException;
    }

    private static Thread runInSeparateThread(final ThrowingRunnable action) {
        Thread thread = createActionThread(action);
        thread.start();
        return thread;
    }

    private static Thread createActionThread(ThrowingRunnable action) {
        return new Thread(() -> {
            try {
                action.run();
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private static ContractCheckingIndexProxy newContractCheckingIndexProxy(IndexProxy inner) {
        return new ContractCheckingIndexProxy(inner);
    }
}
