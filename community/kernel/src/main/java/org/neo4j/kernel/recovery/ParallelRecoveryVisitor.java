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
package org.neo4j.kernel.recovery;

import static java.lang.Integer.max;
import static org.neo4j.util.Preconditions.checkState;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatchRepresentation;
import org.neo4j.lock.LockGroup;
import org.neo4j.lock.LockService;
import org.neo4j.lock.ReentrantLockService;
import org.neo4j.storageengine.api.StorageEngine;
import org.neo4j.storageengine.api.TransactionApplicationMode;

final class ParallelRecoveryVisitor implements RecoveryApplier {
    private final AtomicLong prevLockedTxId = new AtomicLong(-1);
    private final StorageEngine storageEngine;
    private final LockService lockService = new ReentrantLockService();
    private final TransactionApplicationMode mode;
    private final CursorContextFactory contextFactory;
    private final String tracerTag;
    private final ExecutorService appliers;
    private final AtomicReference<Throwable> failure = new AtomicReference<>();
    private final int stride;
    private final Semaphore coordination;

    ParallelRecoveryVisitor(
            StorageEngine storageEngine,
            TransactionApplicationMode mode,
            CursorContextFactory contextFactory,
            String tracerTag) {
        this(
                storageEngine,
                mode,
                contextFactory,
                tracerTag,
                max(1, Runtime.getRuntime().availableProcessors() - 1));
    }

    ParallelRecoveryVisitor(
            StorageEngine storageEngine,
            TransactionApplicationMode mode,
            CursorContextFactory contextFactory,
            String tracerTag,
            int numAppliers) {
        this.storageEngine = storageEngine;
        this.mode = mode;
        this.contextFactory = contextFactory;
        this.tracerTag = tracerTag;
        this.appliers = new ThreadPoolExecutor(
                numAppliers,
                numAppliers,
                1,
                TimeUnit.HOURS,
                new LinkedBlockingQueue<>(numAppliers),
                new ThreadPoolExecutor.CallerRunsPolicy());
        this.stride = mode.isReverseStep() ? -1 : 1;
        this.coordination = new Semaphore(numAppliers);
    }

    @Override
    public boolean visit(CommittedCommandBatchRepresentation commandBatch) throws Exception {
        checkFailure();

        // We need to know the starting point for the "is it my turn yet?" check below that each thread needs to do
        // before acquiring the locks
        prevLockedTxId.compareAndSet(-1, commandBatch.txId() - stride);

        // TODO Also consider the memory usage of all active commandBatch instances and apply back-pressure if
        // surpassing it
        appliers.submit(coordinate(() -> {
            long txId = commandBatch.txId();
            while (prevLockedTxId.get() != txId - stride) {
                for (int i = 0; i < 100_000 && prevLockedTxId.get() != txId - stride; i++) {
                    Thread.onSpinWait();
                }
                checkFailure();
            }
            try (LockGroup locks = new LockGroup()) {
                storageEngine.lockRecoveryCommands(commandBatch.commandBatch(), lockService, locks, mode);
                boolean myTurn = prevLockedTxId.compareAndSet(txId - stride, txId);
                checkState(
                        myTurn,
                        "Something wrong with the algorithm, I thought it was my turn, but apparently it wasn't %d",
                        txId);
                apply(commandBatch);
            } catch (Throwable e) {
                failure.compareAndSet(null, e);
            }
            return null;
        }));
        return false;
    }

    /**
     * The {@link ThreadPoolExecutor} {@link java.util.concurrent.RejectedExecutionHandler} doesn't quite support
     * blocking on submitting, even though the underlying queue is blocking, so this task coordination
     * adds that. Why is it needed? Because we don't want the thread that does the queuing (i.e. reconciler thread)
     * to do any of the actual work. This is because this work may be slow due to:
     * <ul>
     *     <li>contending for locks that are needed to apply the transaction</li>
     *     <li>potentially applying a large or otherwise slow transaction</li>
     * </ul>
     * Every time this thread ends up doing any of those (or both) it will block any further queueing to the
     * other threads, effectively reducing parallelism down to 1 during this time. The more frequently this happens the
     * less parallelism parallel recovery gets as a whole.
     *
     * @param task the actual task to coordinate.
     * @return the task, with added coordination to it.
     * @throws InterruptedException on coordination noticing interruption.
     */
    private Callable<Void> coordinate(Callable<Void> task) throws InterruptedException {
        coordination.acquire();
        return () -> {
            try {
                return task.call();
            } finally {
                coordination.release();
            }
        };
    }

    private void checkFailure() throws Exception {
        Throwable failure = this.failure.get();
        if (failure != null) {
            Exceptions.throwIfUnchecked(failure);
            throw new Exception("One or more recovering transactions failed to apply", failure);
        }
    }

    private void apply(CommittedCommandBatchRepresentation transaction) throws Exception {
        try (CursorContext cursorContext = contextFactory.create(tracerTag);
                var storeCursors = storageEngine.createStorageCursors(cursorContext)) {
            var tx = new CompleteTransaction(transaction, cursorContext, storeCursors);
            storageEngine.apply(tx, mode);
        }
    }

    @Override
    public void close() throws Exception {
        appliers.shutdown();
        try {
            if (!appliers.awaitTermination(1, TimeUnit.HOURS)) {
                throw new IllegalStateException("Recovery couldn't gracefully await remaining appliers");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
        checkFailure();
    }
}
