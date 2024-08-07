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

import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.kernel.impl.api.CompleteTransaction;
import org.neo4j.kernel.impl.transaction.CommittedCommandBatch;
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
    }

    @Override
    public boolean visit(CommittedCommandBatch commandBatch) throws Exception {
        checkFailure();

        // We need to know the starting point for the "is it my turn yet?" check below that each thread needs to do
        // before acquiring the locks
        prevLockedTxId.compareAndSet(-1, commandBatch.txId() - stride);

        // TODO Also consider the memory usage of all active commandBatch instances and apply back-pressure if
        // surpassing it
        appliers.submit(() -> {
            long txId = commandBatch.txId();
            while (prevLockedTxId.get() != txId - stride) {
                Thread.onSpinWait();
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
        });
        return false;
    }

    private void checkFailure() throws Exception {
        Throwable failure = this.failure.get();
        if (failure != null) {
            Exceptions.throwIfUnchecked(failure);
            throw new Exception("One or more recovering transactions failed to apply", failure);
        }
    }

    private void apply(CommittedCommandBatch transaction) throws Exception {
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
