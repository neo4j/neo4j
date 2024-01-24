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
package org.neo4j.kernel.impl.api.chunk;

import static org.neo4j.configuration.GraphDatabaseInternalSettings.multi_version_transaction_chunk_size;

import java.util.function.Supplier;
import org.neo4j.configuration.Config;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.kernel.api.KernelTransaction;
import org.neo4j.kernel.impl.api.LeaseClient;
import org.neo4j.kernel.impl.api.TransactionClockContext;
import org.neo4j.kernel.impl.api.commit.TransactionCommitter;
import org.neo4j.kernel.impl.api.state.TxState;
import org.neo4j.kernel.impl.coreapi.DefaultTransactionExceptionMapper;
import org.neo4j.kernel.impl.transaction.tracing.TransactionEvent;
import org.neo4j.kernel.internal.event.TransactionEventListeners;
import org.neo4j.lock.LockTracer;
import org.neo4j.memory.MemoryTracker;

public final class ChunkSink implements ChunkedTransactionSink {
    private final TransactionEventListeners eventListeners;
    private final TransactionClockContext clocks;
    private final long chunkSize;
    private final TransactionCommitter committer;

    private CursorContext cursorContext;
    private LeaseClient leaseClient;
    private long startTimeMillis;
    private long lastTransactionIdWhenStarted;
    private Supplier<LockTracer> lockTracerSupplier;

    public ChunkSink(
            TransactionCommitter committer,
            TransactionEventListeners eventListeners,
            TransactionClockContext clocks,
            Config config) {
        this.committer = committer;
        this.eventListeners = eventListeners;
        this.clocks = clocks;
        this.chunkSize = config.get(multi_version_transaction_chunk_size);
    }

    @Override
    public void write(TxState txState, TransactionEvent transactionEvent) {
        MemoryTracker memoryTracker = txState.memoryTracker();
        if (memoryTracker.estimatedHeapMemory() > chunkSize) {
            txState.markAsMultiChunk();
            try (var chunkWriteEvent = transactionEvent.beginChunkWriteEvent()) {
                eventListeners.beforeCommit(txState, false);

                committer.commit(
                        chunkWriteEvent,
                        leaseClient,
                        cursorContext,
                        memoryTracker,
                        KernelTransaction.NO_MONITOR,
                        lockTracerSupplier.get(),
                        clocks.systemClock().millis(),
                        startTimeMillis,
                        lastTransactionIdWhenStarted,
                        false);
                txState.reset();
            } catch (Exception e) {
                throw DefaultTransactionExceptionMapper.INSTANCE.mapException(e);
            }
        }
    }

    @Override
    public void initialize(
            LeaseClient leaseClient,
            CursorContext cursorContext,
            Supplier<LockTracer> lockTracerSupplier,
            long startTimeMillis,
            long lastTransactionIdWhenStarted) {
        this.cursorContext = cursorContext;
        this.leaseClient = leaseClient;
        this.lockTracerSupplier = lockTracerSupplier;
        this.startTimeMillis = startTimeMillis;
        this.lastTransactionIdWhenStarted = lastTransactionIdWhenStarted;
    }
}
