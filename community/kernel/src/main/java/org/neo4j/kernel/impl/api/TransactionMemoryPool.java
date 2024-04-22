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
package org.neo4j.kernel.impl.api;

import static org.neo4j.configuration.GraphDatabaseSettings.memory_tracking;
import static org.neo4j.configuration.GraphDatabaseSettings.memory_transaction_max_size;

import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import org.neo4j.configuration.Config;
import org.neo4j.configuration.GraphDatabaseInternalSettings;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;
import org.neo4j.memory.DelegatingMemoryPool;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.ExecutionContextMemoryTracker;
import org.neo4j.memory.HighWaterMarkMemoryPool;
import org.neo4j.memory.LocalMemoryTracker;
import org.neo4j.memory.MemoryGroup;
import org.neo4j.memory.MemoryLimitExceededException;
import org.neo4j.memory.MemoryPoolImpl;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.memory.ScopedMemoryPool;

public class TransactionMemoryPool extends DelegatingMemoryPool implements ScopedMemoryPool {

    private final ScopedMemoryPool delegate;
    private final Config config;
    private final BooleanSupplier openCheck;
    private final LogProvider logProvider;
    private final LocalMemoryTracker transactionTracker;
    private final AtomicReference<HighWaterMarkMemoryPool> heapHighWaterMarkPool = new AtomicReference<>();
    private volatile boolean hasExecutionContextMemoryTrackers = false;

    public TransactionMemoryPool(
            ScopedMemoryPool delegate, Config config, BooleanSupplier openCheck, LogProvider logProvider) {
        super(new MemoryPoolImpl(delegate.totalSize(), true, memory_transaction_max_size.name()));
        this.delegate = delegate;
        this.config = config;
        this.openCheck = openCheck;
        this.logProvider = logProvider;
        this.transactionTracker = createMemoryTracker();
    }

    @Override
    public MemoryGroup group() {
        return delegate.group();
    }

    @Override
    public void close() {
        reset();
    }

    public LocalMemoryTracker getTransactionTracker() {
        return transactionTracker;
    }

    @Override
    public MemoryTracker getPoolMemoryTracker() {
        throw new UnsupportedOperationException("Use getExecutionContextPoolMemoryTracker instead");
    }

    public MemoryTracker getExecutionContextPoolMemoryTracker(long grabSize, long maxGrabSize) {
        if (config.get(memory_tracking)) {
            hasExecutionContextMemoryTrackers = true;
            return createExecutionContextMemoryTracker(grabSize, maxGrabSize);
        } else {
            return EmptyMemoryTracker.INSTANCE;
        }
    }

    @Override
    public void reserveHeap(long bytes) {
        delegate.reserveHeap(bytes);
        try {
            super.reserveHeap(bytes);
        } catch (MemoryLimitExceededException e) {
            delegate.releaseHeap(bytes);
            throw e;
        }
    }

    @Override
    public void releaseHeap(long bytes) {
        super.releaseHeap(bytes);
        delegate.releaseHeap(bytes);
    }

    @Override
    public void reserveNative(long bytes) {
        delegate.reserveNative(bytes);
        try {
            super.reserveNative(bytes);
        } catch (MemoryLimitExceededException e) {
            delegate.releaseNative(bytes);
            throw e;
        }
    }

    @Override
    public void releaseNative(long bytes) {
        super.releaseNative(bytes);
        delegate.releaseNative(bytes);
    }

    private LocalMemoryTracker createMemoryTracker() {
        return new LocalMemoryTracker(
                this,
                LocalMemoryTracker.NO_LIMIT,
                config.get(GraphDatabaseInternalSettings.initial_transaction_heap_grab_size),
                memory_transaction_max_size.name(),
                openCheck,
                memoryLeakLogger(logProvider.getLog(getClass())));
    }

    private HighWaterMarkMemoryPool highWaterMarkMemoryPool() {
        var pool = heapHighWaterMarkPool.get();
        if (pool == null) {
            pool = new HighWaterMarkMemoryPool(this);
            heapHighWaterMarkPool.compareAndSet(null, pool);
        }
        return pool;
    }

    private ExecutionContextMemoryTracker createExecutionContextMemoryTracker(long grabSize, long maxGrabSize) {
        return new ExecutionContextMemoryTracker(
                highWaterMarkMemoryPool(),
                LocalMemoryTracker.NO_LIMIT,
                grabSize,
                maxGrabSize,
                memory_transaction_max_size.name(),
                openCheck);
    }

    private static LocalMemoryTracker.Monitor memoryLeakLogger(Log log) {
        return leakedNativeMemoryBytes -> log.warn(
                "Potential direct memory leak. Expecting all allocated direct memory to be released, but still has "
                        + leakedNativeMemoryBytes);
    }

    public void setLimit(long transactionHeapBytesLimit) {
        setSize(transactionHeapBytesLimit);
    }

    public void reset() {
        transactionTracker.reset();
        if (hasExecutionContextMemoryTrackers) {
            releaseHeap(usedHeap());
            hasExecutionContextMemoryTrackers = false;
        }
        var highWaterMarkMemoryPool = heapHighWaterMarkPool.get();
        if (highWaterMarkMemoryPool != null) {
            highWaterMarkMemoryPool.reset();
        }

        assert usedNative() == 0
                : "Potential direct memory leak. Expecting all allocated direct memory to be released, but still has "
                        + usedNative();
        assert usedHeap() == 0
                : "Heap memory leak. Expecting all allocated memory to be released, but still has " + usedHeap();
    }
}
