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
package org.neo4j.internal.batchimport.staging;

import static java.lang.System.currentTimeMillis;
import static java.lang.System.nanoTime;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicLong;
import org.neo4j.batchimport.api.Configuration;
import org.neo4j.internal.batchimport.executor.DynamicTaskExecutor;
import org.neo4j.internal.batchimport.executor.TaskExecutor;
import org.neo4j.internal.batchimport.stats.StatsProvider;
import org.neo4j.io.pagecache.context.CursorContext;
import org.neo4j.io.pagecache.context.CursorContextFactory;
import org.neo4j.memory.EmptyMemoryTracker;
import org.neo4j.memory.MemoryTracker;
import org.neo4j.util.concurrent.AsyncApply;

/**
 * {@link Step} that uses {@link TaskExecutor} as a queue and execution mechanism.
 * Supports an arbitrary number of threads to execute batches in parallel.
 * Subclasses implement {@link #process(Object, BatchSender, CursorContext, MemoryTracker)} receiving the batch to process
 * and an {@link BatchSender} for sending the modified batch, or other batches downstream.
 */
public abstract class ProcessorStep<T> extends AbstractStep<T> {
    private static final String IMPORT_STEP_TAG_PREFIX = "importStep:";
    private TaskExecutor<Sender> executor;
    // max processors for this step, zero means unlimited, or rather config.maxNumberOfProcessors()
    private final int maxProcessors;
    private final CursorContextFactory contextFactory;

    // Time stamp for when we processed the last queued batch received from upstream.
    // Useful for tracking how much time we spend waiting for batches from upstream.
    private final AtomicLong lastBatchEndTime = new AtomicLong();
    private String cursorTracerName;

    protected ProcessorStep(
            StageControl control,
            String name,
            Configuration config,
            int maxProcessors,
            CursorContextFactory contextFactory,
            StatsProvider... additionalStatsProviders) {
        super(control, name, config, additionalStatsProviders);
        this.maxProcessors = maxProcessors;
        this.contextFactory = contextFactory;
        updateCursorTracerName();
    }

    @Override
    public void start(int orderingGuarantees) {
        super.start(orderingGuarantees);
        this.executor = new DynamicTaskExecutor<>(
                1, maxProcessors, config.maxQueueSize(), name(), Sender::new, control.scheduler());
    }

    @Override
    public long receive(final long ticket, final T batch) {
        // Don't go too far ahead
        incrementQueue();
        long nanoTime = nanoTime();
        executor.submit(sender -> {
            assertHealthy();
            sender.initialize(ticket);
            try (var cursorContext = contextFactory.create(cursorTracerName)) {
                long startTime = nanoTime();
                process(batch, sender, cursorContext, EmptyMemoryTracker.INSTANCE);
                if (downstream == null) {
                    // No batches were emitted so we couldn't track done batches in that way.
                    // We can see that we're the last step so increment here instead
                    doneBatches.incrementAndGet();
                    control.recycle(batch);
                }
                totalProcessingTime.add(nanoTime() - startTime - sender.sendTime);

                decrementQueue();
                checkNotifyEndDownstream();
            } catch (Throwable e) {
                issuePanic(e);
            }
        });
        return nanoTime() - nanoTime;
    }

    private void decrementQueue() {
        // Even though queuedBatches is built into AbstractStep, in that number of received batches
        // is number of done + queued batches, this is the only implementation changing queuedBatches
        // since it's the only implementation capable of such. That's why this code is here
        // and not in AbstractStep.
        int queueSizeAfterThisJobDone = queuedBatches.decrementAndGet();
        assert queueSizeAfterThisJobDone >= 0 : "Negative queue size " + queueSizeAfterThisJobDone;
        if (queueSizeAfterThisJobDone == 0) {
            lastBatchEndTime.set(currentTimeMillis());
        }
    }

    private void incrementQueue() {
        if (queuedBatches.getAndIncrement() == 0) { // This is the first batch after we last drained the queue.
            long lastBatchEnd = lastBatchEndTime.get();
            if (lastBatchEnd != 0) {
                upstreamIdleTime.add(currentTimeMillis() - lastBatchEnd);
            }
        }
    }

    /**
     * Processes a {@link #receive(long, Object) received} batch. Any batch that should be sent downstream
     * as part of processing the supplied batch should be done so using {@link BatchSender#send(Object)}.
     *
     * The most typical implementation of this method is to process the received batch, either by
     * creating a new batch object containing some derivative of the received batch, or the batch
     * object itself with some modifications and {@link BatchSender#send(Object) emit} that in the end of the method.
     *
     * @param batch batch to process.
     * @param sender {@link BatchSender} for sending zero or more batches downstream.
     * @param cursorContext underlying page cursor context
     * @param memoryTracker
     */
    protected abstract void process(
            T batch, BatchSender sender, CursorContext cursorContext, MemoryTracker memoryTracker) throws Throwable;

    @Override
    public void close() throws Exception {
        super.close();
        executor.close();
    }

    @Override
    public void receivePanic(Throwable cause) {
        super.receivePanic(cause);
        executor.receivePanic(cause);
    }

    @Override
    public int processors(int delta) {
        return executor.processors(delta);
    }

    @Override
    public int maxProcessors() {
        return maxProcessors;
    }

    @SuppressWarnings("unchecked")
    private AsyncApply sendDownstream(long ticket, Object batch, AsyncApply downstreamAsync) {
        if (guarantees(ORDER_SEND_DOWNSTREAM)) {
            AsyncApply async = downstreamWorkSync.applyAsync(new SendDownstream(ticket, batch, downstreamIdleTime));
            if (downstreamAsync != null) {
                try {
                    downstreamAsync.await();
                    async.await();
                    return null;
                } catch (ExecutionException e) {
                    issuePanic(e.getCause());
                }
            } else {
                return async;
            }
        } else {
            downstreamIdleTime.add(downstream.receive(ticket, batch));
            doneBatches.incrementAndGet();
        }
        return null;
    }

    @Override
    protected void done() {
        lastCallForEmittingOutstandingBatches(new Sender());
        if (downstreamWorkSync != null) {
            try {
                downstreamWorkSync.apply(new SendDownstream(-1, null, downstreamIdleTime));
            } catch (ExecutionException e) {
                issuePanic(e.getCause());
            }
        }
        super.done();
    }

    protected void lastCallForEmittingOutstandingBatches(
            BatchSender sender) { // Nothing to emit, subclasses might have though
    }

    private void updateCursorTracerName() {
        this.cursorTracerName = buildCursorTracerName();
    }

    protected String buildCursorTracerName() {
        return IMPORT_STEP_TAG_PREFIX + name();
    }

    private class Sender implements BatchSender {
        private long sendTime;
        private long ticket;
        private AsyncApply downstreamAsync;

        @Override
        public void send(Object batch) {
            long time = nanoTime();
            try {
                downstreamAsync = sendDownstream(ticket, batch, downstreamAsync);
            } finally {
                sendTime += nanoTime() - time;
            }
        }

        public void initialize(long ticket) {
            this.ticket = ticket;
            this.sendTime = 0;
        }
    }
}
