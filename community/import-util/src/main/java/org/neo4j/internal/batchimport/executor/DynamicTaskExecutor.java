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
package org.neo4j.internal.batchimport.executor;

import static java.lang.Integer.max;
import static java.lang.Integer.min;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.neo4j.function.Suppliers;

/**
 * Implementation of {@link TaskExecutor} with a maximum queue size and where each processor is a dedicated
 * {@link Thread} pulling queued tasks and executing them.
 */
public class DynamicTaskExecutor<LOCAL> implements TaskExecutor<LOCAL> {
    private final BlockingQueue<Task<LOCAL>> queue;
    private final String processorThreadNamePrefix;

    @SuppressWarnings("unchecked")
    private volatile Processor[] processors = (Processor[]) Array.newInstance(Processor.class, 0);

    private volatile boolean shutDown;
    private final AtomicReference<Throwable> panic = new AtomicReference<>();
    private final Supplier<LOCAL> initialLocalState;
    private final int maxProcessorCount;
    private final ProcessorScheduler scheduler;

    public DynamicTaskExecutor(
            int initialProcessorCount,
            int maxProcessorCount,
            int maxQueueSize,
            String processorThreadNamePrefix,
            Supplier<LOCAL> initialLocalState,
            ProcessorScheduler scheduler) {
        this.maxProcessorCount = maxProcessorCount == 0 ? Integer.MAX_VALUE : maxProcessorCount;
        this.scheduler = scheduler;

        assert this.maxProcessorCount >= initialProcessorCount
                : "Unexpected initial processor count " + initialProcessorCount + " for max " + maxProcessorCount;

        this.processorThreadNamePrefix = processorThreadNamePrefix;
        this.initialLocalState = initialLocalState;
        this.queue = new ArrayBlockingQueue<>(maxQueueSize);
        processors(initialProcessorCount);
    }

    @Override
    public int processors(int delta) {
        if (shutDown || delta == 0) {
            return processors.length;
        }

        synchronized (this) {
            if (shutDown) {
                return processors.length;
            }

            int requestedNumber = processors.length + delta;
            if (delta > 0) {
                requestedNumber = min(requestedNumber, maxProcessorCount);
                if (requestedNumber > processors.length) {
                    Processor[] newProcessors = Arrays.copyOf(processors, requestedNumber);
                    for (int i = processors.length; i < requestedNumber; i++) {
                        newProcessors[i] = new Processor();
                        scheduler.schedule(newProcessors[i], processorThreadNamePrefix + "-" + i);
                    }
                    this.processors = newProcessors;
                }
            } else {
                requestedNumber = max(1, requestedNumber);
                if (requestedNumber < processors.length) {
                    Processor[] newProcessors = Arrays.copyOf(processors, requestedNumber);
                    for (int i = newProcessors.length; i < processors.length; i++) {
                        processors[i].processorShutDown = true;
                    }
                    this.processors = newProcessors;
                }
            }
            return processors.length;
        }
    }

    @Override
    public void submit(Task<LOCAL> task) {
        assertHealthy();
        try {
            while (!queue.offer(task, 10, MILLISECONDS)) { // Then just stay here and try
                assertHealthy();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void assertHealthy() {
        Throwable panic = this.panic.get();
        if (panic != null) {
            throw new TaskExecutionPanicException("Executor has been shut down in panic", panic);
        }
    }

    @Override
    public void receivePanic(Throwable cause) {
        panic.compareAndSet(null, cause);
    }

    @Override
    public synchronized void close() {
        if (shutDown) {
            return;
        }

        this.shutDown = true;
        try {
            for (var processor : processors) {
                while (!processor.endSignal.await(1, SECONDS)) {
                    if (panic.get() != null) {
                        // we in panic there is nothing to wait here.
                        return;
                    }
                }
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }

    public static <T> Supplier<T> noLocalState() {
        return Suppliers.singleton(null);
    }

    private class Processor implements Runnable {
        // In addition to the global shutDown flag in the executor each processor has a local flag
        // so that an individual processor can be shut down, for example when reducing number of processors
        private volatile boolean processorShutDown;
        private final CountDownLatch endSignal = new CountDownLatch(1);

        @Override
        public void run() {
            try {
                // Initialized here since it's the thread itself that needs to call it
                final LOCAL threadLocalState = initialLocalState.get();
                while (shouldContinue()) {
                    Task<LOCAL> task;
                    try {
                        task = queue.poll(1, MILLISECONDS);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                        break;
                    }

                    if (task != null) {
                        try {
                            task.run(threadLocalState);
                        } catch (Throwable e) {
                            receivePanic(e);
                            throw new RuntimeException(e);
                        }
                    }
                }
            } finally {
                endSignal.countDown();
            }
        }

        private boolean shouldContinue() {
            if (processorShutDown || panic.get() != null) {
                return false;
            }
            return !shutDown || !queue.isEmpty();
        }
    }
}
