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
package org.neo4j.bolt.protocol.common.connector.executor;

import java.time.Duration;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import org.neo4j.util.VisibleForTesting;

/**
 * Creates a thread pool based executor service.
 */
public class ThreadPoolExecutorServiceFactory implements ExecutorServiceFactory {

    @VisibleForTesting
    static final int UNBOUNDED_QUEUE = -1;

    @VisibleForTesting
    static final int SYNCHRONOUS_QUEUE = 0; // TODO: probably needed for testing

    private final int corePoolSize;
    private final int maxPoolSize;
    private final boolean prestartCoreThreads;
    private final Duration keepAlive;

    private final int queueSize;
    private final ThreadFactory threadFactory;

    public ThreadPoolExecutorServiceFactory(
            int corePoolSize,
            int maxPoolSize,
            boolean prestartCoreThreads,
            Duration keepAlive,
            int queueSize,
            ThreadFactory threadFactory) {
        this.corePoolSize = corePoolSize;
        this.maxPoolSize = maxPoolSize;
        this.prestartCoreThreads = prestartCoreThreads;
        this.keepAlive = keepAlive;
        this.queueSize = queueSize;
        this.threadFactory = threadFactory;
    }

    @Override
    public ExecutorService create() {
        var executor = new ThreadPoolExecutor(
                this.corePoolSize,
                this.maxPoolSize,
                this.keepAlive.toMillis(),
                TimeUnit.MILLISECONDS,
                createTaskQueue(this.queueSize),
                this.threadFactory,
                new ThreadPoolExecutor.AbortPolicy());

        if (this.prestartCoreThreads) {
            executor.prestartAllCoreThreads();
        }

        return executor;
    }

    private static BlockingQueue<Runnable> createTaskQueue(int queueSize) {
        if (queueSize < UNBOUNDED_QUEUE) {
            throw new IllegalArgumentException(
                    String.format("Unsupported queue size %d for thread pool creation.", queueSize));
        }

        return switch (queueSize) {
            case UNBOUNDED_QUEUE -> new LinkedBlockingQueue<>();
            case SYNCHRONOUS_QUEUE -> new SynchronousQueue<>();
            default -> new ArrayBlockingQueue<>(queueSize);
        };
    }
}
