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
package org.neo4j.kernel.api.impl.index;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.neo4j.scheduler.CallableExecutor;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobScheduler;

/**
 * Adapts a {@link JobScheduler} group into an {@link ExecutorService} suitable for handing to
 * third-party libraries (e.g. Lucene's parallel HNSW merger) that require the full
 * {@link ExecutorService} interface.
 *
 * <p>Lifecycle is owned by the {@link JobScheduler}; {@link #shutdown()} and
 * {@link #shutdownNow()} are intentional no-ops, per the contract on
 * {@link JobScheduler#executor(Group)}.
 */
public final class JobSchedulerExecutorService extends AbstractExecutorService {
    private final CallableExecutor delegate;

    private JobSchedulerExecutorService(CallableExecutor delegate) {
        this.delegate = delegate;
    }

    /**
     * Creates a non-shutdownable {@link ExecutorService} backed by {@code scheduler}'s {@code group} pool.
     */
    public static ExecutorService nonShutdownable(JobScheduler scheduler, Group group) {
        return new JobSchedulerExecutorService(scheduler.executor(group));
    }

    @Override
    public void execute(Runnable command) {
        delegate.execute(command);
    }

    @Override
    public <T> Future<T> submit(Callable<T> task) {
        // CallableExecutor's submit honors the JobScheduler thread-naming and monitoring.
        // Prefer this over AbstractExecutorService's default (which would call execute(newTaskFor(...))).
        return delegate.submit(task);
    }

    @Override
    public void shutdown() {
        // Owned by JobScheduler — no-op.
    }

    @Override
    public List<Runnable> shutdownNow() {
        return Collections.emptyList();
    }

    @Override
    public boolean isShutdown() {
        return false;
    }

    @Override
    public boolean isTerminated() {
        return false;
    }

    @Override
    public boolean awaitTermination(long timeout, TimeUnit unit) {
        // Caller cannot force termination; we report "not terminated" within the timeout.
        return false;
    }
}
