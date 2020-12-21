/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.api.impl.schema;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.helpers.Cancelable;
import org.neo4j.internal.helpers.CancellationRequest;

/**
 * Represents a collection point for various {@link Task} instances that need to be waited on and potentially
 * cancelled en mass. Instances of {@link Task} acquired through the {@link #newTask()} method can be
 * notified of cancellation with the semantics of {@link CancellationRequest}.
 */
public class TaskCoordinator implements Cancelable, CancellationRequest
{
    private static final AtomicInteger tasks = new AtomicInteger();
    private volatile boolean cancelled;

    @Override
    public void cancel()
    {
        cancelled = true;
    }

    public void awaitCompletion() throws InterruptedException
    {
        while ( tasks.get() > 0 )
        {
            TimeUnit.MILLISECONDS.sleep( 10 );
        }
    }

    public boolean cancellationRequested()
    {
        return cancelled;
    }

    public Task newTask()
    {
        Task task = new Task();
        if ( cancelled )
        {
            task.close();
            throw new IllegalStateException( "This manager has already been cancelled." );
        }
        return task;
    }

    public class Task implements AutoCloseable, CancellationRequest
    {
        Task()
        {
            tasks.incrementAndGet();
        }

        @Override
        public void close()
        {
            tasks.decrementAndGet();
        }

        @Override
        public boolean cancellationRequested()
        {
            return TaskCoordinator.this.cancellationRequested();
        }
    }
}
