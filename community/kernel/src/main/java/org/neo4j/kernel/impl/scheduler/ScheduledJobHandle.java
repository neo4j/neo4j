/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.kernel.impl.scheduler;

import java.util.concurrent.CancellationException;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.concurrent.BinaryLatch;
import org.neo4j.scheduler.JobScheduler;
import org.neo4j.scheduler.JobScheduler.CancelListener;
import org.neo4j.scheduler.JobScheduler.JobHandle;

/**
 * The JobHandle implementation for jobs scheduled with the {@link TimeBasedTaskScheduler}.
 * <p>
 * As the handle gets scheduled, it transitions through various states:
 * <ul>
 * <li>The handle is initially in the RUNNABLE state, which means that it is ready to be executed but isn't
 * scheduled to do so yet.</li>
 * <li>When it gets scheduled, it transitions into the SUBMITTED state, and remains there until it has finished
 * executing.</li>
 * <li>A handle that is in the SUBMITTED state cannot be submitted again, even if it comes due.</li>
 * <li>A handle that is both due and SUBMITTED is <em>overdue</em>, and its execution will be delayed until it
 * changes out of the SUBMITTED state.</li>
 * <li>If a scheduled handle successfully finishes its execution, it will transition back to the RUNNABLE state.</li>
 * <li>If an exception is thrown during the execution, then the handle transitions to the FAILED state, which is a
 * terminal state.</li>
 * <li>Failed handles will not be scheduled again.</li>
 * </ul>
 */
final class ScheduledJobHandle extends AtomicInteger implements JobHandle
{
    // We extend AtomicInteger to inline our state field.
    // These are the possible state values:
    private static final int RUNNABLE = 0;
    private static final int SUBMITTED = 1;
    private static final int FAILED = 2;

    // Access is synchronised via the PriorityBlockingQueue in TimeBasedTaskScheduler:
    // - Write to this field happens before the handle is added to the queue.
    // - Reads of this field happens after the handle has been read from the queue.
    // - Reads of this field for the purpose of ordering the queue are either thread local,
    //   or happens after the relevant handles have been added to the queue.
    long nextDeadlineNanos;

    private final JobScheduler.Group group;
    private final CopyOnWriteArrayList<CancelListener> cancelListeners;
    private final BinaryLatch handleRelease;
    private final Runnable task;
    private volatile JobHandle latestHandle;
    private volatile Throwable lastException;

    ScheduledJobHandle( TimeBasedTaskScheduler scheduler, JobScheduler.Group group, Runnable task,
                        long nextDeadlineNanos, long reschedulingDelayNanos )
    {
        this.group = group;
        this.nextDeadlineNanos = nextDeadlineNanos;
        handleRelease = new BinaryLatch();
        cancelListeners = new CopyOnWriteArrayList<>();
        this.task = () ->
        {
            try
            {
                task.run();
                // Use compareAndSet to avoid overriding any cancellation state.
                if ( compareAndSet( SUBMITTED, RUNNABLE ) && reschedulingDelayNanos > 0 )
                {
                    // We only reschedule if the rescheduling delay is greater than zero.
                    // A rescheduling delay of zero means this is a delayed task.
                    // If the rescheduling delay is greater than zero, then this is a recurring task.
                    this.nextDeadlineNanos += reschedulingDelayNanos;
                    scheduler.enqueueTask( this );
                }
            }
            catch ( Throwable e )
            {
                lastException = e;
                set( FAILED );
            }
        };
    }

    void submitIfRunnable( ThreadPoolManager pools )
    {
        if ( compareAndSet( RUNNABLE, SUBMITTED ) )
        {
            latestHandle = pools.submit( group, task );
            handleRelease.release();
        }
    }

    @Override
    public void cancel( boolean mayInterruptIfRunning )
    {
        set( FAILED );
        JobHandle handle = latestHandle;
        if ( handle != null )
        {
            handle.cancel( mayInterruptIfRunning );
        }
        for ( CancelListener cancelListener : cancelListeners )
        {
            cancelListener.cancelled( mayInterruptIfRunning );
        }
        // Release the handle to allow waitTermination() to observe the cancellation.
        handleRelease.release();
    }

    @Override
    public void waitTermination() throws ExecutionException, InterruptedException
    {
        handleRelease.await();
        JobHandle handleDelegate = this.latestHandle;
        if ( handleDelegate != null )
        {
            handleDelegate.waitTermination();
        }
        if ( get() == FAILED )
        {
            Throwable exception = this.lastException;
            if ( exception != null )
            {
                throw new ExecutionException( exception );
            }
            else
            {
                throw new CancellationException();
            }
        }
    }

    @Override
    public void registerCancelListener( CancelListener listener )
    {
        cancelListeners.add( listener );
    }
}
