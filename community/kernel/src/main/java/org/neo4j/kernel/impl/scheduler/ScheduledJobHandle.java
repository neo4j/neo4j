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
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.internal.helpers.Exceptions;
import org.neo4j.scheduler.CancelListener;
import org.neo4j.scheduler.Group;
import org.neo4j.scheduler.JobHandle;
import org.neo4j.util.concurrent.BinaryLatch;

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
 * <li>If an exception is thrown during the execution, then the handle transitions to the FAILED state in case task is not recurring,
 * otherwise its rescheduled for next execution.</li>
 * </ul>
 */
final class ScheduledJobHandle<T> implements JobHandle<T>
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

    private final AtomicInteger state;
    private final TimeBasedTaskScheduler scheduler;
    private final Group group;
    private final CopyOnWriteArrayList<CancelListener> cancelListeners;
    private final BinaryLatch handleRelease;
    private final Runnable task;
    private volatile JobHandle latestHandle;
    private volatile Throwable lastException;

    ScheduledJobHandle( TimeBasedTaskScheduler scheduler, Group group, Runnable task,
                        long nextDeadlineNanos, long reschedulingDelayNanos )
    {
        this.state = new AtomicInteger();
        this.scheduler = scheduler;
        this.group = group;
        this.nextDeadlineNanos = nextDeadlineNanos;
        handleRelease = new BinaryLatch();
        cancelListeners = new CopyOnWriteArrayList<>();
        boolean isRecurring = reschedulingDelayNanos > 0;
        this.task = () ->
        {
            try
            {
                task.run();
                lastException = null;
            }
            catch ( Throwable e )
            {
                lastException = e;
                if ( !isRecurring )
                {
                    state.set( FAILED );
                }
            }
            finally
            {
                // Use compareAndSet to avoid overriding any cancellation state.
                if ( state.compareAndSet( SUBMITTED, RUNNABLE ) && isRecurring )
                {
                    // We only reschedule if the rescheduling delay is greater than zero.
                    // A rescheduling delay of zero means this is a delayed task.
                    // If the rescheduling delay is greater than zero, then this is a recurring task.
                    this.nextDeadlineNanos += reschedulingDelayNanos;
                    scheduler.enqueueTask( this );
                }
            }
        };
    }

    void submitIfRunnable( ThreadPoolManager pools )
    {
        if ( state.compareAndSet( RUNNABLE, SUBMITTED ) )
        {
            latestHandle = pools.getThreadPool( group ).submit( task );
            handleRelease.release();
        }
    }

    @Override
    public void cancel()
    {
        state.set( FAILED );
        JobHandle handle = latestHandle;
        if ( handle != null )
        {
            handle.cancel();
        }
        for ( CancelListener cancelListener : cancelListeners )
        {
            cancelListener.cancelled();
        }
        scheduler.cancelTask( this );
        // Release the handle to allow waitTermination() to observe the cancellation.
        handleRelease.release();
    }

    @Override
    public void waitTermination() throws ExecutionException, InterruptedException
    {
        handleRelease.await();
        RuntimeException runtimeException = null;
        try
        {
            JobHandle handleDelegate = this.latestHandle;
            if ( handleDelegate != null )
            {
                handleDelegate.waitTermination();
            }
        }
        catch ( RuntimeException t )
        {
            runtimeException = t;
        }
        if ( state.get() == FAILED )
        {
            Throwable exception = this.lastException;
            if ( exception != null )
            {
                var executionException = new ExecutionException( exception );
                if ( runtimeException != null )
                {
                    executionException.addSuppressed( runtimeException );
                }
                throw executionException;
            }
            else
            {
                throw Exceptions.chain( new CancellationException(), runtimeException );
            }
        }
    }

    @Override
    public void waitTermination( long timeout, TimeUnit unit )
    {
        throw new UnsupportedOperationException( "Not supported for repeating tasks." );
    }

    @Override
    public T get()
    {
        throw new UnsupportedOperationException( "Not supported for repeating tasks." );
    }

    @Override
    public void registerCancelListener( CancelListener listener )
    {
        cancelListeners.add( listener );
    }
}
