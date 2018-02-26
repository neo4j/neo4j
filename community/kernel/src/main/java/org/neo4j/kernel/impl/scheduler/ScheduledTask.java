/*
 * Copyright (c) 2002-2018 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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

final class ScheduledTask implements JobHandle
{
    static final int STATE_RUNNABLE = 0;
    static final int STATE_SUBMITTED = 1;
    static final int STATE_FAILED = 2;

    // Accessed and modified by the TimeBasedTaskScheduler:
    volatile ScheduledTask next;
    long nextDeadlineNanos;

    private final JobScheduler.Group group;
    private final long reschedulingDelayNanos;
    private final AtomicInteger state;
    private final CopyOnWriteArrayList<CancelListener> cancelListeners;
    private final BinaryLatch handleRelease;
    private final Runnable task;
    private volatile JobHandle latestHandle;
    private volatile Throwable lastException;

    ScheduledTask( JobScheduler.Group group, Runnable task, long nextDeadlineNanos, long reschedulingDelayNanos )
    {
        this.group = group;
        this.nextDeadlineNanos = nextDeadlineNanos;
        this.reschedulingDelayNanos = reschedulingDelayNanos;
        state = new AtomicInteger();
        handleRelease = new BinaryLatch();
        cancelListeners = new CopyOnWriteArrayList<>();
        this.task = () ->
        {
            try
            {
                task.run();
                // Use compareAndSet to avoid overriding any cancellation state.
                compareAndSetState( STATE_SUBMITTED, STATE_RUNNABLE );
            }
            catch ( Throwable e )
            {
                lastException = e;
                state.set( STATE_FAILED );
            }
        };
    }

    boolean compareAndSetState( int expect, int update )
    {
        return state.compareAndSet( expect, update );
    }

    int getState()
    {
        return state.get();
    }

    long getReschedulingDelayNanos()
    {
        return reschedulingDelayNanos;
    }

    void submitTo( ThreadPoolManager pools )
    {
        latestHandle = pools.submit( group, task );
        handleRelease.release();
    }

    @Override
    public void cancel( boolean mayInterruptIfRunning )
    {
        state.set( STATE_FAILED );
        JobHandle handle = latestHandle;
        if ( handle != null )
        {
            handle.cancel( mayInterruptIfRunning );
        }
        for ( CancelListener cancelListener : cancelListeners )
        {
            cancelListener.cancelled( mayInterruptIfRunning );
        }
    }

    @Override
    public void waitTermination() throws ExecutionException, InterruptedException
    {
        handleRelease.await();
        latestHandle.waitTermination();
        if ( state.get() == STATE_FAILED )
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
