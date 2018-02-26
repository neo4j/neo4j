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

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;

import org.neo4j.scheduler.JobScheduler.Group;
import org.neo4j.scheduler.JobScheduler.JobHandle;
import org.neo4j.time.SystemNanoClock;

final class TimeBasedTaskScheduler implements Runnable
{
    private static final ScheduledTask END_SENTINEL = new ScheduledTask( null, null, 0, 0 );
    private static final long NO_TASKS_PARK = TimeUnit.MINUTES.toNanos( 10 );

    private final SystemNanoClock clock;
    private final ThreadPoolManager pools;
    private final AtomicReference<ScheduledTask> inbox;
    private volatile Thread timeKeeper;
    private volatile boolean stopped;
    // This field is only access by the time keeper thread:
    private ScheduledTask delayedTasks;

    TimeBasedTaskScheduler( SystemNanoClock clock, ThreadPoolManager pools )
    {
        this.clock = clock;
        this.pools = pools;
        inbox = new AtomicReference<>( END_SENTINEL );
    }

    public JobHandle submit( Group group, Runnable job, long initialDelayNanos, long reschedulingDelayNanos )
    {
        long now = clock.nanos();
        long nextDeadlineNanos = now + initialDelayNanos;
        ScheduledTask task = new ScheduledTask( group, job, nextDeadlineNanos, reschedulingDelayNanos );
        task.next = inbox.getAndSet( task );
        LockSupport.unpark( timeKeeper );
        return task;
    }

    public long tick()
    {
        long now = clock.nanos();
        sortInbox();
        return scheduleDueTasks( now );
    }

    private void sortInbox()
    {
        ScheduledTask newTasks = inbox.getAndSet( END_SENTINEL );
        while ( newTasks != END_SENTINEL )
        {
            // Capture next chain link before enqueueing among delayed tasks.
            ScheduledTask next;
            do
            {
                next = newTasks.next;
            }
            while ( next == null );

            enqueueTask( newTasks );

            newTasks = next;
        }
    }

    private void enqueueTask( ScheduledTask newTasks )
    {
        if ( delayedTasks == null || newTasks.nextDeadlineNanos <= delayedTasks.nextDeadlineNanos )
        {
            newTasks.next = delayedTasks;
            delayedTasks = newTasks;
        }
        else
        {
            ScheduledTask head = delayedTasks;
            while ( head.next != null && head.next.nextDeadlineNanos < newTasks.nextDeadlineNanos )
            {
                head = head.next;
            }
            newTasks.next = head.next;
            head.next = newTasks;
        }
    }

    private long scheduleDueTasks( long now )
    {
        if ( delayedTasks == null )
        {
            // We have no tasks to run. Park until we're woken up by a submit().
            return NO_TASKS_PARK;
        }
        ScheduledTask due = spliceOutDueTasks( now );
        submitAndEnqueueTasks( due, now );
        return delayedTasks == null ? NO_TASKS_PARK : delayedTasks.nextDeadlineNanos - now;
    }

    private ScheduledTask spliceOutDueTasks( long now )
    {
        ScheduledTask due = null;
        ScheduledTask delayed = delayedTasks;
        while ( delayed != null && delayed.nextDeadlineNanos <= now )
        {
            ScheduledTask next = delayed.next;
            delayed.next = due;
            due = delayed;
            delayed = next;
        }
        delayedTasks = delayed;
        return due;
    }

    private void submitAndEnqueueTasks( ScheduledTask due, long now )
    {
        while ( due != null )
        {
            ScheduledTask next = due.next;
            if ( due.compareAndSetState( ScheduledTask.STATE_RUNNABLE, ScheduledTask.STATE_SUBMITTED ) )
            {
                long reschedulingDelayNanos = due.getReschedulingDelayNanos();
                due.nextDeadlineNanos = reschedulingDelayNanos + now;
                due.submitTo( pools );
                if ( reschedulingDelayNanos > 0 )
                {
                    enqueueTask( due );
                }
                // If the rescheduling delay is zero or less, then this wasn't a recurring task, but just a delayed one,
                // which means we don't enqueue it again.
            }
            else if ( due.getState() != ScheduledTask.STATE_FAILED )
            {
                // It's still running, so it's now overdue.
                due.nextDeadlineNanos = now;
                enqueueTask( due );
            }
            // Otherwise it's failed, in which case we just throw it away, and continue processing the chain.
            due = next;
        }
    }

    @Override
    public void run()
    {
        timeKeeper = Thread.currentThread();
        while ( !stopped )
        {
            long timeToNextTickNanos = tick();
            LockSupport.parkNanos( this, timeToNextTickNanos );
        }
    }

    public void stop()
    {
        stopped = true;
        LockSupport.unpark( timeKeeper );
    }
}
