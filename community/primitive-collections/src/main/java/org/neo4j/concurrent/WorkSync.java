/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Turns multi-threaded unary work into single-threaded stack work.
 * <p>
 *     The technique used here is inspired in part both by the Flat Combining
 *     concept from Hendler, Incze, Shavit &amp; Tzafrir, and in part by the
 *     wait-free linked queue design by Vyukov.
 * </p>
 * <p>
 *     In a sense, this turns many small, presumably concurrent, pieces of work
 *     into fewer, larger batches of work, that is then applied to the material
 *     under synchronisation.
 * </p>
 * <p>
 *     Obviously this only makes sense for work that a) can be combined, and b)
 *     where the performance improvements from batching effects is large enough
 *     to overcome the overhead of collecting and batching up the work units.
 * </p>
 * @see Work
 */
public class WorkSync<Material, W extends Work<Material,W>>
{
    private final Material material;
    private final AtomicReference<WorkUnit<Material,W>> stack;
    private final WorkUnit<Material,W> stackEnd;
    private final AtomicReference<Thread> lock;

    /**
     * Create a new WorkSync that will synchronize the application of work to
     * the given material.
     * @param material The material we want to apply work to, in a thread-safe
     * way.
     */
    public WorkSync( Material material )
    {
        this.material = material;
        this.stackEnd = new WorkUnit<>( null, null, null );
        this.stack = new AtomicReference<>( stackEnd );
        this.lock = new AtomicReference<Thread>();
    }

    /**
     * Apply the given work to the material in a thread-safe way, possibly by
     * combining it with other work.
     * @param work The work to be done.
     */
    public void apply( W work )
    {
        // Schedule our work on the stack.
        WorkUnit<Material,W> unit = new WorkUnit<Material,W>( work, Thread.currentThread(), stackEnd );
        unit.next = stack.getAndSet( unit ); // benign race, see reverse()

        // Try grabbing the lock to do all the work, until our work unit
        // has been completed.
        boolean wasInterrupted = false;
        int tryCount = 0;
        do
        {
            tryCount++;
            try
            {
                if ( tryLock( tryCount, unit ) )
                {
                    try
                    {
                        doSynchronizedWork();
                    }
                    finally
                    {
                        unlock();
                    }
                }
            }
            catch ( InterruptedException e )
            {
                // We can't stop now, because our work has already been
                // scheduled. So instead we're just going to reset the
                // interruption status when we're done.
                wasInterrupted = true;
            }
        }
        while ( !unit.isDone() );

        if ( wasInterrupted )
        {
            Thread.currentThread().interrupt();
        }
    }

    private boolean tryLock( int tryCount, WorkUnit<Material,W> unit ) throws InterruptedException
    {
        if ( lock.compareAndSet( null, Thread.currentThread() ) )
        {
            // Got the lock!
            return true;
        }

        // Did not get the lock, spend some time until our work has aither been completed,
        // or we get the lock.
        if ( tryCount < 10 )
        {
            // todo Thread.onSpinWait() ?
            Thread.yield();
        }
        else
        {
            unit.park( 10, TimeUnit.MILLISECONDS );
        }
        return false;
    }

    private void unlock()
    {
        if ( lock.getAndSet( null ) != Thread.currentThread() )
        {
            throw new IllegalMonitorStateException(
                    "WorkSync accidentally released a lock not owned by the current thread" );
        }
    }

    private void doSynchronizedWork()
    {
        WorkUnit<Material,W> batch = reverse( stack.getAndSet( stackEnd ) );
        W combinedWork = combine( batch );

        if ( combinedWork != null )
        {
            combinedWork.apply( material );
        }

        markAsDone( batch );
    }

    private WorkUnit<Material,W> reverse( WorkUnit<Material,W> batch )
    {
        WorkUnit<Material,W> result = stackEnd;
        while ( batch != stackEnd )
        {
            WorkUnit<Material,W> tmp = batch.next;
            while ( tmp == null )
            {
                // We may see 'null' via race, as work units are put on the
                // stack before their 'next' pointers are updated. We just spin
                // until we observe their volatile write to 'next'.
                Thread.yield();
                tmp = batch.next;
            }
            batch.next = result;
            result = batch;
            batch = tmp;
        }
        return result;
    }

    private W combine( WorkUnit<Material,W> batch )
    {
        W result = null;
        while ( batch != stackEnd )
        {
            if ( result == null )
            {
                result = batch.work;
            }
            else
            {
                result = result.combine( batch.work );
            }

            do
            {
                batch = batch.next;
            }
            while ( batch.isCancelled() );
        }
        return result;
    }

    private void markAsDone( WorkUnit<Material,W> batch )
    {
        // We will mark the first work unit as the successor - the thread in charge of unparking
        // the other blocked threads - before we begin marking the units as done.
        // This way, if the successor wakes up before we finish marking the units as done, it will
        // know that it should wait and complete its task. In fact, it may trail our done-marking
        // process and unpark threads in parallel.
        batch.setAsSuccessor();
        WorkUnit<Material,W> successor = batch;
        while ( batch != stackEnd )
        {
            batch.complete();
            batch = batch.next;
        }
    }

    private static class WorkUnit<Material, W extends Work<Material,W>> extends AtomicInteger
    {
        static final int STATE_QUEUED = 0;
        static final int STATE_PARKED = 1;
        static final int STATE_DONE = 2;
        static final int STATE_CANCELLED = 3;

        final W work;
        final WorkUnit<Material,W> stackEnd;
        final Thread owner;
        volatile WorkUnit<Material,W> next;
        volatile boolean successor;

        private WorkUnit( W work, Thread owner, WorkUnit<Material,W> stackEnd )
        {
            this.work = work;
            this.owner = owner;
            this.stackEnd = stackEnd;
        }

        void park( long time, TimeUnit unit )
        {
            if ( compareAndSet( STATE_QUEUED, STATE_PARKED ) )
            {
                LockSupport.parkNanos( unit.toNanos( time ) );
                compareAndSet( STATE_PARKED, STATE_QUEUED );
            }
            if ( successor )
            {
                // It's our job to go through all the 'next' references, and unpark all the threads
                // for the work units that are marked as done.
                // If we encounter a unit that isn't marked as done, then we have raced with the
                // done-marking, and we just spin until we observe the done flag. This is pretty
                // unlikely, since unparking a thread is pretty expensive on most platforms.
                // We also spin on null 'next' references, in the unlikely event that our thread
                // yet observed the complete enqueueing of work units prior to ours.
                wakeUpAsSuccessor();
            }
        }

        private void wakeUpAsSuccessor()
        {
            WorkUnit<Material,W> t, n;
            do
            {
                n = next;
            }
            while ( n == null );

            do
            {
                boolean canUnparkNext;
                do
                {
                    canUnparkNext = n.isDone();
                }
                while ( !canUnparkNext );
                n.unpark();
                do
                {
                    t = n.next;
                }
                while ( t == null );
                n = t;
            }
            while ( n != stackEnd );
        }

        boolean isDone()
        {
            return get() == STATE_DONE;
        }

        boolean isCancelled()
        {
            return get() == STATE_CANCELLED;
        }

        void complete()
        {
            int previousState = getAndSet( STATE_DONE );
            if ( successor && previousState == STATE_PARKED )
            {
                unpark();
            }
        }

        void setAsSuccessor()
        {
            successor = true;
        }

        void unpark()
        {
            LockSupport.unpark( owner );
        }
    }
}
