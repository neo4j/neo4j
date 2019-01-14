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
package org.neo4j.concurrent;

import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;

/**
 * Turns multi-threaded unary work into single-threaded stack work.
 * <p>
 * The technique used here is inspired in part both by the Flat Combining
 * concept from Hendler, Incze, Shavit &amp; Tzafrir, and in part by the
 * wait-free linked queue design by Vyukov.
 * </p>
 * <p>
 * In a sense, this turns many small, presumably concurrent, pieces of work
 * into fewer, larger batches of work, that is then applied to the material
 * under synchronisation.
 * </p>
 * <p>
 * Obviously this only makes sense for work that a) can be combined, and b)
 * where the performance improvements from batching effects is large enough
 * to overcome the overhead of collecting and batching up the work units.
 * </p>
 *
 * @see Work
 */
@SuppressWarnings( {"unchecked", "NumberEquality"} )
public class WorkSync<Material, W extends Work<Material,W>>
{
    private final Material material;
    private final AtomicReference<WorkUnit<Material,W>> stack;
    private static final WorkUnit<?,?> stackEnd = new WorkUnit<>( null, null );
    private final AtomicReference<Thread> lock;

    /**
     * Create a new WorkSync that will synchronize the application of work to
     * the given material.
     *
     * @param material The material we want to apply work to, in a thread-safe
     * way.
     */
    public WorkSync( Material material )
    {
        this.material = material;
        this.stack = new AtomicReference<>( (WorkUnit<Material,W>) stackEnd );
        this.lock = new AtomicReference<>();
    }

    /**
     * Apply the given work to the material in a thread-safe way, possibly by
     * combining it with other work.
     *
     * @param work The work to be done.
     * @throws ExecutionException if this thread ends up performing the piled up work,
     * and any work unit in the pile throws an exception. Thus the current thread is not
     * guaranteed to observe any exception its unit of work might throw, since the
     * exception will be thrown in whichever thread that ends up actually performing the work.
     */
    public void apply( W work ) throws ExecutionException
    {
        // Schedule our work on the stack.
        WorkUnit<Material,W> unit = enqueueWork( work );

        // Try grabbing the lock to do all the work, until our work unit
        // has been completed.
        int tryCount = 0;
        do
        {
            tryCount++;
            checkFailure( tryDoWork( unit, tryCount, true ) );
        }
        while ( !unit.isDone() );
    }

    /**
     * Apply the given work to the material in a thread-safe way, possibly asynchronously if contention is observed
     * with other threads, and possibly by combining it with other work.
     * <p>
     * The work will be applied immediately, if no other thread is contending for the material. Otherwise, the work
     * will be enqueued for later application, which may occur on the next call to {@link #apply(Work)} on this
     * {@code WorkSync}, or the next call to {@link AsyncApply#await()} from an {@code AsyncApply} instance created
     * from this {@code WorkSync}. These calls, and thus the application of the enqueued work, may occur in an
     * arbitrary thread.
     * <p>
     * The returned {@link AsyncApply} instance is not thread-safe. If so desired, its ownership can be transferred to
     * other threads, but only in a way that ensures safe publication.
     * <p>
     * If the given work causes an exception to be thrown, then that exception will only be observed by the thread that
     * ultimately applies the work. Thus, exceptions caused by this work are not guaranteed to be associated with, or
     * made visible via, the returned {@link AsyncApply} instance.
     *
     * @param work The work to be done.
     * @return An {@link AsyncApply} instance representing the enqueued - and possibly completed - work.
     */
    public AsyncApply applyAsync( W work )
    {
        // Schedule our work on the stack.
        WorkUnit<Material,W> unit = enqueueWork( work );

        // Apply the work if the lock is immediately available.
        Throwable initialThrowable = tryDoWork( unit, 100, false );

        return new AsyncApply()
        {
            Throwable throwable = initialThrowable;

            @Override
            public void await() throws ExecutionException
            {

                checkFailure( throwable );
                int tryCount = 0;
                while ( !unit.isDone() )
                {
                    tryCount++;
                    checkFailure( throwable = tryDoWork( unit, tryCount, true ) );
                }
            }
        };
    }

    private WorkUnit<Material,W> enqueueWork( W work )
    {
        WorkUnit<Material,W> unit = new WorkUnit<>( work, Thread.currentThread() );
        unit.next = stack.getAndSet( unit ); // benign race, see the batch.next read-loop in combine()
        return unit;
    }

    private Throwable tryDoWork( WorkUnit<Material,W> unit, int tryCount, boolean block )
    {
        if ( tryLock( tryCount, unit, block ) )
        {
            WorkUnit<Material,W> batch = grabBatch();
            try
            {
                return doSynchronizedWork( batch );
            }
            finally
            {
                unlock();
                unparkAnyWaiters();
                markAsDone( batch );
            }
        }
        return null;
    }

    private void unparkAnyWaiters()
    {
        WorkUnit<Material,W> waiter = stack.get();
        if ( waiter != stackEnd )
        {
            waiter.unpark();
        }
    }

    private void checkFailure( Throwable failure ) throws ExecutionException
    {
        if ( failure != null )
        {
            throw new ExecutionException( failure );
        }
    }

    private boolean tryLock( int tryCount, WorkUnit<Material,W> unit, boolean block )
    {
        if ( lock.compareAndSet( null, Thread.currentThread() ) )
        {
            // Got the lock!
            return true;
        }

        // Did not get the lock, spend some time until our work has either been completed,
        // or we get the lock.
        if ( tryCount < 10 )
        {
            // todo Java9: Thread.onSpinWait() ?
            Thread.yield();
        }
        else if ( block )
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

    private WorkUnit<Material,W> grabBatch()
    {
        return stack.getAndSet( (WorkUnit<Material,W>) stackEnd );
    }

    private Throwable doSynchronizedWork( WorkUnit<Material,W> batch )
    {
        W combinedWork = combine( batch );
        Throwable failure = null;

        if ( combinedWork != null )
        {
            try
            {
                combinedWork.apply( material );
            }
            catch ( Throwable throwable )
            {
                failure = throwable;
            }
        }
        return failure;
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

            WorkUnit<Material,W> tmp = batch.next;
            while ( tmp == null )
            {
                // We may see 'null' via race, as work units are put on the
                // stack before their 'next' pointers are updated. We just spin
                // until we observe their volatile write to 'next'.
                // todo Java9: Thread.onSpinWait() ?
                Thread.yield();
                tmp = batch.next;
            }
            batch = tmp;
        }
        return result;
    }

    private void markAsDone( WorkUnit<Material,W> batch )
    {
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

        final W work;
        final Thread owner;
        volatile WorkUnit<Material,W> next;

        private WorkUnit( W work, Thread owner )
        {
            this.work = work;
            this.owner = owner;
        }

        void park( long time, TimeUnit unit )
        {
            if ( compareAndSet( STATE_QUEUED, STATE_PARKED ) )
            {
                LockSupport.parkNanos( unit.toNanos( time ) );
                compareAndSet( STATE_PARKED, STATE_QUEUED );
            }
        }

        boolean isDone()
        {
            return get() == STATE_DONE;
        }

        void complete()
        {
            int previousState = getAndSet( STATE_DONE );
            if ( previousState == STATE_PARKED )
            {
                unpark();
            }
        }

        void unpark()
        {
            LockSupport.unpark( owner );
        }
    }
}
