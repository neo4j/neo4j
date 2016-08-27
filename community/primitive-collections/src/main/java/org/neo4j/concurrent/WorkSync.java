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

import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
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
public class WorkSync<Material, W extends Work<Material,W>>
{
    private final Material material;
    private final AtomicReference<WorkUnit<Material,W>> stack;
    private final WorkUnit<Material,W> stackEnd;
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
        this.stackEnd = new WorkUnit<>( null, null, null );
        this.stackEnd.complete();
        this.stack = new AtomicReference<>( stackEnd );
        this.lock = new AtomicReference<>();
    }

    /**
     * Apply the given work to the material in a thread-safe way, possibly by
     * combining it with other work.
     *
     * @param work The work to be done.
     * @throws ExecutionException if this thread ends up performing the piled up work,
     * and any work unit in the pile throws an exception. Thus the current thread is not
     * gauranteed to observe any exception its unit of work might throw, since the
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
     * Apply the given work to the material in a thread-safe way, possibly asynchronously
     * if contention is observed with other threads, and possibly by combining it with other work.
     * <p>
     * The returned future can be cancelled while it is still enqueued to run, but cancellation
     * may race with work combining and application, and have slightly weaker semantics than what
     * the {@link Future} interface specifies. for instance, a unit of that is cancelled, and where
     * the {@link Future#cancel(boolean)} cancel} method returns {@code true}, may still and up
     * being applied.
     * The reason for this departure from specification is implementation efficiency of the other
     * {@code WorkSync} features, since the cancellation feature is likely rarely used.
     * <p>
     * The given unit of work may be done by this thread, or any other thread that is concurrently
     * submitting work to the {@code WorkSync}. If this unit of work, or any of the other units of
     * work, throws an exception, then the exception will surface in the thread that ends up doing
     * the work. This may manifest itself as an {@link ExecutionException} thrown from one of the
     * {@code get} methods of {@link Future}, or from the {@link #apply(Work)} method.
     *
     * @param work The work to be done.
     * @return A {@link Future} representing the eventual completion of the work.
     */
    public Future<?> applyAsync( W work )
    {
        // Schedule our work on the stack.
        WorkUnit<Material,W> unit = enqueueWork( work );

        // Make an attempt at doing the work immediately.
        Throwable failure = tryDoWork( unit, 0, false );
        if ( failure != null )
        {
            return new FutureThrow( failure );
        }

        // Otherwise return a future where the 'get' methods will do the work,
        // if it hasn't been done already.
        return new Future<Object>()
        {

            @Override
            public boolean cancel( boolean mayInterruptIfRunning )
            {
                return unit.cancel();
            }

            @Override
            public boolean isCancelled()
            {
                return unit.isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return unit.isDone();
            }

            @Override
            public Object get() throws InterruptedException, ExecutionException
            {
                int tryCount = 0;
                while ( !unit.isDone() )
                {
                    if ( Thread.interrupted() )
                    {
                        throw new InterruptedException();
                    }
                    tryCount++;
                    checkFailure( tryDoWork( unit, tryCount, true ) );
                }
                unit.checkCancelled();
                return null;
            }

            @Override
            public Object get( long timeout, TimeUnit timeUnit )
                    throws InterruptedException, ExecutionException, TimeoutException
            {
                Objects.requireNonNull( timeUnit );
                if ( unit.isDone() )
                {
                    unit.checkCancelled();
                    return null; // short-circuit potentially expensive calls to nanoTime.
                }
                long deadline = System.nanoTime() + timeUnit.toNanos( timeout );
                int tryCount = 0;
                while ( System.nanoTime() < deadline )
                {
                    if ( Thread.interrupted() )
                    {
                        throw new InterruptedException();
                    }
                    if ( unit.isDone() )
                    {
                        unit.checkCancelled();
                        return null;
                    }
                    tryCount++;
                    checkFailure( tryDoWork( unit, tryCount, true ) );
                }
                throw new TimeoutException();
            }
        };
    }

    private WorkUnit<Material,W> enqueueWork( W work )
    {
        WorkUnit<Material,W> unit = new WorkUnit<>( work, Thread.currentThread(), stackEnd );
        unit.next = stack.getAndSet( unit ); // benign race, see reverse()
        return unit;
    }

    private Throwable tryDoWork( WorkUnit<Material,W> unit, int tryCount, boolean otherwisePark )
    {
        if ( tryLock( tryCount, unit, otherwisePark ) )
        {
            try
            {
                return doSynchronizedWork();
            }
            finally
            {
                unlock();
                WorkUnit<Material,W> nextBatch = stack.get();
                if ( !nextBatch.isDone() )
                {
                    nextBatch.unpark();
                }
            }
        }
        return null;
    }

    private void checkFailure( Throwable failure ) throws ExecutionException
    {
        if ( failure != null )
        {
            throw new ExecutionException( failure );
        }
    }

    private boolean tryLock( int tryCount, WorkUnit<Material,W> unit, boolean otherwisePark )
    {
        if ( lock.compareAndSet( null, Thread.currentThread() ) )
        {
            // Got the lock!
            return true;
        }

        // Did not get the lock, spend some time until our work has aither been completed,
        // or we get the lock.
        if ( otherwisePark )
        {
            if ( tryCount < 1000 )
            {
                // todo Thread.onSpinWait() in Java9?
                Thread.yield();
            }
            else
            {
                unit.park( 10, TimeUnit.MILLISECONDS );
            }
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

    private Throwable doSynchronizedWork()
    {
        WorkUnit<Material,W> batch = reverseAndFilterCancelled( stack.getAndSet( stackEnd ) );
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

        markAsDone( batch );
        return failure;
    }

    private WorkUnit<Material,W> reverseAndFilterCancelled( WorkUnit<Material,W> batch )
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
            if ( !batch.isCancelled() )
            {
                batch.next = result;
                result = batch;
            }
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

            batch = batch.next;
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
        while ( batch != stackEnd )
        {
            batch.complete();
            WorkUnit<Material,W> tmp = batch.next;
            // The batch linked-list has been reversed, so oldest WorkUnits point to newer objects.
            // Because of this, we null out the next reference here, to reduce the probability of
            // ending up with a WorkUnit that has been promoted to the old-gen, that has a reference
            // to objects in the new-gen. Old-gen objects are always considered live during new-gen
            // collections, so a dead old-gen object can keep new-gen objects alive longer than
            // necessary, causing a cascading premature promotion.
            batch.next = null;
            batch = tmp;
        }
    }

    private static class WorkUnit<Material, W extends Work<Material,W>> extends AtomicInteger
    {
        private static final int STATE_QUEUED = 0b00;
        private static final int STATE_PARKED = 0b01;
        private static final int STATE_DONE = 0b10;
        private static final int STATE_CANCELLED = 0b11;

        private final W work;
        private final WorkUnit<Material,W> stackEnd;
        private final Thread owner;
        private volatile WorkUnit<Material,W> next;
        private volatile boolean successor;

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

        boolean isDone()
        {
            return (get() & STATE_DONE) == STATE_DONE;
        }

        boolean isCancelled()
        {
            return get() == STATE_CANCELLED;
        }

        boolean cancel()
        {
            int state;
            do
            {
                state = get();
                if ( state == STATE_DONE )
                {
                    return false;
                }
            }
            while ( !compareAndSet( state, STATE_CANCELLED ) );
            return true;
        }

        void checkCancelled()
        {
            if ( isCancelled() )
            {
                throw new CancellationException();
            }
        }
    }

    private static final class FutureThrow extends FutureTask<Object>
    {
        private static final Callable<Object> NULL_CALLABLE = () -> null;

        FutureThrow( Throwable result )
        {
            super( NULL_CALLABLE );
            setException( result );
        }
    }
}
