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
package org.neo4j.concurrent;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

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
 *     Obviously this only makes sense for that a) can be combined, and b)
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
    private final ReentrantLock lock;

    /**
     * Create a new WorkSync that will synchronize the application of work to
     * the given material.
     * @param material The material we want to apply work to, in a thread-safe
     * way.
     */
    public WorkSync( Material material )
    {
        this.material = material;
        this.stackEnd = new WorkUnit<>( null );
        this.stack = new AtomicReference<>( stackEnd );
        this.lock = new ReentrantLock();
    }

    /**
     * Apply the given work to the material in a thread-safe way, possibly by
     * combining it with other work.
     * @param work The work to be done.
     */
    public void apply( W work )
    {
        // Schedule our work on the stack.
        WorkUnit<Material,W> unit = new WorkUnit<>( work );
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
                if ( lock.tryLock( tryCount < 10? 0 : 10, TimeUnit.MILLISECONDS ) )
                {
                    try
                    {
                        doSynchronizedWork();
                    }
                    finally
                    {
                        lock.unlock();
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
        while ( !unit.done );

        if ( wasInterrupted )
        {
            Thread.currentThread().interrupt();
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
            batch = batch.next;
        }
        return result;
    }

    private void markAsDone( WorkUnit<Material,W> batch )
    {
        while ( batch != stackEnd )
        {
            batch.done = true;
            batch = batch.next;
        }
    }

    private static class WorkUnit<Material, W extends Work<Material,W>>
    {
        final W work;
        volatile WorkUnit<Material,W> next;
        volatile boolean done;

        private WorkUnit( W work )
        {
            this.work = work;
        }
    }
}
