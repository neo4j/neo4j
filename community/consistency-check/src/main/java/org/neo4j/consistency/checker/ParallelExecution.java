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
package org.neo4j.consistency.checker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import java.util.stream.Collectors;

import org.neo4j.internal.helpers.NamedThreadFactory;
import org.neo4j.internal.helpers.collection.LongRange;
import org.neo4j.kernel.impl.store.RecordStore;
import org.neo4j.util.concurrent.Futures;

import static java.lang.Long.min;

/**
 * Contains logic for executing checker tasks in parallel.
 */
class ParallelExecution
{
    static final Consumer<Throwable> NOOP_EXCEPTION_HANDLER = t -> {};
    static final int DEFAULT_IDS_PER_CHUNK = 1_000_000;

    private final int numberOfThreads;
    private final Consumer<Throwable> exceptionHandler;
    private int idsPerChunk;

    ParallelExecution( int numberOfThreads, Consumer<Throwable> exceptionHandler, int idsPerChunk )
    {
        this.numberOfThreads = numberOfThreads;
        this.exceptionHandler = exceptionHandler;
        this.idsPerChunk = idsPerChunk;
    }

    /**
     * Runs the given tasks with a thread pool with a fixed number of threads, which is the number of threads given to this
     * {@link ParallelExecution} instance at construction time. The number of jobs may exceed this number.
     *
     * @param taskName name that the spawned threads will get.
     * @param runnables jobs to run.
     * @throws Exception on any job exception.
     */
    void run( String taskName, ThrowingRunnable... runnables ) throws Exception
    {
        run( taskName, numberOfThreads, runnables );
    }

    /**
     * Runs the given tasks with a thread pool with number of threads set to the number of tasks.
     * I.e. all the given jobs will be run concurrently.
     *
     * @param taskName name that the spawned threads will get.
     * @param runnables jobs to run.
     * @throws Exception on any job exception.
     */
    void runAll( String taskName, ThrowingRunnable... runnables ) throws Exception
    {
        run( taskName, runnables.length, runnables );
    }

    private void run( String taskName, int numberOfThreads, ThrowingRunnable... runnables ) throws Exception
    {
        var pool = Executors.newFixedThreadPool( numberOfThreads, new NamedThreadFactory( getClass().getSimpleName() + "-" + taskName ) );
        try
        {
            List<InternalTask> tasks = Arrays.stream( runnables ).map( InternalTask::new ).collect( Collectors.toList() );
            Futures.getAllResults( pool.invokeAll( tasks ) );
        }
        finally
        {
            pool.shutdown();
        }
    }

    ThrowingRunnable[] partition( RecordStore<?> store, RangeOperation rangeOperation )
    {
        LongRange range = LongRange.range( store.getNumberOfReservedLowIds(), store.getHighId() );
        return partition( range, rangeOperation );
    }

    ThrowingRunnable[] partition( LongRange range, RangeOperation rangeOperation )
    {
        List<ThrowingRunnable> partitions = new ArrayList<>();
        for ( long id = range.from(); id < range.to(); id += idsPerChunk )
        {
            long to = min( id + idsPerChunk, range.to() );
            boolean last = to == range.to();
            partitions.add( rangeOperation.operation( id, to, last ) );
        }
        return partitions.toArray( new ThrowingRunnable[0] );
    }

    int getNumberOfThreads()
    {
        return numberOfThreads;
    }

    interface ThrowingRunnable extends Callable<Void>
    {
        @Override
        default Void call() throws Exception
        {
            doRun();
            return null;
        }

        void doRun() throws Exception;
    }

    private class InternalTask implements Callable<Void>
    {
        private final ThrowingRunnable runnable;
        InternalTask( ThrowingRunnable runnable )
        {
            this.runnable = runnable;
        }

        @Override
        public Void call() throws Exception
        {
            try
            {
                runnable.call();
            }
            catch ( Throwable t )
            {
                exceptionHandler.accept( t );
                throw t;
            }
            return null;
        }
    }

    interface RangeOperation
    {
        ThrowingRunnable operation( long from, long to, boolean last );
    }
}
