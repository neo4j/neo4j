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
package org.neo4j.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ForkJoinTask;

import static java.lang.Runtime.getRuntime;
import static java.util.concurrent.Executors.newCachedThreadPool;
import static java.util.concurrent.Executors.newFixedThreadPool;
import static java.util.concurrent.Executors.newSingleThreadExecutor;

/**
 * Implementations of this interface are used by the {@link JobHandle} implememtation to create the underlying {@link ExecutorService}s that actually run the
 * scheduled jobs. The choice of implementation is decided by the scheduling {@link Group}, which can thereby influence how jobs in the particular group are
 * executed.
 */
interface ExecutorServiceFactory
{
    /**
     * Create an {@link ExecutorService} with a default thread count.
     */
    ExecutorService build( Group group, SchedulerThreadFactory factory );

    /**
     * Create an {@link ExecutorService}, ideally with the desired thread count if possible.
     * Implementations are allowed to ignore the given thread count.
     */
    ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount );

    /**
     * This factory actually prevents the scheduling and execution of any jobs, which is useful for groups that are not meant to be scheduled directly.
     */
    static ExecutorServiceFactory unschedulable()
    {
        return new ExecutorServiceFactory()
        {
            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory )
            {
                throw newUnschedulableException( group );
            }

            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount )
            {
                throw newUnschedulableException( group );
            }

            private IllegalArgumentException newUnschedulableException( Group group )
            {
                return new IllegalArgumentException( "Tasks cannot be scheduled directly to the " + group.groupName() + " group." );
            }
        };
    }

    /**
     * Executes all jobs in the same single thread.
     */
    static ExecutorServiceFactory singleThread()
    {
        return new ExecutorServiceFactory()
        {
            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory )
            {
                return newSingleThreadExecutor( factory );
            }

            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount )
            {
                return build( group, factory ); // Just ignore the thread count.
            }
        };
    }

    /**
     * Execute jobs in a dynamically growing pool of threads. The threads will be cached and kept around for a little while to cope with work load spikes
     * and troughs.
     */
    static ExecutorServiceFactory cached()
    {
        return new ExecutorServiceFactory()
        {
            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory )
            {
                return newCachedThreadPool( factory );
            }

            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount )
            {
                return newFixedThreadPool( threadCount, factory );
            }
        };
    }

    /**
     * Schedules jobs in a work-stealing (ForkJoin) thread pool. {@link java.util.stream.Stream#parallel Parallel streams} and {@link ForkJoinTask}s started
     * from within the scheduled jobs will also run inside the same {@link ForkJoinPool}.
     */
    static ExecutorServiceFactory workStealing()
    {
        return new ExecutorServiceFactory()
        {
            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory )
            {
                return new ForkJoinPool( getRuntime().availableProcessors(), factory, null, false );
            }

            @Override
            public ExecutorService build( Group group, SchedulerThreadFactory factory, int threadCount )
            {
                return new ForkJoinPool( threadCount, factory, null, false );
            }
        };
    }
}
