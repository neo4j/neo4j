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

import java.util.concurrent.Callable;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;

import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.resources.Profiler;

/**
 * To be expanded, the idea here is to have a database-global service for running jobs, handling jobs crashing and so
 * on.
 */
public interface JobScheduler extends Lifecycle, AutoCloseable
{
    /**
     * Assign a specific name to the top-most scheduler group.
     * <p>
     * This is just a suggestion for debugging purpose. The specific scheduler implementation is free to ignore calls
     * to this method.
     */
    void setTopLevelGroupName( String name );

    /**
     * Set the desired parallelism for the given group. This only has an effect if the underlying scheduler for the given group has not already been
     * started, and a desired parallelism has not already been set.
     *
     * @param group The group to set the desired parallelism for.
     * @param parallelism The desired number of threads in the thread pool for the given group.
     */
    void setParallelism( Group group, int parallelism );

    /**
     * Assign a {@link SchedulerThreadFactory} to a given group. This only has an effect if the underlying scheduler for the given group has not already been
     * started.
     *
     * @param group The group to assign the given thread factory for.
     * @param threadFactory The thread factory to assign.
     */
    void setThreadFactory( Group group, SchedulerThreadFactoryFactory threadFactory );

    /**
     * Expose a group scheduler as an {@link Executor}.
     * <p>
     * <strong>NOTE:</strong> The returned instance might be an implementation of the {@link ExecutorService} interface. If so, then it is <em>NOT</em> allowed
     * to shut it down, because the life cycle of the executor is managed by the JobScheduler.
     **/
    Executor executor( Group group );

    /**
     * Expose a group scheduler as a {@link java.util.concurrent.ThreadFactory}.
     * This is a lower-level alternative than {@link #executor(Group)}, where you are in control of when to spin
     * up new threads for your jobs.
     * <p>
     * The lifecycle of the threads you get out of here are not managed by the JobScheduler, you own the lifecycle and
     * must start the thread before it can be used.
     * <p>
     * This mechanism is strongly preferred over manually creating threads, as it allows a central place for record
     * keeping of thread creation, central place for customizing the threads based on their groups, and lays a
     * foundation for controlling things like thread affinity and priorities in a coordinated manner in the future.
     */
    ThreadFactory threadFactory( Group group );

    /** Schedule a new callable in the specified group. */
    <T> JobHandle<T> schedule( Group group, Callable<T> job );

    /** Schedule a new job in the specified group. */
    JobHandle<?> schedule( Group group, Runnable job );

    /** Schedule a new job in the specified group with the given delay */
    JobHandle<?> schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit );

    /** Schedule a recurring job */
    JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit );

    /** Schedule a recurring job where the first invocation is delayed the specified time */
    JobHandle<?> scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit );

    /**
     * Return a stream of all active scheduling groups.
     * @return all active groups.
     */
    Stream<ActiveGroup> activeGroups();

    /**
     * Initiate profiling of all threads within the given group, using the given profiler.
     * @param group the group with all the threads to profile.
     * @param profiler the profiler to use.
     */
    void profileGroup( Group group, Profiler profiler );
}
