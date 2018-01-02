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
package org.neo4j.kernel.impl.util;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.Executor;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.lifecycle.Lifecycle;

import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.NEW_THREAD;
import static org.neo4j.kernel.impl.util.JobScheduler.SchedulingStrategy.POOLED;

/**
 * To be expanded, the idea here is to have a database-global service for running jobs, handling jobs crashing and so on.
 */
public interface JobScheduler extends Lifecycle
{
    enum SchedulingStrategy
    {
        /** Create a new thread each time a job is scheduled */
        NEW_THREAD,
        /** Run the job from a pool of threads, shared among all groups with this strategy */
        POOLED
    }

    /**
     * Represents a common group of jobs, defining how they should be scheduled.
     */
    class Group
    {
        public static final String THREAD_ID = "thread-id";
        public static final Map<String, String> NO_METADATA = Collections.emptyMap();

        private final String name;
        private final SchedulingStrategy strategy;
        private final AtomicInteger threadCounter = new AtomicInteger( 0 );

        public Group( String name, SchedulingStrategy strategy )
        {
            this.name = name;
            this.strategy = strategy;
        }

        public String name()
        {
            return name;
        }

        public SchedulingStrategy strategy()
        {
            return strategy;
        }

        /**
         * Name a new thread. This method may or may not be used, it is up to the scheduling strategy to decide
         * to honor this.
         * @param metadata comes from {@link #schedule(Group, Runnable, Map)}
         */
        public String threadName( Map<String, String> metadata )
        {
            if ( metadata.containsKey( THREAD_ID ) )
            {
                return "neo4j." + name() + "-" + metadata.get( THREAD_ID );
            }
            return "neo4j." + name() + "-" + threadCounter.incrementAndGet();
        }
    }

    /**
     * This is an exhaustive list of job types that run in the database. It should be expanded as needed for new groups
     * of jobs.
     *
     * For now, this does naming only, but it will allow us to define per-group configuration, such as how to handle
     * failures, shared threads and (later on) affinity strategies.
     */
    class Groups
    {
        /** Session workers, these perform the work of actually executing client queries.  */
        public static final Group sessionWorker = new Group( "Session", NEW_THREAD );

        /** Background index population */
        public static final Group indexPopulation = new Group( "IndexPopulation", POOLED );

        /** Push transactions from master to slaves */
        public static final Group masterTransactionPushing = new Group( "TransactionPushing", POOLED );

        /**
         * Rolls back idle transactions on the server.
         */
        public static final Group serverTransactionTimeout = new Group( "ServerTransactionTimeout", POOLED );

        /**
         * Aborts idle slave lock sessions on the master.
         */
        public static final Group slaveLocksTimeout = new Group( "SlaveLocksTimeout", POOLED );

        /**
         * Pulls updates from the master.
         */
        public static final Group pullUpdates = new Group( "PullUpdates", POOLED );

        /**
         * Gathers approximated data about the underlying data store.
         */
        public static final Group indexSamplingController = new Group( "IndexSamplingController", POOLED );
        public static final Group indexSampling = new Group( "IndexSampling", POOLED );

        /**
         * Rotates internal diagnostic logs
         */
        public static final Group internalLogRotation = new Group( "InternalLogRotation", POOLED );

        /**
         * Rotates query logs
         */
        public static final Group queryLogRotation = new Group( "queryLogRotation", POOLED );

        /**
         * Checkpoint and store flush
         */
        public static final Group checkPoint = new Group( "CheckPoint", POOLED );

        /**
         * Network IO threads for the Bolt protocol.
         */
        public static final Group boltNetworkIO = new Group( "BoltNetworkIO", NEW_THREAD );

        /**
         * Storage maintenance.
         */
        public static Group storageMaintenance = new Group( "StorageMaintenance", POOLED );
    }

    interface JobHandle
    {
        void cancel( boolean mayInterruptIfRunning );
    }

    /** Expose a group scheduler as an {@link Executor} */
    Executor executor( Group group );

    /**
     * Expose a group scheduler as a {@link java.util.concurrent.ThreadFactory}.
     * This is a lower-level alternative than {@link #executor(Group)}, where you are in control of when to spin
     * up new threads for your jobs.
     *
     * The lifecycle of the threads you get out of here are not managed by the JobScheduler, you own the lifecycle and
     * must start the thread before it can be used.
     *
     * This mechanism is strongly preferred over manually creating threads, as it allows a central place for record
     * keeping of thread creation, central place for customizing the threads based on their groups, and lays a
     * foundation for controlling things like thread affinity and priorities in a coordinated manner in the future.
     */
    ThreadFactory threadFactory( Group group );

    /** Schedule a new job in the specified group. */
    JobHandle schedule( Group group, Runnable job );

    /** Schedule a new job in the specified group, passing in metadata for the scheduling strategy to use. */
    JobHandle schedule( Group group, Runnable job, Map<String, String> metadata );

    /** Schedule a new job in the specified group with the given delay */
    JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit );

    /** Schedule a recurring job */
    JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit );

    /** Schedule a recurring job where the first invocation is delayed the specified time */
    JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit );
}
