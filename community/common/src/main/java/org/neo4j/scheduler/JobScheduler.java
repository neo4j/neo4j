/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.scheduler;

import java.util.Collections;
import java.util.Map;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * To be expanded, the idea here is to have a database-global service for running jobs, handling jobs crashing and so on.
 */
public interface JobScheduler extends Lifecycle
{
    /**
     * Represents a common group of jobs, defining how they should be scheduled.
     */
    class Group
    {
        public static final String THREAD_ID = "thread-id";
        public static final Map<String, String> NO_METADATA = Collections.emptyMap();

        private final AtomicInteger threadCounter = new AtomicInteger();
        private final String name;

        public Group( String name )
        {
            this.name = name;
        }

        public String name()
        {
            return name;
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
     * For now, this does minimal configuration, but opens up for things like common
     * failure handling, shared threads and affinity strategies.
     */
    class Groups
    {
        /** Session workers, these perform the work of actually executing client queries.  */
        public static final Group sessionWorker = new Group( "Session" );

        /** Background index population */
        public static final Group indexPopulation = new Group( "IndexPopulation" );

        /** Push transactions from master to slaves */
        public static final Group masterTransactionPushing = new Group( "TransactionPushing" );

        /**
         * Rolls back idle transactions on the server.
         */
        public static final Group serverTransactionTimeout = new Group( "ServerTransactionTimeout" );

        /**
         * Aborts idle slave lock sessions on the master.
         */
        public static final Group slaveLocksTimeout = new Group( "SlaveLocksTimeout" );

        /**
         * Pulls updates from the master.
         */
        public static final Group pullUpdates = new Group( "PullUpdates" );

        /**
         * Gathers approximated data about the underlying data store.
         */
        public static final Group indexSamplingController = new Group( "IndexSamplingController" );
        public static final Group indexSampling = new Group( "IndexSampling" );

        /**
         * Rotates internal diagnostic logs
         */
        public static final Group internalLogRotation = new Group( "InternalLogRotation" );

        /**
         * Rotates query logs
         */
        public static final Group queryLogRotation = new Group( "queryLogRotation" );

        /**
         * Rotates bolt message logs
         */
        public static final Group boltLogRotation = new Group( "BoltLogRotation" );

        /**
         * Rotates metrics csv files
         */
        public static final Group metricsLogRotations = new Group( "MetricsLogRotations" );

        /**
         * Checkpoint and store flush
         */
        public static final Group checkPoint = new Group( "CheckPoint" );

        /**
         * Raft Log pruning
         */
        public static final Group raftLogPruning = new Group( "RaftLogPruning" );

        /**
         * Network IO threads for the Bolt protocol.
         */
        public static final Group boltNetworkIO = new Group( "BoltNetworkIO" );

        /**
         * Reporting thread for Metrics events
         */
        public static final Group metricsEvent = new Group( "MetricsEvent" );

        /**
         * Snapshot downloader
         */
        public static final Group downloadSnapshot = new JobScheduler.Group( "DownloadSnapshot" );

        /**
         * UDC timed events.
         */
        public static Group udc  = new Group( "UsageDataCollection" );

        /**
         * Storage maintenance.
         */
        public static Group storageMaintenance = new Group( "StorageMaintenance" );

        /**
         * Raft timers.
         */
        public static Group raft = new Group( "RaftTimer" );

        /**
         * Native security.
         */
        public static Group nativeSecurity = new Group( "NativeSecurity" );

        /**
         * File watch service group
         */
        public static Group fileWatch = new Group( "FileWatcher" );

        /**
         * Recovery cleanup.
         */
        public static Group recoveryCleanup = new Group( "RecoveryCleanup" );

        /**
         * Kernel transaction timeout monitor.
         */
        public static Group transactionTimeoutMonitor = new Group( "TransactionTimeoutMonitor" );

        /**
         * Kernel transaction timeout monitor.
         */
        public static Group cypherWorker = new Group( "CypherWorker" );

        private Groups()
        {
        }
    }

    interface JobHandle
    {
        void cancel( boolean mayInterruptIfRunning );

        void waitTermination() throws InterruptedException, ExecutionException, CancellationException;

        default void registerCancelListener( CancelListener listener )
        {
            throw new UnsupportedOperationException( "Unsupported in this implementation" );
        }
    }

    /**
     * Gets notified about calls to {@link JobHandle#cancel(boolean)}.
     */
    interface CancelListener
    {
        /**
         * Notification that {@link JobHandle#cancel(boolean)} was called.
         *
         * @param mayInterruptIfRunning argument from {@link JobHandle#cancel(boolean)} call.
         */
        void cancelled( boolean mayInterruptIfRunning );
    }

    /** Expose a group scheduler as an {@link Executor} */
    Executor executor( Group group );

    /** Creates an {@link ExecutorService} that does works-stealing - read more about this in {@link ForkJoinPool}*/
    ExecutorService workStealingExecutor( Group group, int parallelism );

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
