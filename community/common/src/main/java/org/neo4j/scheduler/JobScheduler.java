/*
 * Copyright (c) 2002-2018 "Neo4j,"
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

import java.util.concurrent.Executor;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;

import org.neo4j.kernel.lifecycle.Lifecycle;

/**
 * To be expanded, the idea here is to have a database-global service for running jobs, handling jobs crashing and so
 * on.
 */
public interface JobScheduler extends Lifecycle
{
    /**
     * This is an exhaustive list of job types that run in the database. It should be expanded as needed for new groups
     * of jobs.
     * <p>
     * For now, this does minimal configuration, but opens up for things like common
     * failure handling, shared threads and affinity strategies.
     */
    class Groups
    {
        /**
         * This group is used by the JobScheduler implementation itself, for the thread or threads that are in charge of the timely execution of delayed or
         * recurring tasks.
         */
        public static final Group taskScheduler = new Group( "Scheduler" );

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
        public static final Group indexSampling = new Group( "IndexSampling" );
        public static final Group indexSamplingController = indexSampling;

        /**
         * Rotates internal diagnostic logs
         */
        public static final Group internalLogRotation = new Group( "InternalLogRotation" );

        /**
         * Rotates query logs
         */
        public static final Group queryLogRotation = internalLogRotation;

        /**
         * Rotates bolt message logs
         */
        public static final Group boltLogRotation = internalLogRotation;

        /**
         * Rotates metrics csv files
         */
        public static final Group metricsLogRotations = internalLogRotation;

        /**
         * Checkpoint and store flush
         */
        public static final Group checkPoint = new Group( "CheckPoint" );

        /**
         * Raft Log pruning
         */
        public static final Group raftLogPruning = new Group( "RaftLogPruning" );

        /**
         * Raft timers.
         */
        public static final Group raft = new Group( "RaftTimer" );
        public static final Group raftBatchHandler = new Group( "RaftBatchHandler" );
        public static final Group raftReaderPoolPruner = new Group( "RaftReaderPoolPruner" );
        public static final Group topologyHealth = new Group( "HazelcastHealth" );
        public static final Group topologyKeepAlive = new Group( "KeepAlive" );
        public static final Group topologyRefresh = new Group( "TopologyRefresh" );
        public static final Group membershipWaiter = new Group( "MembershipWaiter" );

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
        public static final Group downloadSnapshot = new Group( "DownloadSnapshot" );

        /**
         * UDC timed events.
         */
        public static final Group udc = new Group( "UsageDataCollection" );

        /**
         * Storage maintenance.
         */
        public static final Group storageMaintenance = new Group( "StorageMaintenance" );

        /**
         * Native security.
         */
        public static final Group nativeSecurity = new Group( "NativeSecurity" );

        /**
         * File watch service group
         */
        public static final Group fileWatch = new Group( "FileWatcher" );

        /**
         * Recovery cleanup.
         */
        public static final Group recoveryCleanup = new Group( "RecoveryCleanup" );

        /**
         * Kernel transaction timeout monitor.
         */
        public static final Group transactionTimeoutMonitor = new Group( "TransactionTimeoutMonitor" );

        /**
         * Kernel transaction timeout monitor.
         */
        public static final Group cypherWorker = new Group( "CypherWorker" );

        /**
         * VM pause monitor
         */
        public static final Group vmPauseMonitor = new Group( "VmPauseMonitor" );

        /**
         * IO helper threads for page cache and IO related stuff.
         */
        public static final Group pageCacheIOHelper = new Group( "PageCacheIOHelper" );

        /**
         * Bolt scheduler worker
         */
        public static final Group boltWorker = new Group( "BoltWorker" );

        private Groups()
        {
        }
    }

    /**
     * Assign a specific name to the top-most scheduler group.
     * <p>
     * This is just a suggestion for debugging purpose. The specific scheduler implementation is free to ignore calls
     * to this method.
     */
    void setTopLevelGroupName( String name );

    /** Expose a group scheduler as an {@link Executor} */
    Executor executor( Group group );

    /**
     * Creates an {@link ExecutorService} that does works-stealing - read more about this in {@link ForkJoinPool}
     */
    ExecutorService workStealingExecutor( Group group, int parallelism );

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

    /** Schedule a new job in the specified group. */
    JobHandle schedule( Group group, Runnable job );

    /** Schedule a new job in the specified group with the given delay */
    JobHandle schedule( Group group, Runnable runnable, long initialDelay, TimeUnit timeUnit );

    /** Schedule a recurring job */
    JobHandle scheduleRecurring( Group group, Runnable runnable, long period, TimeUnit timeUnit );

    /** Schedule a recurring job where the first invocation is delayed the specified time */
    JobHandle scheduleRecurring( Group group, Runnable runnable, long initialDelay, long period, TimeUnit timeUnit );
}
