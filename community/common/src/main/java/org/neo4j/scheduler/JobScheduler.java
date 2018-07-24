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
        public static final Group taskScheduler = Group.TASK_SCHEDULER;

        /** Background index population */
        public static final Group indexPopulation = Group.INDEX_POPULATION;

        /** Push transactions from master to slaves */
        public static final Group masterTransactionPushing = Group.MASTER_TRANSACTION_PUSHING;

        /**
         * Rolls back idle transactions on the server.
         */
        public static final Group serverTransactionTimeout = Group.SERVER_TRANSACTION_TIMEOUT;

        /**
         * Aborts idle slave lock sessions on the master.
         */
        public static final Group slaveLocksTimeout = Group.SLAVE_LOCKS_TIMEOUT;

        /**
         * Pulls updates from the master.
         */
        public static final Group pullUpdates = Group.PULL_UPDATES;

        /**
         * Gathers approximated data about the underlying data store.
         */
        public static final Group indexSampling = Group.INDEX_SAMPLING;
        public static final Group indexSamplingController = indexSampling;

        /**
         * Rotates internal diagnostic logs
         */
        public static final Group internalLogRotation = Group.TEXT_LOG_ROTATION;

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
        public static final Group checkPoint = Group.CHECKPOINT;

        /**
         * Raft Log pruning
         */
        public static final Group raftLogPruning = Group.RAFT_LOG_PRUNING;

        /**
         * Raft timers.
         */
        public static final Group raft = Group.RAFT_TIMER;
        public static final Group raftBatchHandler = Group.RAFT_BATCH_HANDLER;
        public static final Group raftReaderPoolPruner = Group.RAFT_READER_POOL_PRUNER;
        public static final Group topologyHealth = Group.TOPOLOGY_HEALTH;
        public static final Group topologyKeepAlive = Group.TOPOLOGY_KEEP_ALIVE;
        public static final Group topologyRefresh = Group.TOPOLOGY_REFRESH;
        public static final Group membershipWaiter = Group.MEMBERSHIP_WAITER;

        /**
         * Network IO threads for the Bolt protocol.
         */
        public static final Group boltNetworkIO = Group.BOLT_NETWORK_IO;

        /**
         * Reporting thread for Metrics events
         */
        public static final Group metricsEvent = Group.METRICS_EVENT;

        /**
         * Snapshot downloader
         */
        public static final Group downloadSnapshot = Group.DOWNLOAD_SNAPSHOT;

        /**
         * UDC timed events.
         */
        public static final Group udc = Group.UDC;

        /**
         * Storage maintenance.
         */
        public static final Group storageMaintenance = Group.STORAGE_MAINTENANCE;

        /**
         * Native security.
         */
        public static final Group nativeSecurity = Group.NATIVE_SECURITY;

        /**
         * File watch service group
         */
        public static final Group fileWatch = Group.FILE_WATCHER;

        /**
         * Recovery cleanup.
         */
        public static final Group recoveryCleanup = storageMaintenance;

        /**
         * Kernel transaction timeout monitor.
         */
        public static final Group transactionTimeoutMonitor = Group.TRANSACTION_TIMEOUT_MONITOR;

        /**
         * Kernel transaction timeout monitor.
         */
        public static final Group cypherWorker = Group.CYPHER_WORKER;

        /**
         * VM pause monitor
         */
        public static final Group vmPauseMonitor = Group.VM_PAUSE_MONITOR;

        /**
         * IO helper threads for page cache and IO related stuff.
         */
        public static final Group pageCacheIOHelper = Group.FILE_IO_HELPER;

        /**
         * Bolt scheduler worker
         */
        public static final Group boltWorker = Group.BOLT_WORKER;

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
