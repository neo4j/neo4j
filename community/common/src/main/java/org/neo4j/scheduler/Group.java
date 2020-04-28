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
package org.neo4j.scheduler;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a common group of jobs, defining how they should be scheduled.
 */
public enum Group
{
    // GENERAL DATABASE GROUPS.
    /** Thread that schedules delayed or recurring tasks. */
    TASK_SCHEDULER( "Scheduler", ExecutorServiceFactory.unschedulable() ),
    /* Page cache background eviction. */
    PAGE_CACHE_EVICTION( "PageCacheEviction" ),
    /* Page cache background eviction. */
    PAGE_CACHE_PRE_FETCHER( "PageCachePreFetcher", ExecutorServiceFactory.cachedWithDiscard() ),
    /** Watch out for, and report, external manipulation of store files. */
    FILE_WATCHER( "FileWatcher" ),
    /** Monitor and report system-wide pauses, in case they lead to service interruption. */
    VM_PAUSE_MONITOR( "VmPauseMonitor" ),
    /** Rotates diagnostic text logs. */
    LOG_ROTATION( "LogRotation" ),
    /** Checkpoint and store flush. */
    CHECKPOINT( "CheckPoint" ),
    /** Various little periodic tasks that need to be done on a regular basis to keep the store in good shape. */
    STORAGE_MAINTENANCE( "StorageMaintenance" ),
    /** Index recovery cleanup. */
    INDEX_CLEANUP( "IndexCleanup" ),
    /** Index recovery cleanup work. */
    INDEX_CLEANUP_WORK( "IndexCleanupWork" ),
    /** Terminates kernel transactions that have timed out. */
    TRANSACTION_TIMEOUT_MONITOR( "TransactionTimeoutMonitor" ),
    /** Background index population. */
    INDEX_POPULATION( "IndexPopulationMain" ),
    /** Background index population work. */
    INDEX_POPULATION_WORK( "IndexPopulationWork", ExecutorServiceFactory.fixedWithBackPressure() ),
    /** Background index sampling */
    INDEX_SAMPLING( "IndexSampling" ),
    /** Background index update applier, for eventually consistent indexes. */
    INDEX_UPDATING( "IndexUpdating", ExecutorServiceFactory.singleThread() ), // Single-threaded to serialise updates with opening/closing/flushing of indexes.
    /** Thread pool for anyone who want some help doing file IO in parallel. */
    FILE_IO_HELPER( "FileIOHelper" ),
    NATIVE_SECURITY( "NativeSecurity" ),
    METRICS_EVENT( "MetricsEvent" ),
    /** Threads that perform database manager operations necessary to bring databases to their desired states. */
    DATABASE_RECONCILER( "DatabaseReconciler" ),
    DATABASE_RECONCILER_UNBOUND( "DatabaseReconcilerUnbound" ),
    /** Ensures DatabaseId lookup is not run from an outer transaction that will be tied to a database */
    DATABASE_ID_REPOSITORY( "DatabaseIdRepository" ),

    // CYPHER.
    /** Thread pool for parallel Cypher query execution. */
    CYPHER_WORKER( "CypherWorker", ExecutorServiceFactory.workStealing() ),

    // DATA COLLECTOR
    DATA_COLLECTOR( "DataCollector" ),

    // BOLT.
    /** Network IO threads for the Bolt protocol. */
    BOLT_NETWORK_IO( "BoltNetworkIO", ExecutorServiceFactory.unschedulable() ),
    /** Transaction processing threads for Bolt. */
    BOLT_WORKER( "BoltWorker", ExecutorServiceFactory.unschedulable() ),

    // CAUSAL CLUSTER, TOPOLOGY & BACKUP.
    RAFT_CLIENT( "RaftClient" ),
    RAFT_SERVER( "RaftServer" ),
    RAFT_TIMER( "RaftTimer" ),
    RAFT_LOG_PRUNING( "RaftLogPruning" ),
    RAFT_BATCH_HANDLER( "RaftBatchHandler" ),
    RAFT_READER_POOL_PRUNER( "RaftReaderPoolPruner" ),
    CORE_STATE_APPLIER( "CoreStateApplier" ),
    AKKA_TOPOLOGY_WORKER( "AkkaTopologyWorkers", ExecutorServiceFactory.workStealingAsync() ),
    DOWNLOAD_SNAPSHOT( "DownloadSnapshot" ),
    CATCHUP_CLIENT( "CatchupClient" ),
    CATCHUP_SERVER( "CatchupServer" ),
    THROUGHPUT_MONITOR( "ThroughputMonitor" ),
    PANIC_SERVICE( "PanicService" ),

    /** Rolls back idle transactions on the server. */
    SERVER_TRANSACTION_TIMEOUT( "ServerTransactionTimeout" ),
    /** Pulls updates from the leader. */
    PULL_UPDATES( "PullUpdates" ),

    // FABRIC
    FABRIC_IDLE_DRIVER_MONITOR( "FabricIdleDriverMonitor" ),
    FABRIC_WORKER( "FabricWorker" );

    private final String name;
    private final ExecutorServiceFactory executorServiceFactory;
    private final AtomicInteger threadCounter;

    Group( String name, ExecutorServiceFactory executorServiceFactory )
    {
        this.name = name;
        this.executorServiceFactory = executorServiceFactory;
        threadCounter = new AtomicInteger();
    }

    Group( String name )
    {
        this( name, ExecutorServiceFactory.cached() );
    }

    /**
     * The slightly more human-readable name of the group. Useful for naming {@link ThreadGroup thread groups}, and also used as a component in the
     * {@link #threadName() thread names}.
     */
    public String groupName()
    {
        return name;
    }

    /**
     * Name a new thread. This method may or may not be used, it is up to the scheduling strategy to decide
     * to honor this.
     */
    public String threadName()
    {
        return "neo4j." + groupName() + '-' + threadCounter.incrementAndGet();
    }

    public ExecutorService buildExecutorService( SchedulerThreadFactory factory, int parallelism )
    {
        return executorServiceFactory.build( this, factory, parallelism );
    }
}
