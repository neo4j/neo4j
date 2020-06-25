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
    /* Background page cache worker. */
    PAGE_CACHE( "PageCacheWorker" ),
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
    /** Terminates kernel transactions that have timed out. */
    TRANSACTION_TIMEOUT_MONITOR( "TransactionTimeoutMonitor" ),
    /** Background index population. */
    INDEX_POPULATION( "IndexPopulation" ),
    /** Background index sampling */
    INDEX_SAMPLING( "IndexSampling" ),
    /** Background index update applier, for eventually consistent indexes. */
    INDEX_UPDATING( "IndexUpdating", ExecutorServiceFactory.singleThread() ), // Single-threaded to serialise updates with opening/closing/flushing of indexes.
    /** Thread pool for anyone who want some help doing file IO in parallel. */
    FILE_IO_HELPER( "FileIOHelper" ),
    NATIVE_SECURITY( "NativeSecurity" ),
    METRICS_EVENT( "MetricsEvent" ),

    // CYPHER.
    /** Thread pool for parallel Cypher query execution. */
    CYPHER_WORKER( "CypherWorker", ExecutorServiceFactory.workStealing() ),

    // DATA COLLECTOR
    DATA_COLLECTOR( "DataCollector" ),

    // BOLT.
    /** Network IO threads for the Bolt protocol. */
    BOLT_NETWORK_IO( "BoltNetworkIO" ),
    /** Transaction processing threads for Bolt. */
    BOLT_WORKER( "BoltWorker" ),

    // CAUSAL CLUSTER, TOPOLOGY & BACKUP.
    RAFT_TIMER( "RaftTimer" ),
    RAFT_LOG_PRUNING( "RaftLogPruning" ),
    RAFT_BATCH_HANDLER( "RaftBatchHandler" ),
    RAFT_READER_POOL_PRUNER( "RaftReaderPoolPruner" ),
    HZ_TOPOLOGY_HEALTH( "HazelcastHealth" ),
    HZ_TOPOLOGY_KEEP_ALIVE( "KeepAlive" ),
    HZ_TOPOLOGY_REFRESH( "TopologyRefresh" ),
    AKKA_TOPOLOGY_WORKER( "AkkaTopologyWorkers", ExecutorServiceFactory.workStealing() ),
    MEMBERSHIP_WAITER( "MembershipWaiter" ),
    DOWNLOAD_SNAPSHOT( "DownloadSnapshot" ),

    // HA.
    /** Push transactions from master to slaves */
    MASTER_TRANSACTION_PUSHING( "TransactionPushing" ),
    /** Rolls back idle transactions on the server. */
    SERVER_TRANSACTION_TIMEOUT( "ServerTransactionTimeout" ),
    /** Aborts idle slave lock sessions on the master. */
    SLAVE_LOCKS_TIMEOUT( "SlaveLocksTimeout" ),
    /** Pulls updates from the master. */
    PULL_UPDATES( "PullUpdates" ),

    // MISC.
    /** UDC timed events. */
    UDC( "UsageDataCollector" ),

    //TESTING
    TESTING( "TestingGroup", ExecutorServiceFactory.callingThread() );

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
        return "neo4j." + groupName() + "-" + threadCounter.incrementAndGet();
    }

    public ExecutorService buildExecutorService( SchedulerThreadFactory factory )
    {
        return executorServiceFactory.build( this, factory );
    }
}
