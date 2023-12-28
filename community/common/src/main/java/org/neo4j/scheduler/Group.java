/*
 * Copyright (c) "Neo4j"
 * Neo4j Sweden AB [https://neo4j.com]
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
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */
package org.neo4j.scheduler;

import java.util.OptionalInt;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a common group of jobs, defining how they should be scheduled.
 */
public enum Group {
    // GENERAL DATABASE GROUPS.
    /** Thread that schedules delayed or recurring tasks. */
    TASK_SCHEDULER("Scheduler", ExecutorServiceFactory.unschedulable()),
    /* Page cache background eviction. */
    PAGE_CACHE_EVICTION("PageCacheEviction"),
    /* Page cache background eviction. */
    PAGE_CACHE_PRE_FETCHER("PageCachePreFetcher", ExecutorServiceFactory.cachedWithDiscard(), 4),
    /** Watch out for, and report, external manipulation of store files. */
    FILE_WATCHER("FileWatcher", ExecutorServiceFactory.unschedulable()),
    /** Monitor and report system-wide pauses, in case they lead to service interruption. */
    VM_PAUSE_MONITOR("VmPauseMonitor"),
    /** Rotates diagnostic text logs. */
    LOG_ROTATION("LogRotation"),
    /** Checkpoint and store flush. */
    CHECKPOINT("CheckPoint"),
    /** Various little periodic tasks that need to be done on a regular basis to keep the store in good shape. */
    STORAGE_MAINTENANCE("StorageMaintenance"),
    /** Index recovery cleanup. */
    INDEX_CLEANUP("IndexCleanup"),
    /** Index recovery cleanup work. */
    INDEX_CLEANUP_WORK("IndexCleanupWork"),
    /** Terminates kernel transactions that have timed out. */
    TRANSACTION_TIMEOUT_MONITOR("TransactionTimeoutMonitor"),
    /** Background index population. */
    INDEX_POPULATION("IndexPopulationMain"),
    /**
     * Background index population work.
     * Threads in this group are used both for reading from store and generating index update for index population
     * as well as other tasks for completing an index after the store scan.
     * As it stands this group should not have a limit on its own because of how tasks are scheduled during population
     * and is instead effectively limited by number of ongoing index populations times number of workers per index population,
     * i.e. settings internal.dbms.index_population.parallelism * internal.dbms.index_population.workers
     */
    INDEX_POPULATION_WORK("IndexPopulationWork", ExecutorServiceFactory.cached()),
    /** Background index sampling */
    INDEX_SAMPLING("IndexSampling"),
    /** Background index update applier, for eventually consistent indexes. */
    INDEX_UPDATING(
            "IndexUpdating",
            ExecutorServiceFactory
                    .singleThread()), // Single-threaded to serialise updates with opening/closing/flushing of indexes.
    /** Thread pool for anyone who want some help doing file IO in parallel. */
    FILE_IO_HELPER("FileIOHelper"),
    LOG_WRITER("LOG_WRITER"),
    NATIVE_SECURITY("NativeSecurity"),
    METRICS_CSV_WRITE("MetricsCsvWrite"),
    METRICS_GRAPHITE_WRITE("MetricsGraphiteWrite"),
    /** Threads that perform database manager operations necessary to bring databases to their desired states. */
    DATABASE_RECONCILER("DatabaseReconciler"),
    /** Ensures DatabaseId lookup is not run from an outer transaction that will be tied to a database */
    DATABASE_ID_REPOSITORY("DatabaseIdRepository"),

    BUFFER_POOL_MAINTENANCE("BufferPoolMaintenance"),

    // CYPHER.
    /** Thread pool for parallel Cypher query execution. */
    CYPHER_WORKER("CypherWorker", ExecutorServiceFactory.workStealing()),
    CYPHER_CACHE("CypherCache", ExecutorServiceFactory.workStealing()),

    /** Removes queries that have timed out */
    CYPHER_QUERY_MONITOR("CypherQueryMonitor"),

    // CDC
    CDC("CDC"),

    // DATA COLLECTOR
    DATA_COLLECTOR("DataCollector"),

    // BOLT.
    /** Network IO threads for the Bolt protocol. */
    BOLT_NETWORK_IO("BoltNetworkIO", ExecutorServiceFactory.unschedulable()),
    /** Transaction processing threads for Bolt. */
    BOLT_WORKER("BoltWorker", ExecutorServiceFactory.unschedulable()),

    // CAUSAL CLUSTER, TOPOLOGY & BACKUP.
    RAFT_CLIENT("RaftClient"),
    RAFT_SERVER("RaftServer"),
    RAFT_LOG_PRUNING("RaftLogPruning"),
    RAFT_HANDLER("RaftBatchHandler"),
    RAFT_READER_POOL_PRUNER("RaftReaderPoolPruner"),
    RAFT_LOG_PREFETCH("RaftLogPrefetch"),
    RAFT_DRAINING_SERVICE("RaftDrainingService"),
    LEADER_TRANSFER_SERVICE("LeaderTransferService"),
    CORE_STATE_APPLIER("CoreStateApplier"),
    AKKA_HELPER("AkkaActorSystemRestarter"),
    LIGHTHOUSE("Lighthouse"),
    LIGHTHOUSE_RECEIVER("LighthouseReceiver", ExecutorServiceFactory.singleThread()),
    LIGHTHOUSE_JOIN_LEAVE("LighthouseJoinLeave", ExecutorServiceFactory.singleThread()),
    LIGHTHOUSE_MEMBER_STATE_TRANSITION_SCHEDULER("LighthouseMemberStateScheduler", ExecutorServiceFactory.singleThread()),
    DOWNLOAD_SNAPSHOT("DownloadSnapshot"),
    CATCHUP_CHANNEL_POOL("CatchupChannelPool"),
    CATCHUP_CLIENT("CatchupClient"),
    CATCHUP_PROCESS("CatchupProcess"),
    CATCHUP_SERVER("CatchupServer"),
    DATABASE_INFO_SERVICE("DatabaseInfoService"),
    STORE_COPY_CLIENT("StoreCopyClient"),
    THROUGHPUT_MONITOR("ThroughputMonitor"),
    PANIC_SERVICE("PanicService"),
    CLUSTER_STATUS_CHECK_SERVICE("ClusterStatusService"),
    TOPOLOGY_LOGGER("TopologyLogger"),
    TOPOLOGY_MAINTENANCE("TopologyMaintenance"),
    TOPOLOGY_GRAPH_WRITE_SUPPORT("TopologyGraphWriteSupport"),
    CONNECTIVITY_CHECKS("ConnectivityChecks"),
    RAFTED_STATUS_CHECKS("RaftedStatusChecks"),
    COMMIT_COORDINATOR("CommitCoordinator"),

    /** Rolls back idle transactions on the server. */
    SERVER_TRANSACTION_TIMEOUT("ServerTransactionTimeout"),
    PULL_UPDATES("PullUpdates"),
    APPLY_UPDATES("ApplyUpdates"),

    // FABRIC
    FABRIC_IDLE_DRIVER_MONITOR("FabricIdleDriverMonitor"),
    FABRIC_WORKER("FabricWorker"),

    // SECURITY
    AUTH_CACHE("AuthCache", ExecutorServiceFactory.workStealing()),
    SECURITY_MAINTAINENCE("SecurityMaintainence"),

    // GDS
    GDS_CLUSTER_WRITE("GdsClusterWrite"),

    // ARROW
    ARROW_WRITE("ArrowWrite"),

    // TESTING
    TESTING("TestingGroup", ExecutorServiceFactory.callingThread());

    private final String name;
    private final ExecutorServiceFactory executorServiceFactory;
    private final Integer defaultParallelism;
    private final AtomicInteger threadCounter;

    Group(String name, ExecutorServiceFactory executorServiceFactory, Integer defaultParallelism) {
        this.name = name;
        this.executorServiceFactory = executorServiceFactory;
        this.defaultParallelism = defaultParallelism;
        this.threadCounter = new AtomicInteger();
    }

    Group(String name, ExecutorServiceFactory executorServiceFactory) {
        this(name, executorServiceFactory, null);
    }

    Group(String name) {
        this(name, ExecutorServiceFactory.cached());
    }

    /**
     * The slightly more human-readable name of the group. Useful for naming {@link ThreadGroup thread groups}, and also used as a component in the
     * {@link #threadName() thread names}.
     */
    public String groupName() {
        return name;
    }

    /**
     * Name a new thread. This method may or may not be used, it is up to the scheduling strategy to decide
     * to honor this.
     */
    public String threadName() {
        return "neo4j." + groupName() + "-" + threadCounter.incrementAndGet();
    }

    public ExecutorService buildExecutorService(SchedulerThreadFactory factory, int parallelism) {
        return executorServiceFactory.build(this, factory, parallelism);
    }

    public OptionalInt defaultParallelism() {
        return defaultParallelism == null ? OptionalInt.empty() : OptionalInt.of(defaultParallelism);
    }
}
