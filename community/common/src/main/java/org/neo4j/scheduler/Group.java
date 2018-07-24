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

import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Represents a common group of jobs, defining how they should be scheduled.
 */
public enum Group
{
    // GENERAL DATABASE GROUPS.
    /** Thread that schedules delayed or recurring tasks. */
    TASK_SCHEDULER( "Scheduler" ),
    /** Watch out for, and report, external manipulation of store files. */
    FILE_WATCHER( "FileWatcher" ),
    /** Monitor and report system-wide pauses, in case they lead to service interruption. */
    VM_PAUSE_MONITOR( "VmPauseMonitor" ),
    /** Rotates diagnostic text logs. */
    TEXT_LOG_ROTATION( "TextLogRotation" ),
    /** Checkpoint and store flush. */
    CHECKPOINT( "CheckPoint" ),
    /** Various little periodic tasks that need to be done on a regular basis to keep the store in good shape. */
    STORAGE_MAINTENANCE( "StorageMaintenance" ),
    /** Terminates kernel transactions that have timed out. */
    TRANSACTION_TIMEOUT_MONITOR( "TransactionTimeoutMonitor" ),
    /** Background index population. */
    INDEX_POPULATION( "IndexPopulation" ),
    /** Background index population */
    INDEX_SAMPLING( "IndexSampling" ),
    /** Thread pool for anyone who want some help doing file IO in parallel. */
    FILE_IO_HELPER( "FileIOHelper" ),
    NATIVE_SECURITY( "NativeSecurity" ),
    METRICS_EVENT( "MetricsEvent" ),

    // CYPHER.
    /** Thread pool for parallel Cypher query execution. */
    CYPHER_WORKER( "CypherWorker" ),

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
    TOPOLOGY_HEALTH( "HazelcastHealth" ),
    TOPOLOGY_KEEP_ALIVE( "KeepAlive" ),
    TOPOLOGY_REFRESH( "TopologyRefresh" ),
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
    UDC( "UsageDataCollector" )
    ;

    private final AtomicInteger threadCounter = new AtomicInteger();
    private final String name;

    Group( String name )
    {
        Objects.requireNonNull( name, "Group name cannot be null." );
        this.name = name;
    }

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
}
