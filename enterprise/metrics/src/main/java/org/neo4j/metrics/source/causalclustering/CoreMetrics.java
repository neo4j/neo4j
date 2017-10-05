/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU Affero General Public License as
 * published by the Free Software Foundation, either version 3 of the
 * License, or (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program. If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;
import java.util.function.Supplier;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented(".Core Metrics")
public class CoreMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.core";

    @Documented("Append index of the RAFT log")
    public static final String APPEND_INDEX = name( CAUSAL_CLUSTERING_PREFIX, "append_index" );
    @Documented("Commit index of the RAFT log")
    public static final String COMMIT_INDEX = name( CAUSAL_CLUSTERING_PREFIX, "commit_index" );
    @Documented("RAFT Term of this server")
    public static final String TERM = name( CAUSAL_CLUSTERING_PREFIX, "term" );
    @Documented("Leader was not found while attempting to commit a transaction")
    public static final String LEADER_NOT_FOUND = name( CAUSAL_CLUSTERING_PREFIX, "leader_not_found" );
    @Documented("Transaction retries")
    public static final String TX_RETRIES = name( CAUSAL_CLUSTERING_PREFIX, "tx_retries" );
    @Documented("Is this server the leader?")
    public static final String IS_LEADER = name( CAUSAL_CLUSTERING_PREFIX, "is_leader" );
    @Documented("How many RAFT messages were dropped?")
    public static final String DROPPED_MESSAGES = name( CAUSAL_CLUSTERING_PREFIX, "dropped_messages" );
    @Documented("How many RAFT messages are queued up?")
    public static final String QUEUE_SIZE = name( CAUSAL_CLUSTERING_PREFIX, "queue_sizes" );
    @Documented("In-flight cache total bytes")
    public static final String TOTAL_BYTES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "total_bytes" );
    @Documented("In-flight cache max bytes")
    public static final String MAX_BYTES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_bytes" );
    @Documented("In-flight cache element count")
    public static final String ELEMENT_COUNT = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "element_count" );
    @Documented("In-flight cache maximum elements")
    public static final String MAX_ELEMENTS = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "max_elements" );
    @Documented("In-flight cache hits")
    public static final String HITS = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "hits" );
    @Documented("In-flight cache misses")
    public static final String MISSES = name( CAUSAL_CLUSTERING_PREFIX, "in_flight_cache", "misses" );

    private Monitors monitors;
    private MetricRegistry registry;
    private Supplier<CoreMetaData> coreMetaData;

    private final RaftLogCommitIndexMetric raftLogCommitIndexMetric = new RaftLogCommitIndexMetric();
    private final RaftLogAppendIndexMetric raftLogAppendIndexMetric = new RaftLogAppendIndexMetric();
    private final RaftTermMetric raftTermMetric = new RaftTermMetric();
    private final LeaderNotFoundMetric leaderNotFoundMetric = new LeaderNotFoundMetric();
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();
    private final TxRetryMetric txRetryMetric = new TxRetryMetric();
    private final MessageQueueMonitorMetric messageQueueMetric = new MessageQueueMonitorMetric();
    private final InFlightCacheMetric inFlightCacheMetric = new InFlightCacheMetric();

    public CoreMetrics( Monitors monitors, MetricRegistry registry, Supplier<CoreMetaData> coreMetaData )
    {
        this.monitors = monitors;
        this.registry = registry;
        this.coreMetaData = coreMetaData;
    }

    @Override
    public void start() throws Throwable
    {
        monitors.addMonitorListener( raftLogCommitIndexMetric );
        monitors.addMonitorListener( raftLogAppendIndexMetric );
        monitors.addMonitorListener( raftTermMetric );
        monitors.addMonitorListener( leaderNotFoundMetric );
        monitors.addMonitorListener( txPullRequestsMetric );
        monitors.addMonitorListener( txRetryMetric );
        monitors.addMonitorListener( messageQueueMetric );
        monitors.addMonitorListener( inFlightCacheMetric );

        registry.register( COMMIT_INDEX, (Gauge<Long>) raftLogCommitIndexMetric::commitIndex );
        registry.register( APPEND_INDEX, (Gauge<Long>) raftLogAppendIndexMetric::appendIndex );
        registry.register( TERM, (Gauge<Long>) raftTermMetric::term );
        registry.register( LEADER_NOT_FOUND, (Gauge<Long>) leaderNotFoundMetric::leaderNotFoundExceptions );
        registry.register( TX_RETRIES, (Gauge<Long>) txRetryMetric::transactionsRetries );
        registry.register( IS_LEADER, new LeaderGauge() );
        registry.register( DROPPED_MESSAGES, (Gauge<Long>) messageQueueMetric::droppedMessages );
        registry.register( QUEUE_SIZE, (Gauge<Long>) messageQueueMetric::queueSizes );
        registry.register( TOTAL_BYTES, (Gauge<Long>) inFlightCacheMetric::getTotalBytes );
        registry.register( HITS, (Gauge<Long>) inFlightCacheMetric::getHits );
        registry.register( MISSES, (Gauge<Long>) inFlightCacheMetric::getMisses );
        registry.register( MAX_BYTES, (Gauge<Long>) inFlightCacheMetric::getMaxBytes );
        registry.register( MAX_ELEMENTS, (Gauge<Long>) inFlightCacheMetric::getMaxElements );
        registry.register( ELEMENT_COUNT, (Gauge<Long>) inFlightCacheMetric::getElementCount );
    }

    @Override
    public void stop() throws IOException
    {
        registry.remove( COMMIT_INDEX );
        registry.remove( APPEND_INDEX );
        registry.remove( TERM );
        registry.remove( LEADER_NOT_FOUND );
        registry.remove( TX_RETRIES );
        registry.remove( IS_LEADER );
        registry.remove( DROPPED_MESSAGES );
        registry.remove( QUEUE_SIZE );
        registry.remove( TOTAL_BYTES );
        registry.remove( HITS );
        registry.remove( MISSES );
        registry.remove( MAX_BYTES );
        registry.remove( MAX_ELEMENTS );
        registry.remove( ELEMENT_COUNT );

        monitors.removeMonitorListener( raftLogCommitIndexMetric );
        monitors.removeMonitorListener( raftLogAppendIndexMetric );
        monitors.removeMonitorListener( raftTermMetric );
        monitors.removeMonitorListener( leaderNotFoundMetric );
        monitors.removeMonitorListener( txPullRequestsMetric );
        monitors.removeMonitorListener( txRetryMetric );
        monitors.removeMonitorListener( messageQueueMetric );
        monitors.removeMonitorListener( inFlightCacheMetric );
    }

    private class LeaderGauge implements Gauge<Integer>
    {
        @Override
        public Integer getValue()
        {
            return coreMetaData.get().isLeader() ? 1 : 0;
        }
    }
}
