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

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented(".Read Replica Metrics")
public class ReadReplicaMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.read_replica";

    @Documented( "The total number of pull requests made by this instance" )
    public static final String PULL_UPDATES = name( CAUSAL_CLUSTERING_PREFIX, "pull_updates" );
    @Documented( "The highest transaction id requested in a pull update by this instance" )
    public static final String PULL_UPDATE_HIGHEST_TX_ID_REQUESTED = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_requested" );
    @Documented( "The highest transaction id that has been pulled in the last pull updates by this instance" )
    public static final String PULL_UPDATE_HIGHEST_TX_ID_RECEIVED = name( CAUSAL_CLUSTERING_PREFIX,
            "pull_update_highest_tx_id_received" );

    private Monitors monitors;
    private MetricRegistry registry;

    private final PullRequestMetric pullRequestMetric = new PullRequestMetric();

    public ReadReplicaMetrics( Monitors monitors, MetricRegistry registry )
    {
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        monitors.addMonitorListener( pullRequestMetric );

        registry.register( PULL_UPDATES, (Gauge<Long>) pullRequestMetric::numberOfRequests );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED, (Gauge<Long>) pullRequestMetric::lastRequestedTxId );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED, (Gauge<Long>) pullRequestMetric::lastReceivedTxId );
    }

    @Override
    public void stop() throws IOException
    {
        registry.remove( PULL_UPDATES );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED );

        monitors.removeMonitorListener( pullRequestMetric );
    }
}
