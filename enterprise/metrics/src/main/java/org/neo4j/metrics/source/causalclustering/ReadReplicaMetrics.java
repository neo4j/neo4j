/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.metrics.source.causalclustering;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Read Replica Metrics" )
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
    public void start()
    {
        monitors.addMonitorListener( pullRequestMetric );

        registry.register( PULL_UPDATES, (Gauge<Long>) pullRequestMetric::numberOfRequests );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED, (Gauge<Long>) pullRequestMetric::lastRequestedTxId );
        registry.register( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED, (Gauge<Long>) pullRequestMetric::lastReceivedTxId );
    }

    @Override
    public void stop()
    {
        registry.remove( PULL_UPDATES );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_REQUESTED );
        registry.remove( PULL_UPDATE_HIGHEST_TX_ID_RECEIVED );

        monitors.removeMonitorListener( pullRequestMetric );
    }
}
