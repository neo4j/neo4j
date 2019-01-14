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

@Documented( ".CatchUp Metrics" )
public class CatchUpMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.catchup";

    @Documented( "TX pull requests received from read replicas" )
    public static final String TX_PULL_REQUESTS_RECEIVED = name( CAUSAL_CLUSTERING_PREFIX, "tx_pull_requests_received" );

    private Monitors monitors;
    private MetricRegistry registry;
    private final TxPullRequestsMetric txPullRequestsMetric = new TxPullRequestsMetric();

    public CatchUpMetrics( Monitors monitors, MetricRegistry registry )
    {
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( txPullRequestsMetric );
        registry.register( TX_PULL_REQUESTS_RECEIVED, (Gauge<Long>) txPullRequestsMetric::txPullRequestsReceived );
    }

    @Override
    public void stop()
    {
        registry.remove( TX_PULL_REQUESTS_RECEIVED );
        monitors.removeMonitorListener( txPullRequestsMetric );
    }
}
