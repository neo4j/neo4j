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

import java.io.IOException;
import java.util.function.Supplier;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.causalclustering.core.consensus.CoreMetaData;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;

import static com.codahale.metrics.MetricRegistry.name;

@Documented(".CatchUp Metrics")
public class CatchUpMetrics extends LifecycleAdapter
{
    private static final String CAUSAL_CLUSTERING_PREFIX = "neo4j.causal_clustering.catchup";

    @Documented("TX pull requests received from read replicas")
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
    public void start() throws Throwable
    {
        monitors.addMonitorListener( txPullRequestsMetric );
        registry.register( TX_PULL_REQUESTS_RECEIVED, (Gauge<Long>) txPullRequestsMetric::txPullRequestsReceived );
    }

    @Override
    public void stop() throws IOException
    {
        registry.remove( TX_PULL_REQUESTS_RECEIVED );
        monitors.removeMonitorListener( txPullRequestsMetric );
    }
}
