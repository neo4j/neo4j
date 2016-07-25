/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.metrics.source.cluster;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.com.storecopy.ToNetworkStoreWriter;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.source.ByteCountsMetric;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Network Metrics" )
public class NetworkMetrics extends LifecycleAdapter
{
    private static final String NAME_PREFIX = "neo4j.network";

    @Documented( "The amount of bytes transmitted on the network containing the transaction data from a slave " +
                 "to the master in order to be committed" )
    public static final String SLAVE_NETWORK_TX_WRITES = name( NAME_PREFIX, "slave_network_tx_writes" );
    @Documented( "The amount of bytes transmitted on the network while copying stores from a machines to another" )
    public static final String MASTER_NETWORK_STORE_WRITES = name( NAME_PREFIX, "master_network_store_writes" );
    @Documented( "The amount of bytes transmitted on the network containing the transaction data from a master " +
                 "to the slaves in order to propagate committed transactions" )
    public static final String MASTER_NETWORK_TX_WRITES = name( NAME_PREFIX, "master_network_tx_writes" );

    private final MetricRegistry registry;
    private final Monitors monitors;
    private final ByteCountsMetric masterNetworkTransactionWrites = new ByteCountsMetric();
    private final ByteCountsMetric masterNetworkStoreWrites = new ByteCountsMetric();
    private final ByteCountsMetric slaveNetworkTransactionWrites = new ByteCountsMetric();

    public NetworkMetrics( MetricRegistry registry, Monitors monitors )
    {
        this.registry = registry;
        this.monitors = monitors;
    }

    @Override
    public void start()
    {
        monitors.addMonitorListener( masterNetworkTransactionWrites, MasterServer.class.getName() );
        monitors.addMonitorListener( masterNetworkStoreWrites, ToNetworkStoreWriter.class.getName(),
                ToNetworkStoreWriter.STORE_COPIER_MONITOR_TAG );
        monitors.addMonitorListener( slaveNetworkTransactionWrites, MasterClient210.class.getName() );

        registry.register( MASTER_NETWORK_TX_WRITES, (Gauge<Long>) masterNetworkTransactionWrites::getBytesWritten );
        registry.register( MASTER_NETWORK_STORE_WRITES, (Gauge<Long>) masterNetworkStoreWrites::getBytesWritten );
        registry.register( SLAVE_NETWORK_TX_WRITES, (Gauge<Long>) slaveNetworkTransactionWrites::getBytesWritten );
    }

    @Override
    public void stop()
    {
        registry.remove( MASTER_NETWORK_TX_WRITES );
        registry.remove( MASTER_NETWORK_STORE_WRITES );
        registry.remove( SLAVE_NETWORK_TX_WRITES );

        monitors.removeMonitorListener( masterNetworkTransactionWrites );
        monitors.removeMonitorListener( masterNetworkStoreWrites );
        monitors.removeMonitorListener( slaveNetworkTransactionWrites );
    }
}
