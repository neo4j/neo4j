/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.metrics.source.cluster;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import org.neo4j.com.storecopy.ToNetworkStoreWriter;
import org.neo4j.kernel.ha.MasterClient320;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.source.ByteCountsMetric;

import static com.codahale.metrics.MetricRegistry.name;

@Documented( ".Network metrics" )
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
        monitors.addMonitorListener( slaveNetworkTransactionWrites, MasterClient320.class.getName() );

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
