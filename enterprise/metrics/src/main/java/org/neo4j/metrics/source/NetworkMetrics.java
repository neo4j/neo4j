/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.metrics.source;

import com.codahale.metrics.Gauge;
import com.codahale.metrics.MetricRegistry;

import java.io.IOException;

import org.neo4j.com.storecopy.ToNetworkStoreWriter;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.ha.MasterClient210;
import org.neo4j.kernel.ha.com.master.MasterServer;
import org.neo4j.kernel.impl.annotations.Documented;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.metrics.MetricsSettings;

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

    private Config config;
    private Monitors monitors;
    private MetricRegistry registry;
    private final ByteCountsMetric masterNetworkTransactionWrites = new ByteCountsMetric();
    private final ByteCountsMetric masterNetworkStoreWrites = new ByteCountsMetric();
    private final ByteCountsMetric slaveNetworkTransactionWrites = new ByteCountsMetric();

    public NetworkMetrics( Config config, Monitors monitors, MetricRegistry registry )
    {
        this.config = config;
        this.monitors = monitors;
        this.registry = registry;
    }

    @Override
    public void start() throws Throwable
    {
        if ( config.get( MetricsSettings.neoNetworkEnabled ) )
        {
            /*
             * COM: MasterServer.class -> Writes transaction streams (writes)
             *      ToNetworkStoreWriter.class, "storeCopier -> Storage files write to network (writes)
             *
             * HA: MasterClientXXX.class -> Transactions written to network for commit (writes)
             */
            monitors.addMonitorListener( masterNetworkTransactionWrites, MasterServer.class.getName() );
            monitors.addMonitorListener( masterNetworkStoreWrites, ToNetworkStoreWriter.class.getName(),
                    ToNetworkStoreWriter.STORE_COPIER_MONITOR_TAG );
            monitors.addMonitorListener( slaveNetworkTransactionWrites, MasterClient210.class.getName() );

            registry.register( MASTER_NETWORK_TX_WRITES, new Gauge<Long>()
            {
                public Long getValue()
                {
                    return masterNetworkTransactionWrites.getBytesWritten();
                }
            } );

            registry.register( MASTER_NETWORK_STORE_WRITES, new Gauge<Long>()
            {
                public Long getValue()
                {
                    return masterNetworkStoreWrites.getBytesWritten();
                }
            } );

            registry.register( SLAVE_NETWORK_TX_WRITES, new Gauge<Long>()
            {
                public Long getValue()
                {
                    return slaveNetworkTransactionWrites.getBytesWritten();
                }
            } );
        }
    }

    @Override
    public void stop() throws IOException
    {
        if ( config.get( MetricsSettings.neoNetworkEnabled ) )
        {
            registry.remove( MASTER_NETWORK_TX_WRITES );
            registry.remove( MASTER_NETWORK_STORE_WRITES );
            registry.remove( SLAVE_NETWORK_TX_WRITES );

            monitors.removeMonitorListener( masterNetworkTransactionWrites );
            monitors.removeMonitorListener( masterNetworkStoreWrites );
            monitors.removeMonitorListener( slaveNetworkTransactionWrites );
        }
    }
}
