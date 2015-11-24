/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.coreedge.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.core.HazelcastInstance;

import org.neo4j.cluster.ClusterSettings;
import org.neo4j.coreedge.server.CoreEdgeClusterSettings;
import org.neo4j.coreedge.server.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;

public class HazelcastClientLifecycle extends LifecycleAdapter implements EdgeDiscoveryService
{
    private Config config;
    private HazelcastInstance hazelcastInstance;

    public HazelcastClientLifecycle( Config config )
    {
        this.config = config;
    }

    @Override
    public void start() throws Throwable
    {
        ClientConfig clientConfig = clientConfig();

        try
        {
            hazelcastInstance = HazelcastClient.newHazelcastClient( clientConfig );
        }
        catch ( IllegalStateException e )
        {
            // assume that IllegalStateExceptions only occur on connection failure
            throw new EdgeServerConnectionException( e );
        }

        addToClusterMap();
    }

    private void addToClusterMap()
    {
        hazelcastInstance
                .getMap( HazelcastClusterTopology.EDGE_SERVERS )
                .put( config.get( ClusterSettings.server_id ), 1 );
    }

    private ClientConfig clientConfig()
    {
        ClientConfig clientConfig = new ClientConfig();

        clientConfig.getGroupConfig().setName( config.get( ClusterSettings.cluster_name ) );


        for ( AdvertisedSocketAddress address : config.get( CoreEdgeClusterSettings.initial_core_cluster_members ) )
        {
            clientConfig.getNetworkConfig().addAddress( address.toString() );
        }
        return clientConfig;
    }

    @Override
    public void stop()
    {
        try
        {
            hazelcastInstance
                    .getMap( HazelcastClusterTopology.EDGE_SERVERS )
                    .remove( config.get( ClusterSettings.server_id ) );
            hazelcastInstance.shutdown();
        }
        catch ( RuntimeException ignored )
        {
            // this can happen if the edge server is trying to shutdown but
            // the core is gone
        }
    }

    @Override
    public ClusterTopology currentTopology()
    {
        return new HazelcastClusterTopology( hazelcastInstance );
    }
}
