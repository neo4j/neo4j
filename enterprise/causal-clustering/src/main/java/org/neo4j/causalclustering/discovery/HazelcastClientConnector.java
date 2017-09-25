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
package org.neo4j.causalclustering.discovery;

import com.hazelcast.client.HazelcastClient;
import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.core.HazelcastInstance;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;

public class HazelcastClientConnector implements HazelcastConnector
{
    private final Config config;
    private final LogProvider logProvider;
    private final HostnameResolver hostnameResolver;

    HazelcastClientConnector( Config config, LogProvider logProvider, HostnameResolver hostnameResolver )
    {
        this.config = config;
        this.logProvider = logProvider;
        this.hostnameResolver = hostnameResolver;
    }

    @Override
    public HazelcastInstance connectToHazelcast()
    {
        ClientConfig clientConfig = new ClientConfig();

        ClientNetworkConfig networkConfig = clientConfig.getNetworkConfig();

        for ( AdvertisedSocketAddress address : config.get( CausalClusteringSettings.initial_discovery_members ) )
        {
            for ( AdvertisedSocketAddress advertisedSocketAddress : hostnameResolver.resolve( address ) )
            {
                networkConfig.addAddress( advertisedSocketAddress.toString() );
            }
        }

        additionalConfig( networkConfig, logProvider );

        return HazelcastClient.newHazelcastClient( clientConfig );
    }

    protected void additionalConfig( ClientNetworkConfig networkConfig, LogProvider logProvider )
    {

    }
}
