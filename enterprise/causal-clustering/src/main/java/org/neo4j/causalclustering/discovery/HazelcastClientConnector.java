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

    public HazelcastClientConnector( Config config, LogProvider logProvider, HostnameResolver hostnameResolver )
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
