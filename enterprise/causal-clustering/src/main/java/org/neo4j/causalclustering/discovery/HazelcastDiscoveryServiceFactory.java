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

import com.hazelcast.spi.properties.GroupProperty;

import java.util.logging.Level;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.identity.MemberId;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.LogProvider;
import org.neo4j.scheduler.JobScheduler;

public class HazelcastDiscoveryServiceFactory implements DiscoveryServiceFactory
{
    @Override
    public CoreTopologyService coreTopologyService( Config config, MemberId myself, JobScheduler jobScheduler,
            LogProvider logProvider, LogProvider userLogProvider, HostnameResolver hostnameResolver,
            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastCoreTopologyService( config, myself, jobScheduler, logProvider, userLogProvider, hostnameResolver,
                topologyServiceRetryStrategy );
    }

    @Override
    public TopologyService topologyService( Config config, LogProvider logProvider,
                                            JobScheduler jobScheduler, MemberId myself, HostnameResolver hostnameResolver,
                                            TopologyServiceRetryStrategy topologyServiceRetryStrategy )
    {
        configureHazelcast( config, logProvider );
        return new HazelcastClient( new HazelcastClientConnector( config, logProvider, hostnameResolver ), jobScheduler,
                logProvider, config, myself );
    }

    public static void configureHazelcast( Config config, LogProvider logProvider )
    {
        // tell hazelcast to not phone home
        GroupProperty.PHONE_HOME_ENABLED.setSystemProperty( "false" );
        GroupProperty.SOCKET_BIND_ANY.setSystemProperty( "false" );
        GroupProperty.SHUTDOWNHOOK_ENABLED.setSystemProperty( "false" );

        String licenseKey = config.get( CausalClusteringSettings.hazelcast_license_key );
        if ( licenseKey != null )
        {
            GroupProperty.ENTERPRISE_LICENSE_KEY.setSystemProperty( licenseKey );
        }

        // Make hazelcast quiet
        if ( config.get( CausalClusteringSettings.disable_middleware_logging ) )
        {
            // This is clunky, but the documented programmatic way doesn't seem to work
            GroupProperty.LOGGING_TYPE.setSystemProperty( "none" );
        }
        else
        {
            HazelcastLogging.enable( logProvider, new HazelcastLogLevel( config ) );
        }
    }

    private static class HazelcastLogLevel extends Level
    {
        HazelcastLogLevel( Config config )
        {
            super( "HAZELCAST", config.get( CausalClusteringSettings.middleware_logging_level ) );
        }
    }
}
