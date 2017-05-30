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

import com.hazelcast.client.config.ClientNetworkConfig;
import com.hazelcast.config.NetworkConfig;
import com.hazelcast.config.SSLConfig;

import org.neo4j.logging.LogProvider;
import org.neo4j.ssl.SslPolicy;

// TODO: Investigate if client auth actually can be configured as below.
// TODO: Fix the enterprise check or fix HZ to not silently fail otherwise.
class HazelcastSslConfiguration
{
    static void configureSsl( NetworkConfig networkConfig, SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = commonSslConfig( sslPolicy, logProvider );
        networkConfig.setSSLConfig( sslConfig );
    }

    static void configureSsl( ClientNetworkConfig clientNetworkConfig, SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = commonSslConfig( sslPolicy, logProvider );
        clientNetworkConfig.setSSLConfig( sslConfig );
    }

    private static SSLConfig commonSslConfig( SslPolicy sslPolicy, LogProvider logProvider )
    {
        SSLConfig sslConfig = new SSLConfig();

        if ( sslPolicy == null )
        {
            return sslConfig;
        }

        /* N.B: this check is currently broken in HZ 3.7 - SSL might be silently unavailable */
//        if ( !com.hazelcast.instance.BuildInfoProvider.getBuildInfo().isEnterprise() )
//        {
//            throw new UnsupportedOperationException( "Hazelcast can only use SSL under the enterprise version." );
//        }

        sslConfig.setFactoryImplementation(
                new HazelcastSslContextFactory( sslPolicy, logProvider ) )
                .setEnabled( true );

        switch ( sslPolicy.getClientAuth() )
        {
        case REQUIRE:
            sslConfig.setProperty( "javax.net.ssl.mutualAuthentication", "REQUIRED" );
            break;
        case OPTIONAL:
            sslConfig.setProperty( "javax.net.ssl.mutualAuthentication", "OPTIONAL" );
            break;
        case NONE:
            break;
        default:
            throw new IllegalArgumentException( "Not supported: " + sslPolicy.getClientAuth() );
        }

        return sslConfig;
    }
}
