/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
