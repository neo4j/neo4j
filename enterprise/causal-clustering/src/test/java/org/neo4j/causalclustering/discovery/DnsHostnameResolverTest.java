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

import org.junit.Test;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.AssertableLogProvider;
import org.neo4j.logging.internal.SimpleLogService;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.discovery.MultiRetryStrategyTest.testRetryStrategy;

public class DnsHostnameResolverTest
{
    private final MapDomainNameResolver mockDomainNameResolver = new MapDomainNameResolver( new HashMap<>() );
    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final Config config = Config.defaults( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "2" );

    private final DnsHostnameResolver resolver =
            new DnsHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockDomainNameResolver, config, testRetryStrategy( 1 ) );

    @Test
    public void hostnamesAreResolvedByTheResolver()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        Collection<AdvertisedSocketAddress> resolvedAddresses =
                resolver.resolve( new AdvertisedSocketAddress( "google.com", 80 ) );

        // then
        assertEquals( 2, resolvedAddresses.size() );
        assertTrue( resolvedAddresses.removeIf( address -> address.getHostname().equals( "1.2.3.4" ) ) );
        assertTrue( resolvedAddresses.removeIf( address -> address.getHostname().equals( "5.6.7.8" ) ) );
    }

    @Test
    public void resolvedHostnamesUseTheSamePort()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        List<AdvertisedSocketAddress> resolvedAddresses =
                new ArrayList<>( resolver.resolve( new AdvertisedSocketAddress( "google.com", 1234 ) ) );

        // then
        assertEquals( 2, resolvedAddresses.size() );
        assertEquals( 1234, resolvedAddresses.get( 0 ).getPort() );
        assertEquals( 1234, resolvedAddresses.get( 1 ).getPort() );
    }

    @Test
    public void resolutionDetailsAreLoggedToUserLogs()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );

        // when
        resolver.resolve( new AdvertisedSocketAddress( "google.com", 1234 ) );

        // then
        userLogProvider.assertContainsMessageContaining( "Resolved initial host '%s' to %s" );
    }

    @Test
    public void unknownHostExceptionsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new AdvertisedSocketAddress( "google.com", 1234 ) );

        // then
        logProvider.assertContainsMessageContaining( "Failed to resolve host '%s'" );
    }

    @Test
    public void resolverRetriesUntilHostnamesAreFound()
    {
        // given
        mockDomainNameResolver.setHostnameAddresses( "google.com", asList( "1.2.3.4", "5.6.7.8" ) );
        DomainNameResolver mockResolver = spy( mockDomainNameResolver );
        when( mockResolver.resolveDomainName( anyString() ) )
                .thenReturn( new InetAddress[0] )
                .thenReturn( new InetAddress[0] )
                .thenCallRealMethod();

        DnsHostnameResolver resolver =
                new DnsHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockResolver, config, testRetryStrategy( 2 ) );

        // when
        List<AdvertisedSocketAddress> resolvedAddresses =
                new ArrayList<>( resolver.resolve( new AdvertisedSocketAddress( "google.com", 1234 ) ) );

        // then
        verify( mockResolver, times( 3 ) ).resolveDomainName( "google.com" );
        assertEquals( 2, resolvedAddresses.size() );
        assertEquals( 1234, resolvedAddresses.get( 0 ).getPort() );
        assertEquals( 1234, resolvedAddresses.get( 1 ).getPort() );
    }
}
