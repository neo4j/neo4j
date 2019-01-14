/*
 * Copyright (c) 2002-2019 "Neo4j,"
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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.logging.AssertableLogProvider;

import static java.util.Arrays.asList;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class DnsHostnameResolverTest
{
    MapDomainNameResolver mockDomainNameResolver = new MapDomainNameResolver( new HashMap<>() );
    AssertableLogProvider logProvider = new AssertableLogProvider();
    AssertableLogProvider userLogProvider = new AssertableLogProvider();

    private DnsHostnameResolver resolver =
            new DnsHostnameResolver( logProvider, userLogProvider, mockDomainNameResolver );

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
        logProvider.assertContainsMessageContaining( "Failed to resolve host 'google.com'" );
    }
}
