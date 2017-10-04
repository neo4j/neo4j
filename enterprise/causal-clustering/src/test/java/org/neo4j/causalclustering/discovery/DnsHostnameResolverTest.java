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
