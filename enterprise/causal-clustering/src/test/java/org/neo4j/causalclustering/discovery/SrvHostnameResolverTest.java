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

public class SrvHostnameResolverTest
{
    MockSrvRecordResolver mockSrvRecordResolver =
            new MockSrvRecordResolver( new HashMap<String,List<SrvRecordResolver.SrvRecord>>()
            {
                {
                    put( "emptyrecord.com", new ArrayList<>() );
                }
            } );

    AssertableLogProvider logProvider = new AssertableLogProvider();
    AssertableLogProvider userLogProvider = new AssertableLogProvider();

    private SrvHostnameResolver resolver = new SrvHostnameResolver( logProvider, userLogProvider, mockSrvRecordResolver );

    @Test
    public void hostnamesAndPortsAreResolvedByTheResolver()
    {
        // given
        mockSrvRecordResolver.addRecords( "_discovery._tcp.google.com",
                asList(
                        SrvRecordResolver.SrvRecord.parse( "1 1 80 1.2.3.4" ),
                        SrvRecordResolver.SrvRecord.parse( "1 1 8080 5.6.7.8" )
                )
        );

        // when
        Collection<AdvertisedSocketAddress> resolvedAddresses = resolver.resolve(
                new AdvertisedSocketAddress( "_discovery._tcp.google.com", 0 )
        );

        // then
        assertEquals( 2, resolvedAddresses.size() );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "1.2.3.4" ) && address.getPort() == 80
        ) );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "5.6.7.8" ) && address.getPort() == 8080
        ) );
    }

    @Test
    public void resolutionDetailsAreLoggedToUserLogs()
    {
        // given
        // given
        mockSrvRecordResolver.addRecord(
                "_resolutionDetailsAreLoggedToUserLogs._test.neo4j.com",
                SrvRecordResolver.SrvRecord.parse( "1 1 4321 1.2.3.4" )
        );

        // when
        resolver.resolve(
                new AdvertisedSocketAddress( "_resolutionDetailsAreLoggedToUserLogs._test.neo4j.com", 0 )
        );

        // then
        userLogProvider.assertContainsMessageContaining( "Resolved initial host '%s' to %s" );
    }

    @Test
    public void unknownHostExceptionsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new AdvertisedSocketAddress( "unknown.com", 0 ) );

        // then
        logProvider.assertContainsMessageContaining( "Failed to resolve srv records for '%s'" );
    }

    @Test
    public void emptyRecordListsAreLoggedAsErrors()
    {
        // when
        resolver.resolve( new AdvertisedSocketAddress( "emptyrecord.com", 0 ) );

        // then
        logProvider.assertContainsMessageContaining( "Failed to resolve srv records for '%s'" );
    }
}
