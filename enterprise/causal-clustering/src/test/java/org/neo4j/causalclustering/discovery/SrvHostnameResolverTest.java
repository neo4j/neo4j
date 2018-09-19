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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Stream;

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

public class SrvHostnameResolverTest
{
    private final MockSrvRecordResolver mockSrvRecordResolver =
            new MockSrvRecordResolver( new HashMap<String,List<SrvRecordResolver.SrvRecord>>()
            {
                {
                    put( "emptyrecord.com", new ArrayList<>() );
                }
            } );

    private final AssertableLogProvider logProvider = new AssertableLogProvider();
    private final AssertableLogProvider userLogProvider = new AssertableLogProvider();
    private final Config config = Config.defaults( CausalClusteringSettings.minimum_core_cluster_size_at_formation, "2" );

    private final SrvHostnameResolver resolver =
            new SrvHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockSrvRecordResolver, config, testRetryStrategy( 1 ) );

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

    @Test
    public void resolverRetriesUntilHostnamesAreFound() throws Exception
    {
        // given
        mockSrvRecordResolver.addRecords( "_discovery._tcp.google.com",
                asList(
                        SrvRecordResolver.SrvRecord.parse( "1 1 80 1.2.3.4" ),
                        SrvRecordResolver.SrvRecord.parse( "1 1 8080 5.6.7.8" )
                )
        );
        SrvRecordResolver mockResolver = spy( mockSrvRecordResolver );
        when(  mockResolver.resolveSrvRecord( anyString() ) )
                .thenReturn( Stream.empty() )
                .thenReturn( Stream.empty() )
                .thenCallRealMethod();

        SrvHostnameResolver resolver =
                new SrvHostnameResolver( new SimpleLogService( userLogProvider, logProvider ), mockResolver, config, testRetryStrategy( 2 ) );

        // when
        Collection<AdvertisedSocketAddress> resolvedAddresses = resolver.resolve(
                new AdvertisedSocketAddress( "_discovery._tcp.google.com", 0 )
        );

        // then
        verify( mockResolver, times( 3 ) ).resolveSrvRecord( "_discovery._tcp.google.com" );

        assertEquals( 2, resolvedAddresses.size() );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "1.2.3.4" ) && address.getPort() == 80
        ) );

        assertTrue( resolvedAddresses.removeIf(
                address -> address.getHostname().equals( "5.6.7.8" ) && address.getPort() == 8080
        ) );

    }
}
