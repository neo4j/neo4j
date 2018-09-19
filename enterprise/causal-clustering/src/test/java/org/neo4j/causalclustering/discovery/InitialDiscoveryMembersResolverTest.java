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
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.kernel.configuration.Config;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.function.Function.identity;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.empty;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.when;
import static org.neo4j.causalclustering.core.CausalClusteringSettings.initial_discovery_members;

@RunWith( MockitoJUnitRunner.class )
public class InitialDiscoveryMembersResolverTest
{
    @Mock
    private HostnameResolver hostnameResolver;

    @Test
    public void shouldReturnEmptyCollectionIfEmptyInitialMembers()
    {
        // given
        Config config = Config.builder()
                .withSetting( initial_discovery_members, "" )
                .build();

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, empty() );
    }

    @Test
    public void shouldResolveAndReturnAllConfiguredAddresses()
    {
        // given
        AdvertisedSocketAddress input1 = new AdvertisedSocketAddress( "foo.bar", 123 );
        AdvertisedSocketAddress input2 = new AdvertisedSocketAddress( "baz.bar", 432 );
        AdvertisedSocketAddress input3 = new AdvertisedSocketAddress( "quux.bar", 789 );

        AdvertisedSocketAddress output1 = new AdvertisedSocketAddress( "a.b", 3 );
        AdvertisedSocketAddress output2 = new AdvertisedSocketAddress( "b.b", 34 );
        AdvertisedSocketAddress output3 = new AdvertisedSocketAddress( "c.b", 7 );

        String configString = Stream.of( input1, input2, input3 ).map( AdvertisedSocketAddress::toString ).collect( Collectors.joining( "," ) );

        Config config = Config.builder()
                .withSetting( initial_discovery_members, configString )
                .build();

        when( hostnameResolver.resolve( input1 ) ).thenReturn( asList( output1, output2 ) );
        when( hostnameResolver.resolve( input2 ) ).thenReturn( emptyList() );
        when( hostnameResolver.resolve( input3 ) ).thenReturn( singletonList( output3 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<AdvertisedSocketAddress> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( identity() );

        // then
        assertThat( result, containsInAnyOrder( output1, output2, output3 ) );
    }

    @Test
    public void shouldApplyTransform()
    {
        // given
        AdvertisedSocketAddress input1 = new AdvertisedSocketAddress( "foo.bar", 123 );

        AdvertisedSocketAddress output1 = new AdvertisedSocketAddress( "a.b", 3 );

        Config config = Config.builder()
                .withSetting( initial_discovery_members, input1.toString() )
                .build();

        when( hostnameResolver.resolve( input1 ) ).thenReturn( singletonList( output1 ) );

        InitialDiscoveryMembersResolver
                hostnameResolvingInitialDiscoveryMembersResolver = new InitialDiscoveryMembersResolver( hostnameResolver, config );

        // when
        Collection<String> result = hostnameResolvingInitialDiscoveryMembersResolver.resolve( address -> address.toString().toUpperCase() );

        // then
        assertThat( result, containsInAnyOrder( output1.toString().toUpperCase() ) );
    }
}
