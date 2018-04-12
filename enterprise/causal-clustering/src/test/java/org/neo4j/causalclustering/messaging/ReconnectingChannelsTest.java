/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.causalclustering.messaging;

import org.junit.Test;
import org.mockito.Mockito;

import java.util.Comparator;
import java.util.Optional;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.protocol.handshake.TestProtocols.TestApplicationProtocols;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;

import static co.unruly.matchers.StreamMatchers.contains;
import static co.unruly.matchers.StreamMatchers.empty;
import static java.util.Collections.emptyList;
import static org.junit.Assert.assertThat;

public class ReconnectingChannelsTest
{
    private ReconnectingChannels reconnectingChannels = new ReconnectingChannels();
    private AdvertisedSocketAddress to1 = new AdvertisedSocketAddress( "host1", 1 );
    private AdvertisedSocketAddress to2 = new AdvertisedSocketAddress( "host2", 1 );
    private ReconnectingChannel channel1 = Mockito.mock( ReconnectingChannel.class );
    private ReconnectingChannel channel2 = Mockito.mock( ReconnectingChannel.class );

    @Test
    public void shouldReturnEmptyStreamOfInstalledProtocolsIfNoChannels()
    {
        // when
        Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols = reconnectingChannels.installedProtocols();

        // then
        assertThat( installedProtocols, empty() );
    }

    @Test
    public void shouldReturnStreamOfInstalledProtocolsForChannelsThatHaveCompletedHandshake()
    {
        // given
        reconnectingChannels.putIfAbsent( to1, channel1 );
        reconnectingChannels.putIfAbsent( to2, channel2 );
        ProtocolStack protocolStack1 = new ProtocolStack( TestApplicationProtocols.RAFT_3, emptyList() );
        ProtocolStack protocolStack2 = new ProtocolStack( TestApplicationProtocols.RAFT_2, emptyList() );
        Mockito.when( channel1.installedProtocolStack() ).thenReturn( Optional.of( protocolStack1 ) );
        Mockito.when( channel2.installedProtocolStack() ).thenReturn( Optional.of( protocolStack2 ) );

        // when
        Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols = reconnectingChannels.installedProtocols();

        // then
        Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> sorted = installedProtocols.sorted( Comparator.comparing( p -> p.first().getHostname() ) );
        assertThat( sorted, contains( Pair.of( to1, protocolStack1 ), Pair.of( to2, protocolStack2 ) ) );
    }

    @Test
    public void shouldExcludeChannelsWithoutInstalledProtocol()
    {
        // given
        reconnectingChannels.putIfAbsent( to1, channel1 );
        reconnectingChannels.putIfAbsent( to2, channel2 );
        ProtocolStack protocolStack1 = new ProtocolStack( TestApplicationProtocols.RAFT_3, emptyList() );
        Mockito.when( channel1.installedProtocolStack() ).thenReturn( Optional.of( protocolStack1 ) );
        Mockito.when( channel2.installedProtocolStack() ).thenReturn( Optional.empty() );

        // when
        Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols = reconnectingChannels.installedProtocols();

        // then
        assertThat( installedProtocols, contains( Pair.of( to1, protocolStack1 ) ) );
    }
}
