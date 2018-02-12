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

import java.util.Collection;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.collection.Pair;
import org.neo4j.stream.Streams;

public class ReconnectingChannels
{
    private final ConcurrentHashMap<AdvertisedSocketAddress,ReconnectingChannel> lazyChannelMap =
            new ConcurrentHashMap<>();

    public int size()
    {
        return lazyChannelMap.size();
    }

    public ReconnectingChannel get( AdvertisedSocketAddress to )
    {
        return lazyChannelMap.get( to );
    }

    public ReconnectingChannel putIfAbsent( AdvertisedSocketAddress to, ReconnectingChannel timestampedLazyChannel )
    {
        return lazyChannelMap.putIfAbsent( to, timestampedLazyChannel );
    }

    public Collection<ReconnectingChannel> values()
    {
        return lazyChannelMap.values();
    }

    public void remove( AdvertisedSocketAddress address )
    {
        lazyChannelMap.remove( address );
    }

    public Stream<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocols()
    {
        return lazyChannelMap.entrySet().stream()
                .map( this::installedProtocolOpt )
                .flatMap( Streams::ofOptional );
    }

    private Optional<Pair<AdvertisedSocketAddress,ProtocolStack>> installedProtocolOpt( Map.Entry<AdvertisedSocketAddress,ReconnectingChannel> entry )
    {
        return entry.getValue().installedProtocolStack()
                .map( protocols -> Pair.of( entry.getKey(), protocols ) );
    }
}
