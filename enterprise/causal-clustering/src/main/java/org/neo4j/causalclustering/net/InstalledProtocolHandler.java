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
package org.neo4j.causalclustering.net;

import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.Stream;

import org.neo4j.causalclustering.protocol.handshake.ProtocolStack;
import org.neo4j.causalclustering.protocol.handshake.ServerHandshakeFinishedEvent;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.helpers.collection.Pair;

@ChannelHandler.Sharable
public class InstalledProtocolHandler extends ChannelInboundHandlerAdapter
{
    private ConcurrentMap<SocketAddress,ProtocolStack> installedProtocols = new ConcurrentHashMap<>();

    @Override
    public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
    {
        if ( evt instanceof ServerHandshakeFinishedEvent.Created )
        {
            ServerHandshakeFinishedEvent.Created created = (ServerHandshakeFinishedEvent.Created) evt;
            installedProtocols.put( created.advertisedSocketAddress, created.protocolStack );
        }
        else if ( evt instanceof ServerHandshakeFinishedEvent.Closed )
        {
            ServerHandshakeFinishedEvent.Closed closed = (ServerHandshakeFinishedEvent.Closed) evt;
            installedProtocols.remove( closed.advertisedSocketAddress );
        }
        else
        {
            super.userEventTriggered( ctx, evt );
        }
    }

    public Stream<Pair<SocketAddress,ProtocolStack>> installedProtocols()
    {
        return installedProtocols.entrySet().stream().map( entry -> Pair.of( entry.getKey(), entry.getValue() ) );
    }
}
