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
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;

import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HandshakeServerInitializer extends ChannelInitializer<SocketChannel>
{
    private final Log log;
    private final ProtocolRepository protocolRepository;
    private final Protocol.Identifier allowedProtocol;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;

    public HandshakeServerInitializer( LogProvider logProvider, ProtocolRepository protocolRepository, Protocol.Identifier allowedProtocol,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.protocolRepository = protocolRepository;
        this.allowedProtocol = allowedProtocol;
        this.protocolInstallerRepository = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
    }

    @Override
    public void initChannel( SocketChannel ch ) throws Exception
    {
        pipelineBuilderFactory.create( ch, log )
                .addFraming()
                .add( new ServerMessageEncoder() )
                .add( new ServerMessageDecoder() )
                .add( createHandshakeServer( ch ) )
                .install();
    }

    private NettyHandshakeServer createHandshakeServer( SocketChannel channel )
    {
        HandshakeServer handshakeServer = new HandshakeServer( new SimpleNettyChannel( channel, log ), protocolRepository, allowedProtocol );
        handshakeServer.protocolStackFuture().whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
        channel.closeFuture().addListener(
                ignored -> channel.parent().pipeline().fireUserEventTriggered( new ServerHandshakeFinishedEvent.Closed( toSocketAddress( channel ) ) )
        );
        return new NettyHandshakeServer( handshakeServer );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, SocketChannel channel, Throwable failure )
    {
        if ( failure != null )
        {
            log.error( "Error when negotiating protocol stack", failure );
            return;
        }

        try
        {
            protocolInstallerRepository.installerFor( protocolStack.applicationProtocol() ).install( channel );
            channel.parent().pipeline().fireUserEventTriggered( new ServerHandshakeFinishedEvent.Created( toSocketAddress( channel ), protocolStack ) );
        }
        catch ( Throwable t )
        {
            log.error( "Error installing protocol stack", t );
        }
    }

    private SocketAddress toSocketAddress( SocketChannel channel )
    {
        InetSocketAddress inetSocketAddress = channel.remoteAddress();
        return new SocketAddress( inetSocketAddress.getHostString(), inetSocketAddress.getPort() );
    }
}
