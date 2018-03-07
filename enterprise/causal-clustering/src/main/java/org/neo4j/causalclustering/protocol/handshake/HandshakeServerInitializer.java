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
    private final ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository;
    private final ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository;
    private final Protocol.ApplicationProtocolIdentifier allowedProtocol;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;

    public HandshakeServerInitializer( LogProvider logProvider, ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository,
            ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository, Protocol.ApplicationProtocolIdentifier allowedProtocol,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.allowedProtocol = allowedProtocol;
        this.protocolInstallerRepository = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
    }

    @Override
    public void initChannel( SocketChannel ch ) throws Exception
    {
        pipelineBuilderFactory.server( ch, log )
                .addFraming()
                .add( "handshake_server_encoder", new ServerMessageEncoder() )
                .add( "handshake_server_decoder", new ServerMessageDecoder() )
                .add( "handshake_server", createHandshakeServer( ch ) )
                .install();
    }

    private NettyHandshakeServer createHandshakeServer( SocketChannel channel )
    {
        HandshakeServer handshakeServer =
                new HandshakeServer( new SimpleNettyChannel( channel, log ), applicationProtocolRepository, modifierProtocolRepository, allowedProtocol );
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
            protocolInstallerRepository.installerFor( protocolStack ).install( channel );
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
