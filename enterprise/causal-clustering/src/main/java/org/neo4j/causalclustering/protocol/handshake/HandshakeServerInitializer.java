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
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.channel.socket.SocketChannel;

import java.net.InetSocketAddress;
import java.util.concurrent.RejectedExecutionException;

import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.net.ChildInitializer;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class HandshakeServerInitializer implements ChildInitializer
{
    private final Log log;
    private final ApplicationProtocolRepository applicationProtocolRepository;
    private final ModifierProtocolRepository modifierProtocolRepository;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;

    public HandshakeServerInitializer( ApplicationProtocolRepository applicationProtocolRepository,
            ModifierProtocolRepository modifierProtocolRepository,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Server> protocolInstallerRepository,
            NettyPipelineBuilderFactory pipelineBuilderFactory,
            LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.protocolInstallerRepository = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
    }

    @Override
    public void initChannel( SocketChannel ch ) throws Exception
    {
        log.info( "Installing handshake server on channel %s", ch );

        pipelineBuilderFactory.server( ch, log )
                .addFraming()
                .add( "handshake_server_encoder", new ServerMessageEncoder() )
                .add( "handshake_server_decoder", new ServerMessageDecoder() )
                .add( "handshake_server", createHandshakeServer( ch ) )
                .install();
    }

    private NettyHandshakeServer createHandshakeServer( SocketChannel channel )
    {
        HandshakeServer handshakeServer = new HandshakeServer(
                applicationProtocolRepository,
                modifierProtocolRepository,
                new SimpleNettyChannel( channel, log )
        );

        handshakeServer.protocolStackFuture().whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
        channel.closeFuture().addListener( f -> {
            try
            {
                channel.parent().pipeline().fireUserEventTriggered(
                        new ServerHandshakeFinishedEvent.Closed( toSocketAddress( channel ) ) );
            }
            catch ( RejectedExecutionException ignored )
            {
            }
        } );
        return new NettyHandshakeServer( handshakeServer );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, SocketChannel channel, Throwable failure )
    {
        if ( failure != null )
        {
            log.error( String.format( "Error when negotiating protocol stack on channel %s", channel ), failure );
            return;
        }

        try
        {
            log.info( "Handshake completed on channel %s. Installing: %s", channel, protocolStack );

            protocolInstallerRepository.installerFor( protocolStack ).install( channel );
            channel.parent().pipeline().fireUserEventTriggered( new ServerHandshakeFinishedEvent.Created( toSocketAddress( channel ), protocolStack ) );
        }
        catch ( Throwable t )
        {
            log.error( String.format( "Error installing protocol stack on channel %s", channel ), t );
        }
    }

    private SocketAddress toSocketAddress( SocketChannel channel )
    {
        InetSocketAddress inetSocketAddress = channel.remoteAddress();
        return new SocketAddress( inetSocketAddress.getHostString(), inetSocketAddress.getPort() );
    }
}
