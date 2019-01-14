/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
        log.info( "Installing handshake server local %s remote %s", ch.localAddress(), ch.remoteAddress() );

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
