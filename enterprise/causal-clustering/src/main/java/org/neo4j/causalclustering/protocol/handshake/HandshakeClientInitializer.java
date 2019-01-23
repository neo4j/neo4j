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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.messaging.ReconnectingChannel;
import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HandshakeClientInitializer extends ChannelInitializer<SocketChannel>
{
    private final ApplicationProtocolRepository applicationProtocolRepository;
    private final ModifierProtocolRepository modifierProtocolRepository;
    private final Duration timeout;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final TimeoutStrategy handshakeDelay;
    private final Log log;

    public HandshakeClientInitializer( ApplicationProtocolRepository applicationProtocolRepository, ModifierProtocolRepository modifierProtocolRepository,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory,
            Duration handshakeTimeout, LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.timeout = handshakeTimeout;
        this.protocolInstaller = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.handshakeDelay = new ExponentialBackoffStrategy( 1, 2000, MILLISECONDS );
    }

    private void installHandlers( Channel channel, HandshakeClient handshakeClient ) throws Exception
    {
        pipelineBuilderFactory.client( channel, log )
                .addFraming()
                .add( "handshake_client_encoder", new ClientMessageEncoder() )
                .add( "handshake_client_decoder", new ClientMessageDecoder() )
                .add( "handshake_client", new NettyHandshakeClient( handshakeClient ) )
                .addGate( msg -> !(msg instanceof ServerMessage) )
                .install();
    }

    @Override
    protected void initChannel( SocketChannel channel ) throws Exception
    {
        HandshakeClient handshakeClient = new HandshakeClient();
        installHandlers( channel, handshakeClient );

        scheduleHandshake( channel, handshakeClient, handshakeDelay.newTimeout() );
        scheduleTimeout( channel, handshakeClient );
    }

    /**
     * schedules the handshake initiation after the connection attempt
     */
    private void scheduleHandshake( SocketChannel ch, HandshakeClient handshakeClient, TimeoutStrategy.Timeout handshakeDelay )
    {
        ch.eventLoop().schedule( () ->
        {
            if ( ch.isActive() )
            {
                initiateHandshake( ch, handshakeClient );
            }
            else if ( ch.isOpen() )
            {
                handshakeDelay.increment();
                scheduleHandshake( ch, handshakeClient, handshakeDelay );
            }
            else
            {
                handshakeClient.failIfNotDone( "Channel closed" );
            }
        }, handshakeDelay.getMillis(), MILLISECONDS );
    }

    private void scheduleTimeout( SocketChannel ch, HandshakeClient handshakeClient )
    {
        ch.eventLoop().schedule( () -> {
            if ( handshakeClient.failIfNotDone( "Timed out after " + timeout ) )
            {
                log.warn( "Failed handshake after timeout" );
            }
        }, timeout.toMillis(), TimeUnit.MILLISECONDS );
    }

    private void initiateHandshake( Channel channel, HandshakeClient handshakeClient )
    {
        log.info( "Initiating handshake local %s remote %s", channel.localAddress(), channel.remoteAddress() );

        SimpleNettyChannel channelWrapper = new SimpleNettyChannel( channel, log );
        CompletableFuture<ProtocolStack> handshake = handshakeClient.initiate( channelWrapper, applicationProtocolRepository, modifierProtocolRepository );

        handshake.whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, Channel channel, Throwable failure )
    {
        if ( failure != null )
        {
            log.error( "Error when negotiating protocol stack", failure );
            channel.pipeline().fireUserEventTriggered( GateEvent.getFailure() );
            channel.close();
        }
        else
        {
            try
            {
                log.info( "Installing: " + protocolStack );
                protocolInstaller.installerFor( protocolStack ).install( channel );
                channel.attr( ReconnectingChannel.PROTOCOL_STACK_KEY ).set( protocolStack );

                channel.pipeline().fireUserEventTriggered( GateEvent.getSuccess() );
                channel.flush();
            }
            catch ( Exception e )
            {
                log.error( "Error installing pipeline", e );
                channel.close();
            }
        }
    }
}
