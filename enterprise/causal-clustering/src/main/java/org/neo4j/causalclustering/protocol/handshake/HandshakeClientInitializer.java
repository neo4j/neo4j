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

import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.socket.SocketChannel;

import java.time.Duration;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.causalclustering.messaging.ReconnectingChannel;
import org.neo4j.causalclustering.messaging.SimpleNettyChannel;
import org.neo4j.causalclustering.protocol.NettyPipelineBuilderFactory;
import org.neo4j.causalclustering.protocol.Protocol;
import org.neo4j.causalclustering.protocol.ProtocolInstaller;
import org.neo4j.causalclustering.protocol.ProtocolInstallerRepository;
import org.neo4j.kernel.configuration.Config;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class HandshakeClientInitializer extends ChannelInitializer<SocketChannel>
{
    private final ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository;
    private final Protocol.ApplicationProtocolIdentifier applicationProtocolIdentifier;
    private final ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository;
    private final Set<Protocol.ModifierProtocolIdentifier> modifierProtocolIdentifiers;
    private final Duration timeout;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final TimeoutStrategy timeoutStrategy;
    private final Log log;

    public HandshakeClientInitializer( ProtocolRepository<Protocol.ApplicationProtocol> applicationProtocolRepository,
            Protocol.ApplicationProtocolIdentifier applicationProtocolIdentifier,
            ProtocolRepository<Protocol.ModifierProtocol> modifierProtocolRepository,
            Set<Protocol.ModifierProtocolIdentifier> modifierProtocolIdentifiers,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository, NettyPipelineBuilderFactory pipelineBuilderFactory,
            Config config, LogProvider logProvider )
    {
        this.log = logProvider.getLog( getClass() );
        this.applicationProtocolRepository = applicationProtocolRepository;
        this.applicationProtocolIdentifier = applicationProtocolIdentifier;
        this.modifierProtocolRepository = modifierProtocolRepository;
        this.modifierProtocolIdentifiers = modifierProtocolIdentifiers;
        this.timeout = config.get( CausalClusteringSettings.handshake_timeout );
        this.protocolInstaller = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.timeoutStrategy = new ExponentialBackoffStrategy( 1, 2000, MILLISECONDS );
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

        scheduleHandshake( channel, handshakeClient, timeoutStrategy.newTimeout() );
        scheduleTimeout( channel, handshakeClient );
    }

    /**
     * schedules the handshake initiation after the connection attempt
     */
    private void scheduleHandshake( SocketChannel ch, HandshakeClient handshakeClient, TimeoutStrategy.Timeout timeout )
    {
        ch.eventLoop().schedule( () ->
        {
            if ( ch.isActive() )
            {
                initiateHandshake( ch, handshakeClient );
            }
            else if ( ch.isOpen() )
            {
                timeout.increment();
                scheduleHandshake( ch, handshakeClient, timeout );
            }
            else
            {
                handshakeClient.failIfNotDone( "Channel closed" );
            }
        }, timeout.getMillis(), MILLISECONDS );
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
        SimpleNettyChannel channelWrapper = new SimpleNettyChannel( channel, log );
        CompletableFuture<ProtocolStack> handshake = handshakeClient.initiate( channelWrapper, applicationProtocolRepository, applicationProtocolIdentifier,
                modifierProtocolRepository, modifierProtocolIdentifiers );

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
