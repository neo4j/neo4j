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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.causalclustering.core.CausalClusteringSettings;
import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
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
    private final Log log;
    private final ProtocolRepository protocolRepository;
    private final Protocol.Identifier protocolName;
    private final Duration timeout;
    private final ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstaller;
    private final NettyPipelineBuilderFactory pipelineBuilderFactory;
    private final TimeoutStrategy timeoutStrategy;

    public HandshakeClientInitializer( LogProvider logProvider, ProtocolRepository protocolRepository, Protocol.Identifier protocolName,
            ProtocolInstallerRepository<ProtocolInstaller.Orientation.Client> protocolInstallerRepository, Config config,
            NettyPipelineBuilderFactory pipelineBuilderFactory )
    {
        this.log = logProvider.getLog( getClass() );
        this.protocolRepository = protocolRepository;
        this.protocolName = protocolName;
        this.timeout = config.get( CausalClusteringSettings.handshake_timeout );
        this.protocolInstaller = protocolInstallerRepository;
        this.pipelineBuilderFactory = pipelineBuilderFactory;
        this.timeoutStrategy = new ExponentialBackoffStrategy( 1, 2000, MILLISECONDS );
    }

    private void installHandlers( Channel channel, HandshakeClient handshakeClient )
    {
        pipelineBuilderFactory.create( channel, log )
                .addFraming()
                .add( new ClientMessageEncoder() )
                .add( new ClientMessageDecoder() )
                .add( new NettyHandshakeClient( handshakeClient ) )
                .install();
    }

    @Override
    protected void initChannel( SocketChannel channel )
    {
        log.info( "Initiating channel: " + channel );
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
        log.info( String.format( "Scheduling handshake after: %d ms", timeout.getMillis() ) );
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
                log.warn( "Channel closed" );
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
        log.info( "Initiating handshake" );
        SimpleNettyChannel channelWrapper = new SimpleNettyChannel( channel, log );
        CompletableFuture<ProtocolStack> handshake = handshakeClient.initiate( channelWrapper, protocolRepository, protocolName );

        handshake.whenComplete( ( protocolStack, failure ) -> onHandshakeComplete( protocolStack, channel, failure ) );
    }

    private void onHandshakeComplete( ProtocolStack protocolStack, Channel channel, Throwable failure )
    {
        log.info( "Handshake completed" );
        if ( failure != null )
        {
            log.error( "Error when negotiating protocol stack", failure );
            channel.pipeline().fireUserEventTriggered( HandshakeFinishedEvent.getFailure() );
        }
        else
        {
            try
            {
                log.info( "Installing: " + protocolStack );
                protocolInstaller.installerFor( protocolStack.applicationProtocol() ).install( channel );
                log.info( "Firing handshake success event to handshake gate" );
                channel.pipeline().fireUserEventTriggered( HandshakeFinishedEvent.getSuccess() );
            }
            catch ( Exception e )
            {
                log.error( "Error installing pipeline", e );
                channel.close();
            }
        }
    }
}
