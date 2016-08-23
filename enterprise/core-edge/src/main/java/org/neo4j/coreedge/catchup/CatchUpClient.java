/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
package org.neo4j.coreedge.catchup;

import java.time.Clock;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.CatchUpRequest;
import org.neo4j.coreedge.messaging.address.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class CatchUpClient extends LifecycleAdapter
{
    private final LogProvider logProvider;
    private final TopologyService discoveryService;
    private final Clock clock;

    private final Map<AdvertisedSocketAddress, CatchUpChannel> idleChannels = new HashMap<>();
    private final Set<CatchUpChannel> activeChannels = new HashSet<>();
    private final Log log;

    private NioEventLoopGroup eventLoopGroup;

    public CatchUpClient( TopologyService discoveryService, LogProvider logProvider, Clock clock )
    {
        this.logProvider = logProvider;
        this.discoveryService = discoveryService;
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
    }

    public <T> T makeBlockingRequest( MemberId memberId, CatchUpRequest request,
                                      long inactivityTimeout, TimeUnit timeUnit,
                                      CatchUpResponseCallback<T> responseHandler )
            throws CatchUpClientException, NoKnownAddressesException
    {
        CompletableFuture<T> future = new CompletableFuture<>();
        CatchUpChannel channel = acquireChannel( memberId );

        future.whenComplete( ( result, e ) -> {
            if ( e == null )
            {
                release( channel );
            }
            else
            {
                dispose( channel );
            }
        } );

        channel.setResponseHandler( responseHandler, future );
        channel.send( request );

        return TimeoutLoop.waitForCompletion( future, channel::millisSinceLastResponse, inactivityTimeout, timeUnit );
    }

    private synchronized void dispose( CatchUpChannel channel )
    {
        activeChannels.remove( channel );
        channel.close();
    }

    private synchronized void release( CatchUpChannel channel )
    {
        activeChannels.remove( channel );
        idleChannels.put( channel.destination, channel );
    }

    private synchronized CatchUpChannel acquireChannel( MemberId memberId ) throws NoKnownAddressesException
    {
        AdvertisedSocketAddress catchUpAddress =
                discoveryService.currentTopology().coreAddresses( memberId ).getCatchupServer();
        CatchUpChannel channel = idleChannels.remove( catchUpAddress );
        if ( channel == null )
        {
            channel = new CatchUpChannel( catchUpAddress );
        }
        activeChannels.add( channel );
        return channel;
    }

    private class CatchUpChannel
    {
        private final TrackingResponseHandler handler;
        private final AdvertisedSocketAddress destination;
        private Channel nettyChannel;

        CatchUpChannel( AdvertisedSocketAddress destination )
        {
            this.destination = destination;
            handler = new TrackingResponseHandler( new CatchUpResponseAdaptor(), clock );
            Bootstrap bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( new ChannelInitializer<SocketChannel>()
                    {
                        @Override
                        protected void initChannel( SocketChannel ch ) throws Exception
                        {
                            CatchUpClientChannelPipeline.initChannel( ch, handler, logProvider );
                        }
                    } );

            ChannelFuture channelFuture = bootstrap.connect( destination.socketAddress() );
            nettyChannel = channelFuture.awaitUninterruptibly().channel();
        }

        void setResponseHandler( CatchUpResponseCallback responseHandler,
                                 CompletableFuture<?> requestOutcomeSignal )
        {
            handler.setResponseHandler( responseHandler, requestOutcomeSignal );
        }

        void send( CatchUpRequest request )
        {
            nettyChannel.write( request.messageType() );
            nettyChannel.writeAndFlush( request );
        }

        long millisSinceLastResponse()
        {
            return clock.millis() - handler.lastResponseTime();
        }

        void close()
        {
            nettyChannel.close();
        }
    }

    @Override
    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup( 0, new NamedThreadFactory( "catch-up-client" ) );
    }

    @Override
    public void stop() throws Throwable
    {
        try
        {
            idleChannels.values().forEach( CatchUpChannel::close );
            activeChannels.forEach( CatchUpChannel::close );
            eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
        }
        catch ( InterruptedException e )
        {
            log.warn( "Interrupted while stopping catch up client." );
        }
    }
}
