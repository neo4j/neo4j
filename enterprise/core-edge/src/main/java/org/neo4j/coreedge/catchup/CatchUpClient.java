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

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.time.Clock;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import org.neo4j.coreedge.discovery.NoKnownAddressesException;
import org.neo4j.coreedge.discovery.TopologyService;
import org.neo4j.coreedge.identity.MemberId;
import org.neo4j.coreedge.messaging.CatchUpRequest;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.util.concurrent.TimeUnit.MICROSECONDS;

public class CatchUpClient extends LifecycleAdapter
{
    private final LogProvider logProvider;
    private final TopologyService discoveryService;
    private final Log log;
    private final Clock clock;
    private final Monitors monitors;
    private final CatchUpChannelPool<CatchUpChannel> pool = new CatchUpChannelPool<>( CatchUpChannel::new );

    private NioEventLoopGroup eventLoopGroup;

    public CatchUpClient( TopologyService discoveryService, LogProvider logProvider, Clock clock, Monitors monitors )
    {
        this.logProvider = logProvider;
        this.discoveryService = discoveryService;
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.monitors = monitors;
    }

    public <T> T makeBlockingRequest( MemberId memberId, CatchUpRequest request,
                                      long inactivityTimeout, TimeUnit timeUnit,
                                      CatchUpResponseCallback<T> responseHandler )
            throws CatchUpClientException, NoKnownAddressesException
    {
        CompletableFuture<T> future = new CompletableFuture<>();
        AdvertisedSocketAddress catchUpAddress =
                discoveryService.coreServers().find( memberId ).getCatchupServer();
        CatchUpChannel channel = pool.acquire( catchUpAddress );

        future.whenComplete( ( result, e ) -> {
            if ( e == null )
            {
                pool.release( channel );
            }
            else
            {
                pool.dispose( channel );
            }
        } );

        channel.setResponseHandler( responseHandler, future );
        channel.send( request );

        String operation = String.format( "Timed out executing operation %s on %s", request, memberId );

        return TimeoutLoop.waitForCompletion( future, operation,
                channel::millisSinceLastResponse, inactivityTimeout, timeUnit );
    }

    private class CatchUpChannel implements CatchUpChannelPool.Channel
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
                            CatchUpClientChannelPipeline.initChannel( ch, handler, logProvider, monitors );
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

        @Override
        public AdvertisedSocketAddress destination()
        {
            return destination;
        }

        @Override
        public void close()
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
        log.info( "CatchUpClient stopping" );
        try
        {
            pool.close();
            eventLoopGroup.shutdownGracefully( 0, 0, MICROSECONDS ).sync();
        }
        catch ( InterruptedException e )
        {
            log.warn( "Interrupted while stopping catch up client." );
        }
    }
}
