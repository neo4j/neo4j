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
package org.neo4j.causalclustering.catchup;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.neo4j.causalclustering.common.EventLoopContext;
import org.neo4j.causalclustering.common.NettyApplication;
import org.neo4j.causalclustering.common.client.ClientConnector;
import org.neo4j.causalclustering.handlers.PipelineHandlerAppender;
import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.Lifecycle;
import org.neo4j.kernel.monitoring.Monitors;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static org.neo4j.causalclustering.catchup.TimeoutLoop.waitForCompletion;

public class CatchUpClient
{
    private final Log log;
    private final Clock clock;
    private final long inactivityTimeoutMillis;
    private final CatchUpChannelPool<CatchUpChannel> pool = new CatchUpChannelPool<>( CatchUpChannel::new );
    private final ClientConnector<NioSocketChannel> clientConnector;

    private final CatchupClientBootstrapper catchupClientBootstrapper;
    private final NettyApplication<NioSocketChannel> nettyApplication;

    public CatchUpClient( LogProvider logProvider, Clock clock, long inactivityTimeoutMillis, Monitors monitors,
            PipelineHandlerAppender pipelineAppender )
    {
        catchupClientBootstrapper = new CatchupClientBootstrapper( logProvider, monitors, pipelineAppender );
        clientConnector = new ClientConnector<>( catchupClientBootstrapper.bootstrapper() );
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        nettyApplication = new NettyApplication<>( clientConnector, this::createExecutorContext );
    }

    private EventLoopContext<NioSocketChannel> createExecutorContext()
    {
        return new EventLoopContext<>( new NioEventLoopGroup( 0, new NamedThreadFactory( "catchup-client" ) ),
                NioSocketChannel.class );
    }

    public <T> T makeBlockingRequest( AdvertisedSocketAddress upstream, CatchUpRequest request,
            CatchUpResponseCallback<T> responseHandler )
            throws CatchUpClientException
    {
        CompletableFuture<T> future = new CompletableFuture<>();

        CatchUpChannel channel = pool.acquire( upstream );

        future.whenComplete( ( result, e ) ->
        {
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

        String operation = format( "Timed out executing operation %s on %s ",
                request, upstream );

        return waitForCompletion( future, operation, channel::millisSinceLastResponse, inactivityTimeoutMillis, log );
    }

    public Lifecycle getLifecycle()
    {
        return nettyApplication;
    }

    private class CatchUpChannel implements CatchUpChannelPool.Channel
    {
        private final TrackingResponseHandler handler;
        private final AdvertisedSocketAddress destination;
        private Channel nettyChannel;

        CatchUpChannel( AdvertisedSocketAddress destination )
        {
            this.destination = destination;
            this.handler = new TrackingResponseHandler( new CatchUpResponseAdaptor(), clock );
            this.nettyChannel = clientConnector
                    .connect( destination.socketAddress(), catchupClientBootstrapper.addHandler( handler ) );
        }

        void setResponseHandler( CatchUpResponseCallback responseHandler, CompletableFuture<?> requestOutcomeSignal )
        {
            handler.setResponseHandler( responseHandler, requestOutcomeSignal );
        }

        void send( CatchUpRequest request )
        {
            nettyChannel.write( request.messageType() );
            nettyChannel.writeAndFlush( request );
        }

        Optional<Long> millisSinceLastResponse()
        {
            return handler.lastResponseTime().map( lastResponseMillis -> clock.millis() - lastResponseMillis );
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

    class CatchupClientBootstrapper
    {
        private final LogProvider logProvider;
        private final Monitors monitors;
        private final PipelineHandlerAppender pipelineAppender;

        CatchupClientBootstrapper( LogProvider logProvider, Monitors monitors,
                PipelineHandlerAppender pipelineAppender )
        {
            this.logProvider = logProvider;
            this.monitors = monitors;
            this.pipelineAppender = pipelineAppender;
        }

        Function<EventLoopContext<NioSocketChannel>,Bootstrap> bootstrapper()
        {
            return eventLoopContext -> new Bootstrap().group( eventLoopContext.eventExecutors() )
                    .channel( eventLoopContext.channelClass() );
        }

        Function<Bootstrap,Bootstrap> addHandler( TrackingResponseHandler handler )
        {
            return bootstrap -> bootstrap.handler( new ChannelInitializer<SocketChannel>()
            {
                @Override
                protected void initChannel( SocketChannel ch ) throws Exception
                {
                    CatchUpClientChannelPipeline
                            .initChannel( ch, handler, logProvider, monitors, pipelineAppender );
                }
            } );
        }
    }
}
