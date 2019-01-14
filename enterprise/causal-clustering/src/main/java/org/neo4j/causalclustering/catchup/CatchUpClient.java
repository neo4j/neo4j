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
package org.neo4j.causalclustering.catchup;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;

import java.net.ConnectException;
import java.time.Clock;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.neo4j.causalclustering.messaging.CatchUpRequest;
import org.neo4j.helpers.AdvertisedSocketAddress;
import org.neo4j.helpers.NamedThreadFactory;
import org.neo4j.kernel.lifecycle.LifecycleAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static java.lang.String.format;
import static java.util.concurrent.TimeUnit.MICROSECONDS;
import static org.neo4j.causalclustering.catchup.TimeoutLoop.waitForCompletion;

public class CatchUpClient extends LifecycleAdapter
{
    private final Log log;
    private final Clock clock;
    private final long inactivityTimeoutMillis;
    private final Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer;

    private final CatchUpChannelPool<CatchUpChannel> pool = new CatchUpChannelPool<>( CatchUpChannel::new );

    private NioEventLoopGroup eventLoopGroup;

    public CatchUpClient( LogProvider logProvider, Clock clock, long inactivityTimeoutMillis,
            Function<CatchUpResponseHandler,ChannelInitializer<SocketChannel>> channelInitializer )
    {
        this.log = logProvider.getLog( getClass() );
        this.clock = clock;
        this.inactivityTimeoutMillis = inactivityTimeoutMillis;
        this.channelInitializer = channelInitializer;
    }

    public <T> T makeBlockingRequest( AdvertisedSocketAddress upstream, CatchUpRequest request, CatchUpResponseCallback<T> responseHandler )
            throws CatchUpClientException
    {
        CompletableFuture<T> future = new CompletableFuture<>();

        CatchUpChannel channel = null;
        try
        {
            channel = pool.acquire( upstream );
            channel.setResponseHandler( responseHandler, future );
            future.whenComplete( new ReleaseOnComplete( channel ) );
            channel.send( request );
        }
        catch ( Exception e )
        {
            if ( channel != null )
            {
                pool.dispose( channel );
            }
            throw new CatchUpClientException( "Failed to send request", e );
        }
        String operation = format( "Completed exceptionally when executing operation %s on %s ", request, upstream );
        return waitForCompletion( future, operation, channel::millisSinceLastResponse, inactivityTimeoutMillis, log );
    }

    private class ReleaseOnComplete implements BiConsumer<Object,Throwable>
    {
        private CatchUpChannel catchUpChannel;

        ReleaseOnComplete( CatchUpChannel catchUpChannel )
        {
            this.catchUpChannel = catchUpChannel;
        }

        @Override
        public void accept( Object o, Throwable throwable )
        {
            if ( throwable == null )
            {
                pool.release( catchUpChannel );
            }
            else
            {
                pool.dispose( catchUpChannel );
            }
        }
    }

    private class CatchUpChannel implements CatchUpChannelPool.Channel
    {
        private final TrackingResponseHandler handler;
        private final AdvertisedSocketAddress destination;
        private Channel nettyChannel;
        private final Bootstrap bootstrap;

        CatchUpChannel( AdvertisedSocketAddress destination )
        {
            this.destination = destination;
            handler = new TrackingResponseHandler( new CatchUpResponseAdaptor(), clock );
            bootstrap = new Bootstrap()
                    .group( eventLoopGroup )
                    .channel( NioSocketChannel.class )
                    .handler( channelInitializer.apply( handler ) );
        }

        void setResponseHandler( CatchUpResponseCallback responseHandler, CompletableFuture<?> requestOutcomeSignal )
        {
            handler.setResponseHandler( responseHandler, requestOutcomeSignal );
        }

        void send( CatchUpRequest request ) throws ConnectException
        {
            if ( !isActive() )
            {
                throw new ConnectException( "Channel is not connected" );
            }
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
        public void connect() throws Exception
        {
            ChannelFuture channelFuture = bootstrap.connect( destination.socketAddress() );
            nettyChannel = channelFuture.sync().channel();
            nettyChannel.closeFuture().addListener( (ChannelFutureListener) future -> handler.onClose() );

        }

        @Override
        public boolean isActive()
        {
            return nettyChannel.isActive();
        }

        @Override
        public void close()
        {
            if ( nettyChannel != null )
            {
                nettyChannel.close();
            }
        }
    }

    @Override
    public void start()
    {
        eventLoopGroup = new NioEventLoopGroup( 0, new NamedThreadFactory( "catch-up-client" ) );
    }

    @Override
    public void stop()
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
