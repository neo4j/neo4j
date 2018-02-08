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
package org.neo4j.causalclustering.messaging;

import io.netty.bootstrap.Bootstrap;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelOutboundInvoker;
import io.netty.util.concurrent.Future;

import java.util.concurrent.CompletableFuture;
import java.util.function.BiFunction;
import java.util.function.Function;

import org.neo4j.causalclustering.helper.ExponentialBackoffStrategy;
import org.neo4j.causalclustering.helper.TimeoutStrategy;
import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ReconnectingChannel implements Channel
{
    private final Log log;
    private final Bootstrap bootstrap;
    private final SocketAddress destination;
    private final Function<io.netty.channel.Channel,ChannelInterceptor> channelInterceptorFactory;
    private final TimeoutStrategy connectionBackoffStrategy;

    private volatile io.netty.channel.Channel channel;
    private volatile ChannelFuture fChannel;

    private volatile boolean disposed;

    private TimeoutStrategy.Timeout connectionBackoff;
    private ChannelInterceptor channelInterceptor;

    ReconnectingChannel( Bootstrap bootstrap, final SocketAddress destination, final Log log,
            Function<io.netty.channel.Channel,ChannelInterceptor> channelInterceptorFactory )
    {
        this( bootstrap, destination, log, channelInterceptorFactory, new ExponentialBackoffStrategy( 100, 1600, MILLISECONDS ) );
    }

    private ReconnectingChannel( Bootstrap bootstrap, final SocketAddress destination, final Log log,
            Function<io.netty.channel.Channel,ChannelInterceptor> channelInterceptorFactory, TimeoutStrategy connectionBackoffStrategy )
    {
        this.bootstrap = bootstrap;
        this.destination = destination;
        this.log = log;
        this.channelInterceptorFactory = channelInterceptorFactory;
        this.connectionBackoffStrategy = connectionBackoffStrategy;
        this.connectionBackoff = connectionBackoffStrategy.newTimeout();
    }

    void start()
    {
        tryConnect();
    }

    private synchronized void tryConnect()
    {
        if ( disposed )
        {
            return;
        }
        else if ( fChannel != null && !fChannel.isDone() )
        {
            return;
        }

        fChannel = bootstrap.connect( destination.socketAddress() );
        channel = fChannel.channel();
        channelInterceptor = channelInterceptorFactory.apply( channel );

        fChannel.addListener( ( ChannelFuture f ) ->
        {
            if ( !f.isSuccess() )
            {
                f.channel().eventLoop().schedule( this::tryConnect, connectionBackoff.getMillis(), MILLISECONDS );
                connectionBackoff.increment();
            }
            else
            {
                log.info( "Connected: " + f.channel() );
                f.channel().closeFuture().addListener( closed ->
                {
                    log.warn( String.format( "Lost connection to: %s (%s)", destination, channel.remoteAddress() ) );
                    connectionBackoff = connectionBackoffStrategy.newTimeout();
                    f.channel().eventLoop().schedule( this::tryConnect, 0, MILLISECONDS );
                } );
            }
        } );
    }

    @Override
    public synchronized void dispose()
    {
        disposed = true;
        channel.close();
    }

    @Override
    public boolean isDisposed()
    {
        return disposed;
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public CompletableFuture<Void> write( Object msg )
    {
        return doWrite( msg, ChannelOutboundInvoker::write );
    }

    @Override
    public CompletableFuture<Void> writeAndFlush( Object msg )
    {
        return doWrite( msg, ChannelOutboundInvoker::writeAndFlush );
    }

    private CompletableFuture<Void> doWrite( Object msg, BiFunction<io.netty.channel.Channel, Object, Future<Void>> writer )
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }

        if ( channel.isActive() )
        {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            channelInterceptor.write( writer, channel, msg, promise );
            return promise;
        }
        else
        {
            CompletableFuture<Void> promise = new CompletableFuture<>();
            deferredWrite( msg, fChannel, promise, true, writer );
            return promise;
        }
    }

    /**
     * Will try to reconnect once before giving up on a send. The reconnection *must* happen
     * after write was scheduled. This is necessary to provide proper ordering when a message
     * is sent right after the non-blocking channel was setup and before the server is ready
     * to accept a connection. This happens frequently in tests.
     */
    private void deferredWrite( Object msg, ChannelFuture channelFuture, CompletableFuture<Void> promise, boolean firstAttempt,
            BiFunction<io.netty.channel.Channel, Object, Future<Void>> writer )
    {
        channelFuture.addListener( (ChannelFutureListener) f ->
        {
            if ( f.isSuccess() )
            {
                channelInterceptor.write( writer, f.channel(), msg, promise );
            }
            else if ( firstAttempt )
            {
                tryConnect();
                deferredWrite( msg, fChannel, promise, false, writer );
            }
            else
            {
                promise.completeExceptionally( f.cause() );
            }
        } );
    }

    @Override
    public String toString()
    {
        return "ReconnectingChannel{" + "channel=" + channel + ", disposed=" + disposed + '}';
    }
}
