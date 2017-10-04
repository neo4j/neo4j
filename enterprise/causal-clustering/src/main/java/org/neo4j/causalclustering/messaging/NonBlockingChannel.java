/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.EventLoop;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.Promise;

import org.neo4j.helpers.SocketAddress;
import org.neo4j.logging.Log;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

class NonBlockingChannel
{
    private static final int CONNECT_BACKOFF_MS = 250;

    private final Log log;
    private final Bootstrap bootstrap;
    private final EventLoop eventLoop;
    private final SocketAddress destination;

    private volatile Channel channel;
    private volatile ChannelFuture fChannel;

    private volatile boolean disposed;

    NonBlockingChannel( Bootstrap bootstrap, EventLoop eventLoop, final SocketAddress destination, final Log log )
    {
        this.bootstrap = bootstrap;
        this.eventLoop = eventLoop;
        this.destination = destination;
        this.log = log;
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

        fChannel.addListener( ( ChannelFuture f ) ->
        {
            if ( !f.isSuccess() )
            {
                f.channel().eventLoop().schedule( this::tryConnect, CONNECT_BACKOFF_MS, MILLISECONDS );
            }
            else
            {
                log.info( "Connected: " + f.channel() );
                f.channel().closeFuture().addListener( closed ->
                {
                    log.warn( String.format( "Lost connection to: %s (%s)", destination, channel.remoteAddress() ) );
                    f.channel().eventLoop().schedule( this::tryConnect, CONNECT_BACKOFF_MS, MILLISECONDS );
                } );
            }
        } );
    }

    public synchronized void dispose()
    {
        disposed = true;
        channel.close();
    }

    public Future<Void> send( Object msg )
    {
        if ( disposed )
        {
            throw new IllegalStateException( "sending on disposed channel" );
        }

        if ( channel.isActive() )
        {
            return channel.writeAndFlush( msg );
        }
        else
        {
            Promise<Void> promise = eventLoop.newPromise();
            deferredWrite( msg, fChannel, promise, true );
            return promise;
        }
    }

    /**
     * Will try to reconnect once before giving up on a send. The reconnection *must* happen
     * after write was scheduled. This is necessary to provide proper ordering when a message
     * is sent right after the non-blocking channel was setup and before the server is ready
     * to accept a connection. This happens frequently in tests.
     */
    private void deferredWrite( Object msg, ChannelFuture channelFuture, Promise<?> promise, boolean firstAttempt )
    {
        channelFuture.addListener( (ChannelFutureListener) f ->
        {
            if ( f.isSuccess() )
            {
                f.channel().writeAndFlush( msg ).addListener( x -> promise.setSuccess( null ) );
            }
            else if ( firstAttempt )
            {
                tryConnect();
                deferredWrite( msg, fChannel, promise, false );
            }
            else
            {
                promise.setFailure( f.cause() );
            }
        } );
    }
}
