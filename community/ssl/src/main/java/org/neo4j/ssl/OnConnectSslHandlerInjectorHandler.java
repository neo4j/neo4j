/*
 * Copyright (c) 2002-2018 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.ssl;

import io.netty.channel.Channel;
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.util.concurrent.Future;
import io.netty.util.concurrent.GenericFutureListener;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;

import org.neo4j.util.VisibleForTesting;

public class OnConnectSslHandlerInjectorHandler extends ChannelDuplexHandler
{
    private final ChannelPipeline pipeline;
    private final SslContext sslContext;
    private SslHandler sslHandler;
    private final Collection<Function<SSLEngine,SSLEngine>> engineModifications;

    OnConnectSslHandlerInjectorHandler( Channel channel, SslContext sslContext, boolean isClient, boolean verifyHostname, String[] tlsVersions )
    {
        this.pipeline = channel.pipeline();
        this.sslContext = sslContext;

        this.engineModifications = new ArrayList<>();
        engineModifications.add( new EssentialEngineModifications( tlsVersions, isClient ) );
        if ( verifyHostname )
        {
            engineModifications.add( new HostnameVerificationEngineModification() );
        }
    }

    /**
     * Main event that is triggered for connections and swapping out SslHandler for this handler. channelActive and handlerAdded handlers are
     * secondary boundary cases to this.
     *
     * @param ctx Context of the existing channel
     * @param remoteAddress the address used for initating a connection to a remote host (has type InetSocketAddress)
     * @param localAddress the local address that will be used for receiving responses from the remote host
     * @param promise the Channel promise to notify once the operation completes
     * @throws Exception when there is an error of any sort
     */
    @Override
    public void connect( ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise ) throws Exception
    {
        if ( sslHandler != null )
        {
            throw new IllegalStateException( "Connection initiated, but handler already exists" );
        }
        sslHandler = createSslHandler( ctx, (InetSocketAddress) remoteAddress );
        replaceSelfWith( sslHandler );
        sslHandler.connect( ctx, remoteAddress, localAddress, promise );
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx ) throws Exception
    {
        // Sometimes the connect event will have happened before adding, the channel will be active then
        if ( ctx.channel().isActive() )
        {
            sslHandler = createSslHandler( ctx, (InetSocketAddress) ctx.channel().remoteAddress() );
            replaceSelfWith( sslHandler );
            sslHandler.handlerAdded( ctx );
        }
    }

    @Override
    public void write( ChannelHandlerContext ctx, Object msg, ChannelPromise promise ) throws Exception
    {
        throw new RuntimeException( Thread.currentThread().getName() + " - This handler does not write" );
    }

    /**
     * Replaces this entry of handler in the netty pipeline with the provided SslHandler and maintains the handler name
     *
     * @param sslHandler configured netty handler that enables TLS
     */
    private void replaceSelfWith( SslHandler sslHandler )
    {
        String myName = pipeline.toMap()
                .entrySet()
                .stream()
                .filter( entry -> this.equals( entry.getValue() ) )
                .map( Map.Entry::getKey )
                .findFirst()
                .orElseThrow( () -> new IllegalStateException( "This handler has no name" ) );
        pipeline.replace( this, myName, sslHandler );
    }

    private SslHandler createSslHandler( ChannelHandlerContext ctx, InetSocketAddress inetSocketAddress )
    {
        SSLEngine sslEngine = sslContext.newEngine( ctx.alloc(), inetSocketAddress.getHostName(), inetSocketAddress.getPort() );
        for ( Function<SSLEngine,SSLEngine> mod : engineModifications )
        {
            sslEngine = mod.apply( sslEngine );
        }
        // Don't need to set tls versions since that is set up from the context
        return new SslHandler( sslEngine );
    }

    /**
     * Pretends to be a future, but instead delegates all methods to the actual future in SslHandler. Fails if sslHandler hasn't been initialised.
     * Don't use this method, instead favour a ChannelInboundHandler#userEventTriggered(ChannelHandlerContext ctx, SslHandshakeCompletionEvent evt)
     * TODO fix tests that use this method
     *
     * @return a Future type, that represents the state of the TLS handshake when the SslHandler has been initialised
     */
    @VisibleForTesting
    Future<Channel> handshakeFuture()
    {
        return new Future<Channel>()
        {
            private Future<Channel> handlerNotNullFuture()
            {
                if ( sslHandler == null )
                {
                    throw new RuntimeException( "SslHandler has not been created" );
                }
                return sslHandler.handshakeFuture();
            }

            @Override
            public boolean isSuccess()
            {
                return handlerNotNullFuture().isSuccess();
            }

            @Override
            public boolean isCancellable()
            {
                return handlerNotNullFuture().isCancellable();
            }

            @Override
            public Throwable cause()
            {
                return handlerNotNullFuture().cause();
            }

            @Override
            public Future<Channel> addListener( GenericFutureListener<? extends Future<? super Channel>> listener )
            {
                return handlerNotNullFuture().addListener( listener );
            }

            @Override
            public Future<Channel> addListeners( GenericFutureListener<? extends Future<? super Channel>>... listeners )
            {
                return handlerNotNullFuture().addListeners( listeners );
            }

            @Override
            public Future<Channel> removeListener( GenericFutureListener<? extends Future<? super Channel>> listener )
            {
                return handlerNotNullFuture().removeListener( listener );
            }

            @Override
            public Future<Channel> removeListeners( GenericFutureListener<? extends Future<? super Channel>>... listeners )
            {
                return handlerNotNullFuture().removeListeners( listeners );
            }

            @Override
            public Future<Channel> sync() throws InterruptedException
            {
                return handlerNotNullFuture().sync();
            }

            @Override
            public Future<Channel> syncUninterruptibly()
            {
                return handlerNotNullFuture().syncUninterruptibly();
            }

            @Override
            public Future<Channel> await() throws InterruptedException
            {
                return handlerNotNullFuture().await();
            }

            @Override
            public Future<Channel> awaitUninterruptibly()
            {
                return handlerNotNullFuture().awaitUninterruptibly();
            }

            @Override
            public boolean await( long timeout, TimeUnit unit ) throws InterruptedException
            {
                return handlerNotNullFuture().await( timeout, unit );
            }

            @Override
            public boolean await( long timeoutMillis ) throws InterruptedException
            {
                return handlerNotNullFuture().await( timeoutMillis );
            }

            @Override
            public boolean awaitUninterruptibly( long timeout, TimeUnit unit )
            {
                return handlerNotNullFuture().awaitUninterruptibly( timeout, unit );
            }

            @Override
            public boolean awaitUninterruptibly( long timeoutMillis )
            {
                return handlerNotNullFuture().awaitUninterruptibly( timeoutMillis );
            }

            @Override
            public Channel getNow()
            {
                return handlerNotNullFuture().getNow();
            }

            @Override
            public boolean cancel( boolean mayInterruptIfRunning )
            {
                return handlerNotNullFuture().cancel( mayInterruptIfRunning );
            }

            @Override
            public boolean isCancelled()
            {
                return handlerNotNullFuture().isCancelled();
            }

            @Override
            public boolean isDone()
            {
                return handlerNotNullFuture().isDone();
            }

            @Override
            public Channel get() throws InterruptedException, ExecutionException
            {
                return handlerNotNullFuture().get();
            }

            @Override
            public Channel get( long timeout, TimeUnit unit ) throws InterruptedException, ExecutionException, TimeoutException
            {
                return handlerNotNullFuture().get( timeout, unit );
            }
        };
    }

    public SslHandler getSslHandler()
    {
        if ( sslHandler == null )
        {
            throw new RuntimeException( "Ssl handler has not been initialised" );
        }
        return sslHandler;
    }
}
