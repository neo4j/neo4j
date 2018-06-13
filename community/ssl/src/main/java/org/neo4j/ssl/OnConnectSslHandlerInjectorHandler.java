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
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import javax.net.ssl.SSLEngine;

import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

public class OnConnectSslHandlerInjectorHandler extends ChannelDuplexHandler
{
    private final ChannelPipeline pipeline;
    private final SslContext sslContext;
    private SslHandler sslHandler;
    private final Log log;
    private final HostnameVerificationEngineModification hostnameVerificationEngineModification;
    private final boolean isClient;
    private final boolean verifyHostname;

    public OnConnectSslHandlerInjectorHandler( Channel channel, SslContext sslContext, boolean isClient, boolean verifyHostname, LogProvider logProvider )
    {
        this.pipeline = channel.pipeline();
        this.sslContext = sslContext;
        String rnd = UUID.randomUUID().toString();
        this.log = logProvider.getLog( OnConnectSslHandlerInjectorHandler.class.getName() + "-" + rnd );
        this.hostnameVerificationEngineModification = new HostnameVerificationEngineModification( logProvider );
        this.isClient = isClient;
        this.verifyHostname = verifyHostname;
    }

    /**
     * Main event that is triggered for connections and swapping out SslHandler for this handler. channelActive and handlerAdded handlers are
     * secondary boundary cases to this.
     *
     * @param ctx
     * @param remoteAddress
     * @param localAddress
     * @param promise
     * @throws Exception
     */
    @Override
    public void connect( ChannelHandlerContext ctx, SocketAddress remoteAddress, SocketAddress localAddress, ChannelPromise promise ) throws Exception
    {
        log.debug( "connect" );
        if ( sslHandler != null )
        {
            throw new IllegalStateException( "Connection initiated, but handler already exists" );
        }
        sslHandler = createSslHandler( ctx, (InetSocketAddress) remoteAddress );
        replaceSelfWith( sslHandler );
        sslHandler.connect( ctx, remoteAddress, localAddress, promise );
    }

    @Override
    public void channelActive( ChannelHandlerContext ctx ) throws Exception
    {
        log.debug( "channelActive" );
        if ( sslHandler != null )
        {
            throw new IllegalStateException( "Connection initiated, but handler already exists" );
        }
        sslHandler = createSslHandler( ctx );
        replaceSelfWith( sslHandler );
        sslHandler.channelActive( ctx );
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx ) throws Exception
    {
        if ( ctx.channel().isActive() )
        {
            if ( sslHandler != null )
            {
                throw new IllegalStateException(
                        "handlerAdded called on injector handler. Channel is active but handler is defined - the handler should have been replaced by now. " );
            }
            sslHandler = createSslHandler( ctx );
            replaceSelfWith( sslHandler );
            sslHandler.handlerAdded( ctx ); //we dont need to trigger active since the sslHandler checks if the provided ctx channel is active
        }
    }

    /**
     * Replaces this entry of handler in the netty pipeline with the provided SslHandler and maintains the handler name
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

    private SslHandler createSslHandler( ChannelHandlerContext ctx )
    {
        InetSocketAddress inetSocketAddress = (InetSocketAddress) ctx.channel().remoteAddress();
        return createSslHandler( ctx, inetSocketAddress );
    }

    private SslHandler createSslHandler( ChannelHandlerContext ctx, InetSocketAddress inetSocketAddress )
    {
        log.debug( "Creating new sslHandler for address %s:%d", inetSocketAddress.getHostName(), inetSocketAddress.getPort() );
        SSLEngine sslEngine = sslContext.newEngine( ctx.alloc(), inetSocketAddress.getHostName(), inetSocketAddress.getPort() );
        if ( verifyHostname )
        {
            sslEngine = hostnameVerificationEngineModification.configureHostnameVerification( sslEngine, inetSocketAddress.getHostName() );
        }
        sslEngine.setUseClientMode( isClient );

        // Dont need to set tls versions since that is set up from the context
        return new SslHandler( sslEngine );
    }

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
        return Optional.ofNullable( sslHandler ).orElseThrow( () -> new RuntimeException( "Ssl handler has not been initialised" ) );
    }
}
