/*
 * Copyright (c) 2002-2020 "Neo4j,"
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
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.channel.ChannelPipeline;
import io.netty.channel.ChannelPromise;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslHandler;
import io.netty.handler.ssl.SslHandshakeCompletionEvent;

import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Map;
import java.util.function.Function;
import javax.net.ssl.SSLEngine;

public class ClientSideOnConnectSslHandler extends ChannelDuplexHandler
{
    private final ChannelPipeline pipeline;
    private final SslContext sslContext;
    private final Collection<Function<SSLEngine,SSLEngine>> engineModifications;

    ClientSideOnConnectSslHandler( Channel channel, SslContext sslContext, boolean verifyHostname, String[] tlsVersions )
    {
        this.pipeline = channel.pipeline();
        this.sslContext = sslContext;

        this.engineModifications = new ArrayList<>();
        engineModifications.add( new EssentialEngineModifications( tlsVersions, true ) );
        if ( verifyHostname )
        {
            engineModifications.add( new ClientSideHostnameVerificationEngineModification() );
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
        SslHandler sslHandler = createSslHandler( ctx, (InetSocketAddress) remoteAddress );
        replaceSelfWith( sslHandler );
        ctx.connect( remoteAddress, localAddress, promise );
    }

    @Override
    public void handlerAdded( ChannelHandlerContext ctx ) throws Exception
    {
        // Sometimes the connect event will have happened before adding, the channel will be active then
        if ( ctx.channel().isActive() )
        {
            SslHandler sslHandler = createSslHandler( ctx, (InetSocketAddress) ctx.channel().remoteAddress() );
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
        pipeline.addAfter( myName, "handshakeCompletionSslDetailsHandler", new HandshakeCompletionSslDetailsHandler() );
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
     * Ssl protocol details are negotiated after handshake is complete.
     * Some tests rely on having these ssl details available.
     * Having this adapter exposes those details to the tests.
     */
    private class HandshakeCompletionSslDetailsHandler extends ChannelInboundHandlerAdapter
    {
        @Override
        public void userEventTriggered( ChannelHandlerContext ctx, Object evt ) throws Exception
        {
            if ( evt instanceof SslHandshakeCompletionEvent )
            {
                SslHandshakeCompletionEvent sslHandshakeEvent = (SslHandshakeCompletionEvent) evt;
                if ( sslHandshakeEvent.cause() == null )
                {
                    SslHandler sslHandler = ctx.pipeline().get( SslHandler.class );
                    String ciphers = sslHandler.engine().getSession().getCipherSuite();
                    String protocols = sslHandler.engine().getSession().getProtocol();

                    ctx.fireUserEventTriggered( new SslHandlerDetailsRegisteredEvent( ciphers, protocols ) );
                }
            }
            ctx.fireUserEventTriggered( evt );
        }
    }
}
