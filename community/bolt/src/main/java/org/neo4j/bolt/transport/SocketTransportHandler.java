/*
 * Copyright (c) 2002-2017 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.bolt.transport;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import org.neo4j.bolt.BoltChannel;
import org.neo4j.bolt.BoltMessageLog;
import org.neo4j.bolt.BoltMessageLogger;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Handles incoming chunks of data for a given client channel. This initially will negotiate a protocol version to use,
 * and then delegate future messages to the chosen protocol.
 * <p/>
 * This class is stateful, one instance is expected per channel.
 */
public class SocketTransportHandler extends ChannelInboundHandlerAdapter
{
    private final BoltHandshakeProtocolHandler handshake;
    private final Log log;
    private final BoltMessageLog messageLog;

    private BoltMessagingProtocolHandler protocol;

    public SocketTransportHandler( BoltHandshakeProtocolHandler handshake,
                                   LogProvider logging, BoltMessageLog messageLog )
    {
        this.handshake = handshake;
        this.log = logging.getLog( getClass() );
        this.messageLog = messageLog;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( msg instanceof ByteBuf )
        {
            ByteBuf buffer = (ByteBuf) msg;
            if ( protocol == null )
            {
                BoltMessageLogger messageLogger = new BoltMessageLogger( messageLog, ctx.channel() );
                messageLogger.clientEvent( "OPEN" );
                performHandshake( BoltChannel.open( ctx, messageLogger ), buffer );
            }
            else
            {
                protocol.handle( ctx, buffer );
            }
        }
        else
        {
            ctx.fireChannelRead( msg );
        }
    }

    @Override
    public void channelInactive( ChannelHandlerContext ctx ) throws Exception
    {
        close( ctx );
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx ) throws Exception
    {
        close( ctx );
    }

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        log.error( "Fatal error occurred when handling a client connection: " + cause.getMessage(), cause );
        close( ctx );
    }

    private void close( ChannelHandlerContext ctx )
    {
        if ( protocol != null )
        {
            // handshake was successful and protocol was initialized, so it needs to be closed now
            // channel will be closed as part of the protocol's close procedure
            protocol.close();
            protocol = null;
        }
        else
        {
            // handshake did not happen or failed, protocol was not initialized, so we need to close the channel
            // channel will be closed as part of the context's close procedure
            ctx.close();
        }
    }

    private void performHandshake( BoltChannel boltChannel, ByteBuf buffer ) throws Exception
    {
        ChannelHandlerContext ctx = boltChannel.channelHandlerContext();
        HandshakeOutcome outcome = handshake.perform( boltChannel, buffer );
        switch ( outcome )
        {
        case PROTOCOL_CHOSEN:
            // A protocol version has been successfully agreed upon, therefore we can
            // reply positively with four bytes reflecting this selection and leave
            // the connection open for INIT etc...
            protocol = handshake.chosenProtocol();
            ctx.writeAndFlush( ctx.alloc().buffer( 4 ).writeInt( protocol.version() ) );

            // If there is more data pending, the client optimistically sent this in its initial payload. It really
            // shouldn't be doing that since it can't know which versions we support, but here we are anyway.
            // Emulate a second call to channelRead, the remaining data in the buffer will be forwarded to the newly
            // selected protocol.
            if ( buffer.readableBytes() > 0 )
            {
                channelRead( ctx, buffer );
            }
            else
            {
                buffer.release();
            }
            return;
        case NO_APPLICABLE_PROTOCOL:
            // No protocol match could be found between the versions suggested by the
            // client and the versions supported by the server. In this case, we have
            // no option but to report a 'zero' version match and close the connection.
            buffer.release();
            ctx.writeAndFlush( wrappedBuffer( new byte[]{0, 0, 0, 0} ) )
                    .sync()
                    .channel()
                    .close();
            return;
        case INSECURE_HANDSHAKE:
            // There has been an attempt to carry out an unencrypted handshake over a
            // connection that requires encryption. No response will be sent and the
            // connection will be closed immediately. Note this is the same action as
            // for INVALID_HANDSHAKE below, so we can just fall through.
        case INVALID_HANDSHAKE:
            // The handshake went horribly wrong for some reason. As above, we'll
            // simply close the connection and say no more about it.
            buffer.release();
            ctx.close();
            return;
        case PARTIAL_HANDSHAKE:
            // Handshake ongoing. More bytes are required before the exchange is
            // complete so we'll simply return and take no specific action.
            return;
        default:
            throw new IllegalStateException( "Unknown handshake outcome: " + outcome );
        }
    }
}
