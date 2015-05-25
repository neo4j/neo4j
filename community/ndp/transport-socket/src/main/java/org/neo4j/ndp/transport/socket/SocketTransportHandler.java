/*
 * Copyright (c) 2002-2015 "Neo Technology,"
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
package org.neo4j.ndp.transport.socket;

import io.netty.buffer.ByteBuf;
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.neo4j.collection.primitive.PrimitiveLongObjectMap;
import org.neo4j.function.Factory;
import org.neo4j.function.Function;

import static io.netty.buffer.Unpooled.wrappedBuffer;
import static org.neo4j.collection.primitive.Primitive.longObjectMap;

/**
 * Handles incoming chunks of data for a given client channel. This initially will negotiate a protocol version to use,
 * and then delegate future messages to the chosen protocol.
 * <p/>
 * This class is stateful, one instance is expected per channel.
 */
public class SocketTransportHandler extends ChannelInboundHandlerAdapter
{
    private final ProtocolChooser protocolChooser;
    private SocketProtocol protocol;

    public SocketTransportHandler( ProtocolChooser protocolChooser )
    {
        this.protocolChooser = protocolChooser;
    }

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        if ( msg instanceof ByteBuf )
        {
            ByteBuf buffer = (ByteBuf) msg;
            if ( protocol == null )
            {
                chooseProtocolVersion( ctx, buffer );
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
        close();
    }

    @Override
    public void handlerRemoved( ChannelHandlerContext ctx ) throws Exception
    {
        close();
    }

    private void close()
    {
        if(protocol != null)
        {
            protocol.close();
            protocol = null;
        }
    }

    private void chooseProtocolVersion( ChannelHandlerContext ctx, ByteBuf buffer ) throws Exception
    {
        switch ( protocolChooser.handleVersionHandshakeChunk( buffer, ctx.channel() ) )
        {
        case PROTOCOL_CHOSEN:
            protocol = protocolChooser.chosenProtocol();
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
            ctx.writeAndFlush( wrappedBuffer( new byte[]{0, 0, 0, 0} ) )
                    .sync()
                    .channel()
                    .close();
            return;
        case PARTIAL_HANDSHAKE:
        }
    }

    public enum HandshakeOutcome
    {
        /** Yay! */
        PROTOCOL_CHOSEN,
        /** Pending more bytes before handshake can complete */
        PARTIAL_HANDSHAKE,
        /** None of the clients suggested protocol versions are available :( */
        NO_APPLICABLE_PROTOCOL
    }

    /**
     * Manages the state for choosing the protocol version to use.
     * The protocol opens with the client sending four suggested protocol versions, in preference order and big endian,
     * each a 4-byte unsigned integer. Since that message could get split up along the way, we first gather the
     * 16 bytes of data we need, and then choose a protocol to use.
     */
    public static class ProtocolChooser
    {
        private final PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> availableVersions;
        private final ByteBuffer suggestedVersions = ByteBuffer.allocateDirect( 4 * 4 ).order( ByteOrder.BIG_ENDIAN );

        private SocketProtocol protocol;

        /**
         * @param availableVersions version -> protocol mapping
         */
        public ProtocolChooser( PrimitiveLongObjectMap<Function<Channel, SocketProtocol>> availableVersions )
        {
            this.availableVersions = availableVersions;
        }

        public HandshakeOutcome handleVersionHandshakeChunk( ByteBuf buffer, Channel ch )
        {
            if ( suggestedVersions.remaining() > buffer.readableBytes() )
            {
                suggestedVersions.limit( suggestedVersions.position() + buffer.readableBytes() );
                buffer.readBytes( suggestedVersions );
                suggestedVersions.limit( suggestedVersions.capacity() );
            }
            else
            {
                buffer.readBytes( suggestedVersions );
            }

            if ( suggestedVersions.remaining() == 0 )
            {
                suggestedVersions.flip();
                for ( int i = 0; i < 4; i++ )
                {
                    long suggestion = suggestedVersions.getInt() & 0xFFFFFFFFL;
                    if ( availableVersions.containsKey( suggestion ) )
                    {
                        protocol = availableVersions.get( suggestion ).apply( ch );
                        return HandshakeOutcome.PROTOCOL_CHOSEN;
                    }
                }

                // None of the suggested protocol versions are available.
                return HandshakeOutcome.NO_APPLICABLE_PROTOCOL;
            }
            return HandshakeOutcome.PARTIAL_HANDSHAKE;
        }

        public SocketProtocol chosenProtocol()
        {
            return protocol;
        }
    }
}
