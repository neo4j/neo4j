/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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
import io.netty.channel.Channel;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import org.neo4j.logging.Log;
import org.neo4j.logging.LogProvider;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.function.BiFunction;

import static io.netty.buffer.Unpooled.wrappedBuffer;

/**
 * Handles incoming chunks of data for a given client channel. This initially will negotiate a protocol version to use,
 * and then delegate future messages to the chosen protocol.
 * <p/>
 * This class is stateful, one instance is expected per channel.
 */
public class SocketTransportHandler extends ChannelInboundHandlerAdapter
{
    private final ProtocolChooser protocolChooser;
    private final Log log;

    private BoltProtocol protocol;

    public SocketTransportHandler( ProtocolChooser protocolChooser, LogProvider logging )
    {
        this.protocolChooser = protocolChooser;
        this.log = logging.getLog( getClass() );
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

    @Override
    public void exceptionCaught( ChannelHandlerContext ctx, Throwable cause ) throws Exception
    {
        log.error( "Fatal error occurred when handling a client connection: " + cause.getMessage(), cause );
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
        HandshakeOutcome outcome = protocolChooser.handleVersionHandshakeChunk( buffer, ctx.channel() );
        switch ( outcome )
        {
        case PROTOCOL_CHOSEN:
            // A protocol version has been successfully agreed upon, therefore we can
            // reply positively with four bytes reflecting this selection and leave
            // the connection open for INIT etc...
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

    public enum HandshakeOutcome
    {
        /** Yay! */
        PROTOCOL_CHOSEN,
        /** Pending more bytes before handshake can complete */
        PARTIAL_HANDSHAKE,
        /** the client sent an invalid handshake */
        INVALID_HANDSHAKE,
        /** None of the clients suggested protocol versions are available :( */
        NO_APPLICABLE_PROTOCOL,
        /** Encryption is required but the connection is not encrypted */
        INSECURE_HANDSHAKE
    }

    /**
     * Manages the state for choosing the protocol version to use.
     * The protocol opens with the client sending four bytes (0x6060 B017) followed by four suggested protocol
     * versions in preference order. All bytes are expected to be big endian, and each of the suggested protocols are
     * 4-byte unsigned integers. Since that message could get split up along the way, we first gather the
     * 20 bytes of data we need, and then choose a protocol to use.
     */
    public static class ProtocolChooser
    {
        private final Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> availableVersions;
        private final boolean encryptionRequired;
        private final boolean isEncrypted;
        private final ByteBuffer handShake = ByteBuffer.allocateDirect( 5 * 4 ).order( ByteOrder.BIG_ENDIAN );
        public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;

        private BoltProtocol protocol;

        /**
         * @param availableVersions version -> protocol mapping
         * @param encryptionRequired whether or not the server allows only encrypted connections
         * @param isEncrypted whether of not this connection is encrypted
         */
        public ProtocolChooser( Map<Long, BiFunction<Channel, Boolean, BoltProtocol>> availableVersions, boolean encryptionRequired, boolean isEncrypted )
        {
            this.availableVersions = availableVersions;
            this.encryptionRequired = encryptionRequired;
            this.isEncrypted = isEncrypted;
        }

        public HandshakeOutcome handleVersionHandshakeChunk( ByteBuf buffer, Channel ch )
        {
            if ( encryptionRequired && !isEncrypted )
            {
                return HandshakeOutcome.INSECURE_HANDSHAKE;
            }
            else if ( handShake.remaining() > buffer.readableBytes() )
            {
                handShake.limit( handShake.position() + buffer.readableBytes() );
                buffer.readBytes( handShake );
                handShake.limit( handShake.capacity() );
            }
            else
            {
                buffer.readBytes( handShake );
            }

            if ( handShake.remaining() == 0 )
            {
                handShake.flip();
                // Verify that the handshake starts with a Bolt-shaped preamble.
                if ( handShake.getInt() != BOLT_MAGIC_PREAMBLE )
                {
                    return HandshakeOutcome.INVALID_HANDSHAKE;
                }
                else {
                    for ( int i = 0; i < 4; i++ )
                    {
                        long suggestion = handShake.getInt() & 0xFFFFFFFFL;
                        if ( availableVersions.containsKey( suggestion ) )
                        {
                            protocol = availableVersions.get( suggestion ).apply( ch, isEncrypted );
                            return HandshakeOutcome.PROTOCOL_CHOSEN;
                        }
                    }
                }

                // None of the suggested protocol versions are available.
                return HandshakeOutcome.NO_APPLICABLE_PROTOCOL;
            }
            return HandshakeOutcome.PARTIAL_HANDSHAKE;
        }

        public BoltProtocol chosenProtocol()
        {
            return protocol;
        }
    }
}
