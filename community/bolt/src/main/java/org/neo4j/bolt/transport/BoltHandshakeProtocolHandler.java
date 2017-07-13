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
import org.neo4j.bolt.BoltChannel;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.function.Function;

import static java.lang.String.format;

/**
 * Manages the state for choosing the protocol version to use.
 * The protocol opens with the client sending four bytes (0x6060 B017) followed by four suggested protocol
 * versions in preference order. All bytes are expected to be big endian, and each of the suggested protocols are
 * 4-byte unsigned integers. Since that message could get split up along the way, we first gather the
 * 20 bytes of data we need, and then choose a protocol to use.
 * <p>
 * This class is stateful, one instance is expected per channel.
 */
public class BoltHandshakeProtocolHandler
{
    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;

    private final Map<Long,Function<BoltChannel, BoltMessagingProtocolHandler>> protocolHandlers;
    private final boolean encryptionRequired;
    private final boolean isEncrypted;
    private final ByteBuffer handshakeBuffer = ByteBuffer.allocate( 5 * 4 ).order( ByteOrder.BIG_ENDIAN );

    private BoltMessagingProtocolHandler protocol;

    /**
     * @param protocolHandlers version -> protocol mapping
     * @param encryptionRequired whether or not the server allows only encrypted connections
     * @param isEncrypted whether of not this connection is encrypted
     */
    public BoltHandshakeProtocolHandler( Map<Long,Function<BoltChannel, BoltMessagingProtocolHandler>> protocolHandlers,
                                         boolean encryptionRequired, boolean isEncrypted )
    {
        this.protocolHandlers = protocolHandlers;
        this.encryptionRequired = encryptionRequired;
        this.isEncrypted = isEncrypted;
    }

    public HandshakeOutcome perform( BoltChannel boltChannel, ByteBuf buffer )
    {
        if ( encryptionRequired && !isEncrypted )
        {
            boltChannel.log().serverError( "HANDSHAKE", "Insecure handshake" );
            return HandshakeOutcome.INSECURE_HANDSHAKE;
        }
        else if ( handshakeBuffer.remaining() > buffer.readableBytes() )
        {
            handshakeBuffer.limit( handshakeBuffer.position() + buffer.readableBytes() );
            buffer.readBytes( handshakeBuffer );
            handshakeBuffer.limit( handshakeBuffer.capacity() );
        }
        else
        {
            buffer.readBytes(handshakeBuffer);
        }

        if ( handshakeBuffer.remaining() == 0 )
        {
            handshakeBuffer.flip();
            // Verify that the handshake starts with a Bolt-shaped preamble.
            if ( handshakeBuffer.getInt() != BOLT_MAGIC_PREAMBLE )
            {
                boltChannel.log().clientError( "HANDSHAKE", format( "0x%08X", handshakeBuffer.getInt() ),
                                               "Invalid Bolt signature" );
                return HandshakeOutcome.INVALID_HANDSHAKE;
            }
            else
            {
                boltChannel.log().clientEvent( "HANDSHAKE", format( "0x%08X", BOLT_MAGIC_PREAMBLE ) );
                for ( int i = 0; i < 4; i++ )
                {
                    long suggestion = handshakeBuffer.getInt() & 0xFFFFFFFFL;
                    if ( protocolHandlers.containsKey( suggestion ) )
                    {
                        protocol = protocolHandlers.get( suggestion ).apply( boltChannel );
                        boltChannel.log().serverEvent( "HANDSHAKE", format( "0x%02X", protocol.version() ) );
                        return HandshakeOutcome.PROTOCOL_CHOSEN;
                    }
                }
            }

            // None of the suggested protocol versions are available.
            boltChannel.log().serverError( "HANDSHAKE", "No applicable protocol" );
            return HandshakeOutcome.NO_APPLICABLE_PROTOCOL;
        }
        boltChannel.log().serverError( "HANDSHAKE", "Partial handshake" );
        return HandshakeOutcome.PARTIAL_HANDSHAKE;
    }

    public BoltMessagingProtocolHandler chosenProtocol()
    {
        return protocol;
    }
}
