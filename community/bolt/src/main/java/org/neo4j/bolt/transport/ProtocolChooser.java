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
import io.netty.channel.Channel;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Map;
import java.util.function.BiFunction;

/**
 * Manages the state for choosing the protocol version to use.
 * The protocol opens with the client sending four bytes (0x6060 B017) followed by four suggested protocol
 * versions in preference order. All bytes are expected to be big endian, and each of the suggested protocols are
 * 4-byte unsigned integers. Since that message could get split up along the way, we first gather the
 * 20 bytes of data we need, and then choose a protocol to use.
 * <p>
 * This class is stateful, one instance is expected per channel.
 */
public class ProtocolChooser
{
    public static final int BOLT_MAGIC_PREAMBLE = 0x6060B017;

    private final Map<Long,BiFunction<Channel,Boolean,BoltProtocol>> availableVersions;
    private final boolean encryptionRequired;
    private final boolean isEncrypted;
    private final ByteBuffer handShake = ByteBuffer.allocate( 5 * 4 ).order( ByteOrder.BIG_ENDIAN );

    private BoltProtocol protocol;

    /**
     * @param availableVersions version -> protocol mapping
     * @param encryptionRequired whether or not the server allows only encrypted connections
     * @param isEncrypted whether of not this connection is encrypted
     */
    public ProtocolChooser( Map<Long,BiFunction<Channel,Boolean,BoltProtocol>> availableVersions,
            boolean encryptionRequired, boolean isEncrypted )
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
            else
            {
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
