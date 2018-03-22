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
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.function.BiConsumer;

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

/**
 * Encodes messages sent by the server.
 */
public class ServerMessageEncoder extends MessageToByteEncoder<ClientMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ClientMessage msg, ByteBuf out )
    {
        msg.dispatch( new Encoder( out ) );
    }

    class Encoder implements ClientMessageHandler
    {
        private final ByteBuf out;

        Encoder( ByteBuf out )
        {
            this.out = out;
        }

        @Override
        public void handle( InitialMagicMessage magicMessage )
        {
            out.writeInt( InitialMagicMessage.MESSAGE_CODE );
            StringMarshal.marshal( out, magicMessage.magic() );
        }

        @Override
        public void handle( ApplicationProtocolResponse applicationProtocolResponse )
        {
            out.writeInt( 0 );
            encodeProtocolResponse( applicationProtocolResponse, ByteBuf::writeInt );
        }

        @Override
        public void handle( ModifierProtocolResponse modifierProtocolResponse )
        {
            out.writeInt( 1 );
            encodeProtocolResponse( modifierProtocolResponse, StringMarshal::marshal );
        }

        @Override
        public void handle( SwitchOverResponse switchOverResponse )
        {
            out.writeInt( 2 );
            out.writeInt( switchOverResponse.status().codeValue() );
        }

        private <U extends Comparable<U>> void encodeProtocolResponse( BaseProtocolResponse<U> protocolResponse, BiConsumer<ByteBuf,U> writer )
        {
            out.writeInt( protocolResponse.statusCode().codeValue() );
            StringMarshal.marshal( out, protocolResponse.protocolName() );
            writer.accept( out, protocolResponse.version() );
        }
    }
}
