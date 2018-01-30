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

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

/**
 * Encodes messages sent by the client.
 */
public class ClientMessageEncoder extends MessageToByteEncoder<ServerMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ServerMessage msg, ByteBuf out ) throws Exception
    {
        msg.dispatch( new Encoder( out ) );
    }

    class Encoder implements ServerMessageHandler
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
        public void handle( ApplicationProtocolRequest applicationProtocolRequest )
        {
            out.writeInt( 1 );
            StringMarshal.marshal( out, applicationProtocolRequest.protocolName() );
            out.writeInt( applicationProtocolRequest.versions().size() );
            for ( int version : applicationProtocolRequest.versions() )
            {
                out.writeInt( version );
            }
        }

        @Override
        public void handle( ModifierProtocolRequest modifierProtocolRequest )
        {
            out.writeInt( 2 );
            // TODO
        }

        @Override
        public void handle( SwitchOverRequest switchOverRequest )
        {
            out.writeInt( 3 );
            StringMarshal.marshal( out, switchOverRequest.protocolName() );
            out.writeInt( switchOverRequest.version() );
        }
    }
}
