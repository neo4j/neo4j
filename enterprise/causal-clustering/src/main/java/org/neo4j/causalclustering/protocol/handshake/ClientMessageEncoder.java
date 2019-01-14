/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * GNU AFFERO GENERAL PUBLIC LICENSE Version 3
 * (http://www.fsf.org/licensing/licenses/agpl-3.0.html) with the
 * Commons Clause, as found in the associated LICENSE.txt file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * Neo4j object code can be licensed independently from the source
 * under separate terms from the AGPL. Inquiries can be directed to:
 * licensing@neo4j.com
 *
 * More information is also available at:
 * https://neo4j.com/licensing/
 */
package org.neo4j.causalclustering.protocol.handshake;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.MessageToByteEncoder;

import java.util.function.BiConsumer;

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

/**
 * Encodes messages sent by the client.
 */
public class ClientMessageEncoder extends MessageToByteEncoder<ServerMessage>
{
    @Override
    protected void encode( ChannelHandlerContext ctx, ServerMessage msg, ByteBuf out )
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
            encodeProtocolRequest( applicationProtocolRequest, ByteBuf::writeInt );
        }

        @Override
        public void handle( ModifierProtocolRequest modifierProtocolRequest )
        {
            out.writeInt( 2 );
            encodeProtocolRequest( modifierProtocolRequest, StringMarshal::marshal );
        }

        @Override
        public void handle( SwitchOverRequest switchOverRequest )
        {
            out.writeInt( 3 );
            StringMarshal.marshal( out, switchOverRequest.protocolName() );
            out.writeInt( switchOverRequest.version() );
            out.writeInt( switchOverRequest.modifierProtocols().size() );
            switchOverRequest.modifierProtocols().forEach( pair ->
                    {
                        StringMarshal.marshal( out, pair.first() );
                        StringMarshal.marshal( out, pair.other() );
                    }
            );
        }

        private <U extends Comparable<U>> void encodeProtocolRequest( BaseProtocolRequest<U> protocolRequest, BiConsumer<ByteBuf,U> writer )
        {
            StringMarshal.marshal( out, protocolRequest.protocolName() );
            out.writeInt( protocolRequest.versions().size() );
            protocolRequest.versions().forEach( version -> writer.accept( out, version) );
        }
    }
}
