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
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

/**
 * Decodes messages received by the server.
 */
public class ServerMessageDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        int messageCode = in.readInt();

        switch ( messageCode )
        {
        case InitialMagicMessage.MESSAGE_CODE:
        {
            String magic = StringMarshal.unmarshal( in );
            out.add( new InitialMagicMessage( magic ) );
            return;
        }
        case 1:
        {
            String protocolName = StringMarshal.unmarshal( in );
            int versionArrayLength = in.readInt();

            Set<Integer> versions = Stream.generate( in::readInt ).limit( versionArrayLength ).collect( Collectors.toSet() );

            out.add( new ApplicationProtocolRequest( protocolName, versions ) );
            return;
        }
        case 2:
            // TODO
            out.add( new ModifierProtocolRequest() );
            return;
        case 3:
        {
            String protocolName = StringMarshal.unmarshal( in );
            int version = in.readInt();
            out.add( new SwitchOverRequest( protocolName, version ) );
            return;
        }
        default:
            // TODO
            return;
        }
    }
}
