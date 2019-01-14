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
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;
import java.util.Optional;
import java.util.function.Function;

import org.neo4j.causalclustering.messaging.marshalling.StringMarshal;

/**
 * Decodes messages received by the client.
 */
public class ClientMessageDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws ClientHandshakeException
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
        case 0:
        {
            ApplicationProtocolResponse applicationProtocolResponse = decodeProtocolResponse( ApplicationProtocolResponse::new, ByteBuf::readInt, in );

            out.add( applicationProtocolResponse );
            return;
        }
        case 1:
        {
            ModifierProtocolResponse modifierProtocolResponse = decodeProtocolResponse( ModifierProtocolResponse::new, StringMarshal::unmarshal, in );

            out.add( modifierProtocolResponse );
            return;
        }
        case 2:
        {
            int statusCodeValue = in.readInt();
            Optional<StatusCode> statusCode = StatusCode.fromCodeValue( statusCodeValue );
            if ( statusCode.isPresent() )
            {
                out.add( new SwitchOverResponse( statusCode.get() ) );
            }
            else
            {
                // TODO
            }
            return;
        }
        default:
            // TODO
            return;
        }
    }

    private <U extends Comparable<U>,T extends BaseProtocolResponse<U>> T decodeProtocolResponse( TriFunction<StatusCode,String,U,T> constructor,
            Function<ByteBuf,U> reader, ByteBuf in )
            throws ClientHandshakeException
    {
        int statusCodeValue = in.readInt();
        String identifier = StringMarshal.unmarshal( in );
        U version = reader.apply( in );

        Optional<StatusCode> statusCode = StatusCode.fromCodeValue( statusCodeValue );

        return statusCode
                .map( status -> constructor.apply( status, identifier, version ) )
                .orElseThrow( () -> new ClientHandshakeException(
                        String.format( "Unknown status code %s for protocol %s version %d", statusCodeValue, identifier, version ) ) );
    }

    @FunctionalInterface
    interface TriFunction<T,U,V,W>
    {
        W apply( T t, U u, V v );
    }
}
