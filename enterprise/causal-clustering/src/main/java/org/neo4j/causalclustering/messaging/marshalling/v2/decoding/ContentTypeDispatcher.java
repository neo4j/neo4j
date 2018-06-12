/*
 * Copyright (c) 2002-2018 "Neo4j,"
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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.catchup.Protocol;
import org.neo4j.causalclustering.messaging.marshalling.v2.ContentType;

public class ContentTypeDispatcher extends ByteToMessageDecoder
{
    private final Protocol<ContentType> contentTypeProtocol;

    public ContentTypeDispatcher( Protocol<ContentType> contentTypeProtocol )
    {
        this.contentTypeProtocol = contentTypeProtocol;
    }

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out )
    {
        if ( contentTypeProtocol.isExpecting( ContentType.ContentType ) )
        {
            byte messageCode = in.readByte();
            ContentType contentType = getContentType( messageCode );
            contentTypeProtocol.expect( contentType );
        }
        else
        {
            ctx.fireChannelRead( in.retain() );
        }
    }

    private ContentType getContentType( byte messageCode )
    {
        for ( ContentType contentType : ContentType.values() )
        {
            if ( contentType.get() == messageCode )
            {
                return contentType;
            }
        }
        throw new IllegalArgumentException( "Illegal inbound. Could not find a ContentType with value " + messageCode );
    }
}
