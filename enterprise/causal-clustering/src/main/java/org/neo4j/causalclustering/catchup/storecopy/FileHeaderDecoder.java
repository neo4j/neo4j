/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j Enterprise Edition. The included source
 * code can be redistributed and/or modified under the terms of the
 * Neo4j Sweden Software License, as found in the associated LICENSE.txt
 * file.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * Neo4j Sweden Software License for more details.
 */
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.string.UTF8;

public class FileHeaderDecoder extends ByteToMessageDecoder
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out )
    {
        int length = msg.readInt();
        byte[] bytes = new byte[length];
        msg.readBytes( bytes );
        String name = UTF8.decode( bytes );
        int requiredAlignment = msg.readInt();
        out.add( new FileHeader( name, requiredAlignment ) );
    }
}
