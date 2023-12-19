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
import io.netty.handler.codec.MessageToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;

/**
 * This class does not consume bytes during the decode method. Instead, it puts a {@link FileChunk} object with
 * a reference to the buffer, to be consumed later. This is the reason it does not extend
 * {@link io.netty.handler.codec.ByteToMessageDecoder}, since that class fails if an object is added in the out
 * list but no bytes have been consumed.
 */
public class FileChunkDecoder extends MessageToMessageDecoder<ByteBuf>
{
    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf msg, List<Object> out ) throws Exception
    {
        out.add( FileChunk.marshal().unmarshal( new NetworkReadableClosableChannelNetty4( msg ) ) );
    }
}
