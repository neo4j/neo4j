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
package org.neo4j.causalclustering.messaging.marshalling.v2.decoding;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.ReplicatedContentChunk;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentSerializer;

public class ReplicatedContentChunkDecoder extends ByteToMessageDecoder
{
    private UnfinishedChunk unfinishedChunk;
    private final CoreReplicatedContentSerializer coreReplicatedContentSerializer = new CoreReplicatedContentSerializer();

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        ReplicatedContentChunk replicatedContentChunk = ReplicatedContentChunk.deSerialize( in );
        if ( unfinishedChunk == null )
        {
            if ( replicatedContentChunk.isLast() )
            {
                out.add( coreReplicatedContentSerializer.read( replicatedContentChunk.contentType(),
                        new NetworkReadableClosableChannelNetty4( replicatedContentChunk.content() ) ) );
            }
            else
            {
                unfinishedChunk = new UnfinishedChunk( replicatedContentChunk );
            }
        }
        else
        {
            unfinishedChunk.consume( replicatedContentChunk );

            if ( replicatedContentChunk.isLast() )
            {
                out.add( coreReplicatedContentSerializer.read( unfinishedChunk.contentType,
                        new NetworkReadableClosableChannelNetty4( unfinishedChunk.content() ) ) );
                unfinishedChunk.content().release();
                unfinishedChunk = null;
            }
        }
    }

    private static class UnfinishedChunk extends DefaultByteBufHolder
    {
        private final byte contentType;

        UnfinishedChunk( ReplicatedContentChunk replicatedContentChunk )
        {
            super( replicatedContentChunk.content().copy() );
            contentType = replicatedContentChunk.contentType();
        }

        void consume( ReplicatedContentChunk replicatedContentChunk )
        {
            if ( replicatedContentChunk.contentType() != contentType )
            {
                throw new IllegalArgumentException( "Wrong content type. Expected " + contentType + " got " + replicatedContentChunk.contentType() );
            }
            content().writeBytes( replicatedContentChunk.content() );
        }
    }
}
