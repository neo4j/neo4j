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
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;

import java.util.List;

import org.neo4j.causalclustering.messaging.NetworkReadableClosableChannelNetty4;
import org.neo4j.causalclustering.messaging.marshalling.CoreReplicatedContentMarshal;

public class ReplicatedContentChunkDecoder extends ByteToMessageDecoder implements AutoCloseable
{
    private final CoreReplicatedContentMarshal contentMarshal = new CoreReplicatedContentMarshal();

    private UnfinishedChunk unfinishedChunk;

    @Override
    protected void decode( ChannelHandlerContext ctx, ByteBuf in, List<Object> out ) throws Exception
    {
        try
        {
            boolean isLast = in.readBoolean();
            if ( unfinishedChunk == null )
            {
                byte contentType = in.readByte();
                int allocationSize = in.readInt();
                if ( isLast )
                {

                    out.add( contentMarshal.read( contentType,
                            new NetworkReadableClosableChannelNetty4( in.readSlice( in.readableBytes() ) ) ) );
                }
                else
                {
                    ByteBuf replicatedContentBuffer;
                    if ( allocationSize == -1 )
                    {
                        replicatedContentBuffer = in.copy();
                    }
                    else
                    {
                        replicatedContentBuffer = ctx.alloc().buffer( allocationSize, allocationSize );
                    }
                    unfinishedChunk = new UnfinishedChunk( contentType, replicatedContentBuffer );
                    unfinishedChunk.consume( in );
                }
            }
            else
            {
                unfinishedChunk.consume( in );

                if ( isLast )
                {
                    out.add( contentMarshal.read( unfinishedChunk.contentType,
                            new NetworkReadableClosableChannelNetty4( unfinishedChunk.content() ) ) );
                    unfinishedChunk.release();
                    unfinishedChunk = null;
                }
            }
        }
        catch ( Throwable e )
        {
            release();
            throw e;
        }
    }

    private void release()
    {
        if ( unfinishedChunk != null )
        {
            unfinishedChunk.release();
            unfinishedChunk = null;
        }
    }

    @Override
    public void close()
    {
        release();
    }

    private static class UnfinishedChunk extends DefaultByteBufHolder
    {
        private final byte contentType;

        UnfinishedChunk( byte contentType, ByteBuf byteBuf )
        {
            super( byteBuf );
            this.contentType = contentType;
        }

        void consume( ByteBuf replicatedContentChunk )
        {
            content().writeBytes( replicatedContentChunk );
        }
    }
}
