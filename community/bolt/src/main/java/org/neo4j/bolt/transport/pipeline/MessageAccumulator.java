/*
 * Copyright (c) 2002-2019 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.bolt.transport.pipeline;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.ByteToMessageDecoder;
import io.netty.handler.codec.DecoderException;

import java.util.List;

public class MessageAccumulator extends ByteToMessageDecoder
{
    private boolean readMessageBoundary;

    @Override
    public void channelRead( ChannelHandlerContext ctx, Object msg ) throws Exception
    {
        ByteBuf buf = (ByteBuf) msg;

        if ( buf.readableBytes() == 0 )
        {
            assertNonEmptyMessage();

            readMessageBoundary = true;
        }

        super.channelRead( ctx, msg );
    }

    @Override
    protected void decode( ChannelHandlerContext channelHandlerContext, ByteBuf in, List<Object> out )
    {
        if ( readMessageBoundary )
        {
            // now we have a complete message in the input buffer

            // increment ref count of the buffer and create it's duplicate that shares the content
            // duplicate will be the output of this decoded and input for the next one
            ByteBuf messageBuf = in.retainedDuplicate();

            // signal that whole message was read by making input buffer seem like it was fully read/consumed
            in.readerIndex( in.readableBytes() );

            // pass the full message to the next handler in the pipeline
            out.add( messageBuf );

            readMessageBoundary = false;
        }
    }

    private void assertNonEmptyMessage()
    {
        if ( actualReadableBytes() == 0 )
        {
            throw new DecoderException( "Message boundary received when there's nothing to decode." );
        }
    }
}
