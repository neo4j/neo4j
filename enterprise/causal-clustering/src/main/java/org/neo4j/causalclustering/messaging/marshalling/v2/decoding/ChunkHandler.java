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
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.DefaultByteBufHolder;
import io.netty.util.ReferenceCountUtil;

public class ChunkHandler implements AutoCloseable
{
    private ComposedChunks composedChunks;
    private boolean closed;

    ComposedChunks handle( ByteBuf in )
    {
        if ( closed )
        {
            throw new IllegalStateException( "Cannot handle bytes. Handler has been closed." );
        }
        boolean isLast = in.readBoolean();
        int length = in.readInt();
        // check if first chunk
        if ( composedChunks == null )
        {
            // first chunk contains a content type
            byte contentType = in.readByte();
            composedChunks = new ComposedChunks( contentType, new CompositeByteBuf( in.alloc(), false, Integer.MAX_VALUE ) );
        }
        composedChunks.addComponent( in.readRetainedSlice( length ) );

        return maybeReturn( isLast );
    }

    private ComposedChunks maybeReturn( boolean returnAndReset )
    {
        if ( returnAndReset )
        {
            return getAndReset();
        }
        return null;
    }

    private ComposedChunks getAndReset()
    {
        ComposedChunks composedChunks = this.composedChunks;
        this.composedChunks = null;
        return composedChunks;
    }

    @Override
    public void close()
    {
        closed = true;
        ReferenceCountUtil.release( composedChunks );
    }

    public static class ComposedChunks extends DefaultByteBufHolder
    {
        private final byte contentType;

        private ComposedChunks( byte contentType, CompositeByteBuf compositeByteBuf )
        {
            super( compositeByteBuf );
            this.contentType = contentType;
        }

        private CompositeByteBuf compositeByteBuf()
        {
            return (CompositeByteBuf) content();
        }

        void addComponent( ByteBuf byteBuf )
        {
            compositeByteBuf().addComponent( true, byteBuf );
        }

        public byte contentType()
        {
            return contentType;
        }
    }
}
