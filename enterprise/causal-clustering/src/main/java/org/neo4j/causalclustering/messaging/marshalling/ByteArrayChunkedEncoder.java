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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import static java.util.Objects.requireNonNull;
import static org.neo4j.util.Preconditions.requireNonNegative;
import static org.neo4j.util.Preconditions.requirePositive;

public class ByteArrayChunkedEncoder implements ChunkedInput<ByteBuf>
{
    private static final int DEFAULT_CHUNK_SIZE = 32 * 1024;
    private final byte[] content;
    private int chunkSize;
    private int pos;
    private boolean hasRead;

    ByteArrayChunkedEncoder( byte[] content, int chunkSize )
    {
        requireNonNull( content, "content cannot be null" );
        requireNonNegative( content.length );
        requirePositive( chunkSize );
        this.content = content;
        this.chunkSize = chunkSize;
    }

    public ByteArrayChunkedEncoder( byte[] content )
    {
        this( content, DEFAULT_CHUNK_SIZE );
    }

    private int available()
    {
        return content.length - pos;
    }

    @Override
    public boolean isEndOfInput()
    {
        return pos == content.length && hasRead;
    }

    @Override
    public void close()
    {
        pos = content.length;
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx )
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator )
    {
        hasRead = true;
        if ( isEndOfInput() )
        {
            return null;
        }
        int toWrite = Math.min( available(), chunkSize );
        ByteBuf buffer = allocator.buffer( toWrite );
        try
        {
            buffer.writeBytes( content, pos, toWrite );
            pos += toWrite;
            return buffer;
        }
        catch ( Throwable t )
        {
            buffer.release();
            throw t;
        }
    }

    @Override
    public long length()
    {
        return content.length;
    }

    @Override
    public long progress()
    {
        return pos;
    }
}
