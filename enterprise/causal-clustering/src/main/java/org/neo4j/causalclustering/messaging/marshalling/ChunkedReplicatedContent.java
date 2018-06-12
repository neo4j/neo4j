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

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ChunkedReplicatedContent implements Marshal, ChunkedInput<ByteBuf>
{

    private static final int DEFAULT_CHUNK_SIZE = 8192;
    private static final int MINIMUM_CHUNK_SIZE = 7;
    private final byte contentType;
    private final ByteBufAwareMarshal byteBufAwareMarshal;
    private final int chunkSize;
    private boolean endOfInput;
    private int progress;

    public ChunkedReplicatedContent( byte contentType, ByteBufAwareMarshal byteBufAwareMarshal, int chunkSize )
    {
        this.byteBufAwareMarshal = byteBufAwareMarshal;
        this.chunkSize = chunkSize;
        if ( chunkSize < MINIMUM_CHUNK_SIZE )
        {
            throw new IllegalArgumentException( "Chunk size must be at least " + MINIMUM_CHUNK_SIZE + " bytes" );
        }
        this.contentType = contentType;
    }

    public ChunkedReplicatedContent( byte contentType, ByteBufAwareMarshal byteBufAwareMarshal )
    {
        this( contentType, byteBufAwareMarshal, DEFAULT_CHUNK_SIZE );
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        channel.put( contentType );
        byteBufAwareMarshal.marshal( channel );
    }

    @Override
    public boolean isEndOfInput()
    {
        return endOfInput;
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public ByteBuf readChunk( ChannelHandlerContext ctx ) throws IOException
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ByteBuf readChunk( ByteBufAllocator allocator ) throws IOException
    {
        if ( isEndOfInput() )
        {
            return null;
        }
        // assume this is the last chunk
        boolean lastChunk = true;
        ByteBuf buffer = allocator.buffer( chunkSize );
        try
        {
            buffer.writeBoolean( lastChunk );
            if ( progress() == 0 )
            {
                // extra metadata on first chunk
                buffer.writeByte( contentType );
                buffer.writeInt( byteBufAwareMarshal.length() );
            }
            if ( !byteBufAwareMarshal.encode( buffer ) )
            {
                this.endOfInput = true;
            }
            if ( isEndOfInput() != lastChunk )
            {
                // status changed after writing to buffer.
                buffer.setBoolean( 0, isEndOfInput() );
            }
            progress += buffer.readableBytes();
            return buffer;
        }
        catch ( Throwable e )
        {
            buffer.release();
            throw e;
        }
    }

    @Override
    public long length()
    {
        return -1;
    }

    @Override
    public long progress()
    {
        return progress;
    }
}
