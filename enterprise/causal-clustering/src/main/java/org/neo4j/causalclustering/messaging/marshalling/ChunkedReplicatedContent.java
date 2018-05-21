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
package org.neo4j.causalclustering.messaging.marshalling;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ChunkedReplicatedContent implements Marshal, ChunkedInput<ReplicatedContentChunk>
{

    private static final int DEFAULT_CHUNK_SIZE = 8192;
    private final byte contentType;
    private final Serializer serializer;
    private final int chunkSize;
    private boolean lastByteWasWritten;
    private int progress;

    public ChunkedReplicatedContent( byte contentType, Serializer serializer, int chunkSize )
    {
        this.serializer = serializer;
        this.chunkSize = chunkSize;
        if ( chunkSize < 4 )
        {
            throw new IllegalArgumentException( "Chunk size must be at least 4 bytes" );
        }
        this.contentType = contentType;
    }

    public ChunkedReplicatedContent( byte contentType, Serializer serializer )
    {
        this( contentType, serializer, DEFAULT_CHUNK_SIZE );
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        channel.put( contentType );
        serializer.marshal( channel );
    }

    @Override
    public boolean isEndOfInput()
    {
        return lastByteWasWritten;
    }

    @Override
    public void close()
    {
        // do nothing
    }

    @Override
    public ReplicatedContentChunk readChunk( ChannelHandlerContext ctx ) throws IOException
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public ReplicatedContentChunk readChunk( ByteBufAllocator allocator ) throws IOException
    {
        if ( isEndOfInput() )
        {
            return null;
        }
        ByteBuf buffer = allocator.buffer( chunkSize );
        if ( !serializer.encode( buffer ) )
        {
            lastByteWasWritten = true;
        }
        progress += buffer.readableBytes();
        return new ReplicatedContentChunk( contentType, isEndOfInput(), buffer );
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
