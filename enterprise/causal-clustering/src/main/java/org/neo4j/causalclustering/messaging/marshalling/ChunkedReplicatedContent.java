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
import io.netty.buffer.CompositeByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ChunkedReplicatedContent implements Marshal, ChunkedInput<ByteBuf>
{
    private static final int METADATA_SIZE = Integer.BYTES + 1;

    private final byte contentType;
    private final ChunkedEncoder byteBufAwareMarshal;
    private boolean endOfInput;
    private int progress;

    ChunkedReplicatedContent( byte contentType, ChunkedEncoder byteBufAwareMarshal )
    {
        this.byteBufAwareMarshal = byteBufAwareMarshal;
        this.contentType = contentType;
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
        if ( endOfInput )
        {
            return null;
        }
        ByteBuf data = byteBufAwareMarshal.encodeChunk( allocator );
        if ( data == null )
        {
            return null;
        }
        endOfInput = byteBufAwareMarshal.isEndOfInput();
        CompositeByteBuf allData = new CompositeByteBuf( allocator, false, 2 );
        allData.addComponent( true, data );
        try
        {
            boolean isFirstChunk = progress() == 0;
            allData.addComponent( true, 0, writeMetadata( isFirstChunk, allocator, data ) );
            progress += allData.readableBytes();
            assert progress > 0; // logic relies on this
            return allData;
        }
        catch ( Throwable e )
        {
            allData.release();
            throw e;
        }
    }

    private int metadataSize( boolean isFirstChunk )
    {
        return METADATA_SIZE + (isFirstChunk ? 1 : 0);
    }

    private ByteBuf writeMetadata( boolean isFirstChunk, ByteBufAllocator allocator, ByteBuf data )
    {
        int length = data.writerIndex();
        int capacity = metadataSize( isFirstChunk );
        ByteBuf metaData = allocator.buffer( capacity, capacity );
        metaData.writeBoolean( byteBufAwareMarshal.isEndOfInput() );
        metaData.writeInt( length );
        if ( isFirstChunk )
        {
            metaData.writeByte( contentType );
        }
        return metaData;
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
