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

import java.io.ByteArrayInputStream;
import java.io.IOException;

import org.neo4j.storageengine.api.WritableChannel;

public class ByteArrayChunkedEncoder implements ChunkedEncoder
{
    private static final int CHUNK_SIZE = 8 * 1024;
    private final byte[] content;
    private final ByteArrayInputStream inputStream;
    private final int chunkSize;

    ByteArrayChunkedEncoder( byte[] content, int chunkSize )
    {
        inputStream = new ByteArrayInputStream( content );
        this.content = content;
        this.chunkSize = chunkSize;
    }

    public ByteArrayChunkedEncoder( byte[] content )
    {
        this( content, CHUNK_SIZE );
    }

    @Override
    public ByteBuf encodeChunk( ByteBufAllocator allocator ) throws IOException
    {
        if ( isEndOfInput() )
        {
            return null;
        }
        int toWrite = Math.min( inputStream.available(), chunkSize );
        ByteBuf buffer = allocator.buffer( toWrite );
        try
        {
            if ( inputStream.available() == content.length )
            {
                buffer.writeInt( content.length );
                toWrite -= 4;
            }
            buffer.writeBytes( inputStream, toWrite );
            return buffer;
        }
        catch ( Throwable t )
        {
            buffer.release();
            throw t;
        }
    }

    @Override
    public boolean isEndOfInput()
    {
        return inputStream.available() == 0;
    }

    @Override
    public void marshal( WritableChannel channel ) throws IOException
    {
        int length = content.length;
        channel.putInt( length );
        channel.put( content, length );
    }
}
