/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import static org.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_SIZE;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.FINISHED;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.FULL_PENDING;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.LAST_PENDING;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.PRE_INIT;

class FileSender implements ChunkedInput<FileChunk>
{
    private final ReadableByteChannel channel;
    private final ByteBuffer byteBuffer;

    private byte[] nextBytes;
    private State state = PRE_INIT;

    FileSender( ReadableByteChannel channel ) throws IOException
    {
        this.channel = channel;
        this.byteBuffer = ByteBuffer.allocateDirect( MAX_SIZE );
    }

    @Override
    public boolean isEndOfInput() throws Exception
    {
        return state == FINISHED;
    }

    @Override
    public void close() throws Exception
    {
        channel.close();
    }

    @Override
    public FileChunk readChunk( ByteBufAllocator allocator ) throws Exception
    {
        if ( state == FINISHED )
        {
            return null;
        }
        else if ( state == PRE_INIT )
        {
            nextBytes = prefetch();
            if ( nextBytes == null )
            {
                state = FINISHED;
                return FileChunk.create( new byte[0], true );
            }
            else
            {
                state = nextBytes.length < MAX_SIZE ? LAST_PENDING : FULL_PENDING;
            }
        }

        if ( state == FULL_PENDING )
        {
            byte[] toSend = nextBytes;
            nextBytes = prefetch();
            if ( nextBytes == null )
            {
                state = FINISHED;
                return FileChunk.create( toSend, true );
            }
            else if ( nextBytes.length < MAX_SIZE )
            {
                state = LAST_PENDING;
                return FileChunk.create( toSend, false );
            }
            else
            {
                return FileChunk.create( toSend, false );
            }
        }
        else if ( state == LAST_PENDING )
        {
            state = FINISHED;
            return FileChunk.create( nextBytes, true );
        }
        else
        {
            throw new IllegalStateException();
        }
    }

    @Override
    public FileChunk readChunk( ChannelHandlerContext ctx ) throws Exception
    {
        return readChunk( ctx.alloc() );
    }

    @Override
    public long length()
    {
        return -1;
    }

    @Override
    public long progress()
    {
        return 0;
    }

    private byte[] prefetch() throws IOException
    {
        do
        {
            int bytesRead = channel.read( byteBuffer );
            if ( bytesRead == -1 )
            {
                break;
            }
        }
        while ( byteBuffer.remaining() > 0 );

        if ( byteBuffer.position() > 0 )
        {
            return createByteArray( byteBuffer );
        }
        else
        {
            return null;
        }
    }

    private byte[] createByteArray( ByteBuffer buffer )
    {
        buffer.flip();
        byte[] bytes = new byte[buffer.limit()];
        buffer.get( bytes );
        buffer.clear();
        return bytes;
    }

    enum State
    {
        PRE_INIT,
        FULL_PENDING,
        LAST_PENDING,
        FINISHED
    }
}
