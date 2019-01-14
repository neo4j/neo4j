/*
 * Copyright (c) 2002-2019 "Neo4j,"
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
package org.neo4j.causalclustering.catchup.storecopy;

import io.netty.buffer.ByteBufAllocator;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.stream.ChunkedInput;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;
import java.util.Objects;

import static org.neo4j.causalclustering.catchup.storecopy.FileChunk.MAX_SIZE;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.FINISHED;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.FULL_PENDING;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.LAST_PENDING;
import static org.neo4j.causalclustering.catchup.storecopy.FileSender.State.PRE_INIT;

class FileSender implements ChunkedInput<FileChunk>
{
    private final StoreResource resource;
    private final ByteBuffer byteBuffer;

    private ReadableByteChannel channel;
    private byte[] nextBytes;
    private State state = PRE_INIT;

    FileSender( StoreResource resource )
    {
        this.resource = resource;
        this.byteBuffer = ByteBuffer.allocateDirect( MAX_SIZE );
    }

    @Override
    public boolean isEndOfInput()
    {
        return state == FINISHED;
    }

    @Override
    public void close() throws Exception
    {
        if ( channel != null )
        {
            channel.close();
            channel = null;
        }
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
            channel = resource.open();
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

    @Override
    public boolean equals( Object o )
    {
        if ( this == o )
        {
            return true;
        }
        if ( o == null || getClass() != o.getClass() )
        {
            return false;
        }
        FileSender that = (FileSender) o;
        return Objects.equals( resource, that.resource );
    }

    @Override
    public int hashCode()
    {
        return Objects.hash( resource );
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
        while ( byteBuffer.hasRemaining() );

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
