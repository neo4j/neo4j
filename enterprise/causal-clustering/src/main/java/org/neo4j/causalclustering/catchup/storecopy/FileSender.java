/*
 * Copyright (c) 2002-2016 "Neo Technology,"
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

import io.netty.channel.ChannelHandlerContext;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.causalclustering.catchup.ResponseMessageType;
import org.neo4j.io.fs.FileSystemAbstraction;
import org.neo4j.io.fs.StoreChannel;

class FileSender
{
    private FileSystemAbstraction fs;
    private final ChannelHandlerContext ctx;

    FileSender( FileSystemAbstraction fs, ChannelHandlerContext ctx )
    {
        this.fs = fs;
        this.ctx = ctx;
    }

    void sendFile( File file ) throws IOException
    {
        ctx.writeAndFlush( ResponseMessageType.FILE );
        ctx.writeAndFlush( new FileHeader( file.getName() ) );
        ByteBuffer buffer = ByteBuffer.allocateDirect( FileChunk.MAX_SIZE );
        byte[] bytes = null;
        try ( StoreChannel channel = fs.open( file, "r" ) )
        {
            boolean fileEmpty = true;
            while ( channel.read( buffer ) != -1 )
            {
                fileEmpty = false;
                // send out the previous chunk if there is any
                perhapsWriteAndFlush( ctx, bytes, false );
                // let's set the bytes to null in case we are not able to create another full array and overwrite it
                bytes = null;

                // if the buffer is full...
                if ( !buffer.hasRemaining() )
                {
                    // create the next chunk
                    bytes = createByteArray( buffer );
                }
            }

            // if there are some more bytes left or the file is empty...
            if ( buffer.position() > 0 || fileEmpty )
            {
                // send out the previous chunk if there is any
                perhapsWriteAndFlush( ctx, bytes, false );
                // and create a new one for the remaining bytes (or an empty byte array in case the file is empty)
                bytes = createByteArray( buffer );
            }

            // send out whatever is left to send as the last chunk
            perhapsWriteAndFlush( ctx, bytes, true );
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

    private void perhapsWriteAndFlush( ChannelHandlerContext ctx, byte[] bytes, boolean last )
    {
        if ( bytes != null )
        {
            ctx.writeAndFlush( FileChunk.create( bytes, last ) );
        }
    }
}
