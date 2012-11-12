/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
package org.neo4j.com;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.jboss.netty.buffer.ChannelBuffer;

public class BlockLogReader implements ReadableByteChannel
{
    private final ChannelBuffer source;
    private final byte[] byteArray = new byte[BlockLogBuffer.MAX_SIZE];
    private final ByteBuffer byteBuffer = ByteBuffer.wrap( byteArray );
    private boolean moreBlocks;
    
    public BlockLogReader( ChannelBuffer source )
    {
        this.source = source;
        readNextBlock();
    }
    
    private void readNextBlock()
    {
        int blockSize = source.readUnsignedByte();
        byteBuffer.clear();
        moreBlocks = blockSize == BlockLogBuffer.FULL_BLOCK_AND_MORE;
        int limit = moreBlocks ? BlockLogBuffer.DATA_SIZE : blockSize;
        byteBuffer.limit( limit );
        source.readBytes( byteBuffer );
        byteBuffer.flip();
    }

    public boolean isOpen()
    {
        return true;
    }

    public void close() throws IOException
    {
        // This is to make sure that reader index in the ChannelBuffer is left
        // in the right place even if this reader wasn't completely read through.
        readToTheEnd();
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        int bytesWanted = dst.limit();
        int bytesRead = 0;
        while ( bytesWanted > 0 )
        {
            int bytesReadThisTime = readAsMuchAsPossible( dst, bytesWanted );
            if ( bytesReadThisTime == 0 )
            {
                break;
            }
            bytesRead += bytesReadThisTime;
            bytesWanted -= bytesReadThisTime;
        }
        return bytesRead == 0 && !moreBlocks ? -1 : bytesRead;
    }

    private int readAsMuchAsPossible( ByteBuffer dst, int maxBytesWanted )
    {
        if ( byteBuffer.remaining() == 0 && moreBlocks )
        {
            readNextBlock();
        }
        
        int bytesToRead = Math.min( maxBytesWanted, byteBuffer.remaining() );
        dst.put( byteArray, byteBuffer.position(), bytesToRead );
        byteBuffer.position( byteBuffer.position()+bytesToRead );
        return bytesToRead;
    }
    
    private void readToTheEnd()
    {
        while ( moreBlocks )
        {
            readNextBlock();
        }
    }
}
