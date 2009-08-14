/*
 * Copyright (c) 2002-2009 "Neo Technology,"
 *     Network Engine for Objects in Lund AB [http://neotechnology.com]
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
package org.neo4j.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

class DirectMappedLogBuffer implements LogBuffer
{
    private static final int BUFFER_SIZE = 1024 * 1024 * 2;

    private final FileChannel fileChannel;

    private ByteBuffer byteBuffer = null;
    private long bufferStartPosition;

    DirectMappedLogBuffer( FileChannel fileChannel ) throws IOException
    {
        this.fileChannel = fileChannel;
        bufferStartPosition = fileChannel.position();
        byteBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE );
        getNewMappedBuffer();
    }

    private void getNewMappedBuffer() throws IOException
    {
        byteBuffer.flip();
        bufferStartPosition += fileChannel.write( byteBuffer, 
            bufferStartPosition );
        byteBuffer.clear();
    }

    public LogBuffer put( byte b ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < 1 )
        {
            getNewMappedBuffer();
        }
        byteBuffer.put( b );
        return this;
    }

    public LogBuffer putInt( int i ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < 4 )
        {
            getNewMappedBuffer();
        }
        byteBuffer.putInt( i );
        return this;
    }

    public LogBuffer putLong( long l ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < 8 )
        {
            getNewMappedBuffer();
        }
        byteBuffer.putLong( l );
        return this;
    }

    public LogBuffer put( byte[] bytes ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < bytes.length )
        {
            getNewMappedBuffer();
        }
        byteBuffer.put( bytes );
        return this;
    }

    public LogBuffer put( char[] chars ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < (chars.length * 2) )
        {
            getNewMappedBuffer();
        }
        int oldPos = byteBuffer.position();
        byteBuffer.asCharBuffer().put( chars );
        byteBuffer.position( oldPos + chars.length * 2 );
        return this;
    }

    public void force() throws IOException
    {
        getNewMappedBuffer();
        fileChannel.force( false );
    }

    public long getFileChannelPosition()
    {
        if ( byteBuffer != null )
        {
            return bufferStartPosition + byteBuffer.position();
        }
        return bufferStartPosition;
    }

    public FileChannel getFileChannel()
    {
        return fileChannel;
    }
}