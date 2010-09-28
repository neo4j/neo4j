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
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

import org.neo4j.kernel.impl.util.BufferNumberPutter;

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
    }

    private void getNewDirectBuffer() throws IOException
    {
        byteBuffer.flip();
        bufferStartPosition += fileChannel.write( byteBuffer, 
            bufferStartPosition );
        byteBuffer.clear();
    }

    private LogBuffer putNumber( Number number, BufferNumberPutter putter ) throws IOException
    {
        if ( byteBuffer == null || 
            (BUFFER_SIZE - byteBuffer.position()) < putter.size() )
        {
            getNewDirectBuffer();
        }
        putter.put( byteBuffer, number );
        return this;
    }
    
    public LogBuffer put( byte b ) throws IOException
    {
        return putNumber( b, BufferNumberPutter.BYTE );
    }

    public LogBuffer putInt( int i ) throws IOException
    {
        return putNumber( i, BufferNumberPutter.INT );
    }

    public LogBuffer putLong( long l ) throws IOException
    {
        return putNumber( l, BufferNumberPutter.LONG );
    }

    public LogBuffer putFloat( float f ) throws IOException
    {
        return putNumber( f, BufferNumberPutter.FLOAT );
    }
    
    public LogBuffer putDouble( double d ) throws IOException
    {
        return putNumber( d, BufferNumberPutter.DOUBLE );
    }
    
    public LogBuffer put( byte[] bytes ) throws IOException
    {
        put( bytes, 0 );
        return this;
    }

    private void put( byte[] bytes, int offset ) throws IOException
    {
        int bytesToWrite = bytes.length - offset;
        if ( bytesToWrite > BUFFER_SIZE )
        {
            bytesToWrite = BUFFER_SIZE;
        }
        if ( byteBuffer == null || 
                (BUFFER_SIZE - byteBuffer.position()) < bytesToWrite )
        {
            getNewDirectBuffer();
        }
        byteBuffer.put( bytes, offset, bytesToWrite );
        offset += bytesToWrite;
        if ( offset < bytes.length )
        {
            put( bytes, offset );
        }
    }
    
    public LogBuffer put( char[] chars ) throws IOException
    {
        put( chars, 0 );
        return this;
    }

    private void put( char[] chars, int offset ) throws IOException
    {
        int charsToWrite = chars.length - offset;
        if ( charsToWrite * 2 > BUFFER_SIZE )
        {
            charsToWrite = BUFFER_SIZE / 2;
        }
        if ( byteBuffer == null || 
                (BUFFER_SIZE - byteBuffer.position()) < (charsToWrite * 2 ) )
        {
            getNewDirectBuffer();
        }
        int oldPos = byteBuffer.position();
        byteBuffer.asCharBuffer().put( chars, offset, charsToWrite );
        byteBuffer.position( oldPos + (charsToWrite * 2) );
        offset += charsToWrite;
        if ( offset < chars.length )
        {
            put( chars, offset );
        }
    }
    
    public void force() throws IOException
    {
        getNewDirectBuffer();
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