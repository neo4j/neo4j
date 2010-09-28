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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;

import javax.transaction.xa.Xid;

import org.neo4j.kernel.impl.nioneo.store.UnderlyingStorageException;
import org.neo4j.kernel.impl.util.BufferNumberPutter;

class MemoryMappedLogBuffer implements LogBuffer
{
    private static final int MAPPED_SIZE = 1024 * 1024 * 2;

    private final FileChannel fileChannel;

    private MappedByteBuffer mappedBuffer = null;
    private long mappedStartPosition;
    private final ByteBuffer fallbackBuffer;

    MemoryMappedLogBuffer( FileChannel fileChannel ) throws IOException
    {
        this.fileChannel = fileChannel;
        mappedStartPosition = fileChannel.position();
        getNewMappedBuffer();
        fallbackBuffer = ByteBuffer.allocateDirect( 9 + Xid.MAXGTRIDSIZE
            + Xid.MAXBQUALSIZE * 10 );
    }

    MappedByteBuffer getMappedBuffer()
    {
        return mappedBuffer;
    }

    private int mapFail = -1;

    private void getNewMappedBuffer()
    {
        try
        {
            if ( mappedBuffer != null )
            {
                mappedStartPosition += mappedBuffer.position();
                mappedBuffer.force();
                mappedBuffer = null;
            }
            if ( mapFail > 1000 )
            {
                mapFail = -1;
            }
            if ( mapFail > 0 )
            {
                mapFail++;
                return;
            }
            mappedBuffer = fileChannel.map( MapMode.READ_WRITE,
                mappedStartPosition, MAPPED_SIZE );
        }
        catch ( Throwable t )
        {
            mapFail = 1;
            t.printStackTrace();
        }
    }

    private LogBuffer putNumber( Number value, BufferNumberPutter putter ) throws IOException
    {
        if ( mappedBuffer == null || 
                (MAPPED_SIZE - mappedBuffer.position()) < putter.size() )
            {
                getNewMappedBuffer();
                if ( mappedBuffer == null )
                {
                    fallbackBuffer.clear();
                    putter.put( fallbackBuffer, value );
                    fallbackBuffer.flip();
                    fileChannel.write( fallbackBuffer, mappedStartPosition );
                    mappedStartPosition += putter.size();
                    return this;
                }
            }
            putter.put( mappedBuffer, value );
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
        if ( bytesToWrite > MAPPED_SIZE )
        {
            bytesToWrite = MAPPED_SIZE;
        }
        if ( mappedBuffer == null || 
                (MAPPED_SIZE - mappedBuffer.position()) < bytesToWrite )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                bytesToWrite = bytes.length - offset; // reset
                ByteBuffer buf = ByteBuffer.wrap( bytes );
                buf.position( offset );
                int count = fileChannel.write( buf, mappedStartPosition );
                if ( count != bytesToWrite )
                {
                    throw new UnderlyingStorageException( "Failed to write from " + 
                        offset + " expected " + bytesToWrite + " but wrote " + 
                        count );
                }
                mappedStartPosition += bytesToWrite;
                return;
            }
        }
        mappedBuffer.put( bytes, offset, bytesToWrite );
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
        if ( charsToWrite * 2 > MAPPED_SIZE )
        {
            charsToWrite = MAPPED_SIZE / 2;
        }
        if ( mappedBuffer == null || 
                (MAPPED_SIZE - mappedBuffer.position()) < (charsToWrite * 2 ) )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                int bytesToWrite = ( chars.length - offset ) * 2;
                ByteBuffer buf = ByteBuffer.allocate( bytesToWrite );
                buf.asCharBuffer().put( chars, offset, chars.length - offset );
                buf.limit( chars.length * 2 );
                int count = fileChannel.write( buf, mappedStartPosition );
                if ( count != bytesToWrite )
                {
                    throw new UnderlyingStorageException( "Failed to write from " + 
                        offset + " expected " + bytesToWrite + " but wrote " + 
                        count );
                }
                mappedStartPosition += bytesToWrite;
                return;
            }
        }
        int oldPos = mappedBuffer.position();
        mappedBuffer.asCharBuffer().put( chars, offset, charsToWrite );
        mappedBuffer.position( oldPos + (charsToWrite * 2) );
        offset += charsToWrite;
        if ( offset < chars.length )
        {
            put( chars, offset );
        }
    }
    
    void releaseMemoryMapped()
    {
        if ( mappedBuffer != null )
        {
            mappedBuffer.force();
            mappedBuffer = null;
        }
    }

    public void force() throws IOException
    {
        if ( mappedBuffer != null )
        {
            mappedBuffer.force();
        }
        fileChannel.force( false );
    }

    public long getFileChannelPosition()
    {
        if ( mappedBuffer != null )
        {
            return mappedStartPosition + mappedBuffer.position();
        }
        return mappedStartPosition;
    }

    public FileChannel getFileChannel()
    {
        return fileChannel;
    }
}