/**
 * Copyright (c) 2002-2015 "Neo Technology,"
 * Network Engine for Objects in Lund AB [http://neotechnology.com]
 *
 * This file is part of Neo4j.
 *
 * Neo4j is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 */
package org.neo4j.kernel.impl.transaction.xaframework;

import java.io.IOException;
import java.nio.ByteBuffer;

import org.neo4j.kernel.impl.nioneo.store.StoreChannel;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * Implementation of a LogBuffer that buffers content in a direct byte buffer
 * and flushes in a file channel. Flushing is based on size cap and force()
 * operations.
 * <p>
 * Currently this is the default LogBuffer implementation.
 */
public class DirectMappedLogBuffer implements LogBuffer
{
    // 500k
    static final int BUFFER_SIZE = 1024 * 512;

    private final StoreChannel fileChannel;

    private final ByteBuffer byteBuffer;
    private long bufferStartPosition;

    private final ByteCounterMonitor monitor;

    public DirectMappedLogBuffer( StoreChannel fileChannel, ByteCounterMonitor monitor ) throws IOException
    {
        this.fileChannel = fileChannel;
        this.monitor = monitor;
        bufferStartPosition = fileChannel.position();
        byteBuffer = ByteBuffer.allocateDirect( BUFFER_SIZE );
    }

    private void ensureCapacity( int plusSize ) throws IOException
    {
        if (( BUFFER_SIZE - byteBuffer.position() ) < plusSize )
        {
            writeOut();
        }
        assert BUFFER_SIZE - byteBuffer.position() >= plusSize : "after writing out buffer, position is "
                                                                 + byteBuffer.position()
                                                                 + " and requested size is "
                                                                 + plusSize;
    }

    public LogBuffer put( byte b ) throws IOException
    {
        ensureCapacity( 1 );
        byteBuffer.put( b );
        return this;
    }

    public LogBuffer putShort( short s ) throws IOException
    {
        ensureCapacity( 2 );
        byteBuffer.putShort( s );
        return this;
    }

    public LogBuffer putInt( int i ) throws IOException
    {
        ensureCapacity( 4 );
        byteBuffer.putInt( i );
        return this;
    }

    public LogBuffer putLong( long l ) throws IOException
    {
        ensureCapacity( 8 );
        byteBuffer.putLong( l );
        return this;
    }

    public LogBuffer putFloat( float f ) throws IOException
    {
        ensureCapacity( 4 );
        byteBuffer.putFloat( f );
        return this;
    }

    public LogBuffer putDouble( double d ) throws IOException
    {
        ensureCapacity( 8 );
        byteBuffer.putDouble( d );
        return this;
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
        ensureCapacity( bytesToWrite );
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
        ensureCapacity( charsToWrite * 2 );
        int oldPos = byteBuffer.position();
        byteBuffer.asCharBuffer().put( chars, offset, charsToWrite );
        byteBuffer.position( oldPos + ( charsToWrite * 2 ) );
        offset += charsToWrite;
        if ( offset < chars.length )
        {
            put( chars, offset );
        }
    }

    @Override
    public void writeOut() throws IOException
    {
        byteBuffer.flip();

        // We use .clear() to reset this buffer, so position will always be 0
        long expectedEndPosition = bufferStartPosition + byteBuffer.limit();
        long bytesWritten;

        while((bufferStartPosition += (bytesWritten = fileChannel.write( byteBuffer, bufferStartPosition ))) < expectedEndPosition)
        {
            if( bytesWritten <= 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }

        monitor.bytesWritten( bytesWritten );
        byteBuffer.clear();
    }

    public void force() throws IOException
    {
        writeOut();
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

    public StoreChannel getFileChannel()
    {
        return fileChannel;
    }
}
