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

class DirectLogBuffer implements LogBuffer
{
    private final FileChannel fileChannel;
    private final ByteBuffer buffer;

    DirectLogBuffer( FileChannel fileChannel, ByteBuffer buffer )
    {
        if ( fileChannel == null || buffer == null )
        {
            throw new IllegalArgumentException( "Null argument" );
        }
        if ( buffer.capacity() < 8 )
        {
            throw new IllegalArgumentException( "Capacity less then 8" );
        }
        this.fileChannel = fileChannel;
        this.buffer = buffer;
    }

    public LogBuffer putNumber( Number number, BufferNumberPutter putter ) throws IOException
    {
        buffer.clear();
        putter.put( buffer, number );
        buffer.flip();
        fileChannel.write( buffer );
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
        fileChannel.write( ByteBuffer.wrap( bytes ) );
        return this;
    }

    public LogBuffer put( char[] chars ) throws IOException
    {
        int position = 0;
        do
        {
            buffer.clear();
            int leftToWrite = chars.length - position;
            if ( leftToWrite * 2 < buffer.capacity() )
            {
                buffer.asCharBuffer().put( chars, position, leftToWrite );
                buffer.limit( leftToWrite * 2);
                fileChannel.write( buffer );
                position += leftToWrite;
            }
            else
            {
                int length = buffer.capacity() / 2;
                buffer.asCharBuffer().put( chars, position, length );
                buffer.limit( length * 2 );
                fileChannel.write( buffer );
                position += length;
            }
        } while ( position < chars.length );
        return this;
    }

    public void force() throws IOException
    {
        fileChannel.force( false );
    }

    public long getFileChannelPosition() throws IOException
    {
        return fileChannel.position();
    }

    public FileChannel getFileChannel()
    {
        return fileChannel;
    }
}