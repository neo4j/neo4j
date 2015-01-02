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

public class DirectLogBuffer implements LogBuffer
{
    private final StoreChannel fileChannel;
    private final ByteBuffer buffer;

    public DirectLogBuffer( StoreChannel fileChannel, ByteBuffer buffer )
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

    public LogBuffer put( byte b ) throws IOException
    {
        buffer.clear();
        buffer.put( b );
        return flipAndWrite();
    }

    public LogBuffer putShort( short s ) throws IOException
    {
        buffer.clear();
        buffer.putShort( s );
        return flipAndWrite();
    }
    
    public LogBuffer putInt( int i ) throws IOException
    {
        buffer.clear();
        buffer.putInt( i );
        return flipAndWrite();
    }

    public LogBuffer putLong( long l ) throws IOException
    {
        buffer.clear();
        buffer.putLong( l );
        return flipAndWrite();
    }

    public LogBuffer putFloat( float f ) throws IOException
    {
        buffer.clear();
        buffer.putFloat( f );
        return flipAndWrite();
    }
    
    public LogBuffer putDouble( double d ) throws IOException
    {
        buffer.clear();
        buffer.putDouble( d );
        return flipAndWrite();
    }
    
    private LogBuffer flipAndWrite() throws IOException
    {
        buffer.flip();
        fileChannel.write( buffer );
        return this;
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
    
    @Override
    public void writeOut() throws IOException
    {
        // Nothing to do, since the data is always written in the put... methods.
    }

    public void force() throws IOException
    {
        fileChannel.force( false );
    }

    public long getFileChannelPosition() throws IOException
    {
        return fileChannel.position();
    }

    public StoreChannel getFileChannel()
    {
        return fileChannel;
    }
}
