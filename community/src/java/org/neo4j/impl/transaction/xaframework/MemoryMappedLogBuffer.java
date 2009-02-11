/*
 * Copyright (c) 2002-2008 "Neo Technology,"
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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileChannel.MapMode;
import javax.transaction.xa.Xid;

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

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#put(byte)
     */
    public LogBuffer put( byte b ) throws IOException
    {
        if ( mappedBuffer == null || 
            (MAPPED_SIZE - mappedBuffer.position()) < 1 )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                fallbackBuffer.clear();
                fallbackBuffer.put( b );
                fallbackBuffer.flip();
                fileChannel.write( fallbackBuffer, mappedStartPosition );
                mappedStartPosition += 1;
                return this;
            }
        }
        mappedBuffer.put( b );
        return this;
    }

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#putInt(int)
     */
    public LogBuffer putInt( int i ) throws IOException
    {
        if ( mappedBuffer == null || 
            (MAPPED_SIZE - mappedBuffer.position()) < 4 )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                fallbackBuffer.clear();
                fallbackBuffer.putInt( i );
                fallbackBuffer.flip();
                fileChannel.write( fallbackBuffer, mappedStartPosition );
                mappedStartPosition += 4;
                return this;
            }
        }
        mappedBuffer.putInt( i );
        return this;
    }

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#putLong(long)
     */
    public LogBuffer putLong( long l ) throws IOException
    {
        if ( mappedBuffer == null || 
            (MAPPED_SIZE - mappedBuffer.position()) < 8 )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                fallbackBuffer.clear();
                fallbackBuffer.putLong( l );
                fallbackBuffer.flip();
                fileChannel.write( fallbackBuffer, mappedStartPosition );
                mappedStartPosition += 8;
                return this;
            }
        }
        mappedBuffer.putLong( l );
        return this;
    }

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#put(byte[])
     */
    public LogBuffer put( byte[] bytes ) throws IOException
    {
        if ( mappedBuffer == null || 
            (MAPPED_SIZE - mappedBuffer.position()) < bytes.length )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                fallbackBuffer.clear();
                fallbackBuffer.put( bytes );
                fallbackBuffer.flip();
                fileChannel.write( fallbackBuffer, mappedStartPosition );
                mappedStartPosition += bytes.length;
                return this;
            }
        }
        mappedBuffer.put( bytes );
        return this;
    }

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#put(char[])
     */
    public LogBuffer put( char[] chars ) throws IOException
    {
        if ( mappedBuffer == null || 
            (MAPPED_SIZE - mappedBuffer.position()) < (chars.length * 2) )
        {
            getNewMappedBuffer();
            if ( mappedBuffer == null )
            {
                fallbackBuffer.clear();
                fallbackBuffer.asCharBuffer().put( chars );
                fallbackBuffer.limit( chars.length * 2 );
                fileChannel.write( fallbackBuffer, mappedStartPosition );
                mappedStartPosition += (chars.length * 2);
                return this;
            }
        }
        int oldPos = mappedBuffer.position();
        mappedBuffer.asCharBuffer().put( chars );
        mappedBuffer.position( oldPos + chars.length * 2 );
        return this;
    }

    void releaseMemoryMapped()
    {
        if ( mappedBuffer != null )
        {
            mappedBuffer.force();
            mappedBuffer = null;
        }
    }

    /* (non-Javadoc)
     * @see org.neo4j.impl.transaction.xaframework.ILogBuffer#force()
     */
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