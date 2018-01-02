/*
 * Copyright (c) 2002-2018 "Neo Technology,"
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
package org.neo4j.kernel.impl.util;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.ReadableByteChannel;

import org.neo4j.io.fs.AbstractStoreChannel;
import org.neo4j.io.fs.StoreChannel;
import org.neo4j.kernel.monitoring.ByteCounterMonitor;

/**
 * Puts a buffer in front of a {@link ReadableByteChannel} so that even small reads,
 * byte/int/long will be fast.
 */
public class BufferedFileChannel extends AbstractStoreChannel
{
    private final StoreChannel source;
    private final ByteCounterMonitor monitor;
    private final byte[] intermediaryBuffer = new byte[1024*8];
    private int intermediaryBufferSize;
    private int intermediaryBufferPosition;

    public BufferedFileChannel( StoreChannel source, ByteCounterMonitor monitor ) throws IOException
    {
        this.source = source;
        this.monitor = monitor;
        fillUpIntermediaryBuffer();
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int read = 0;
        while ( read < dst.limit() )
        {
            read += readAsMuchAsPossibleFromIntermediaryBuffer( dst );
            if ( read < dst.limit() && fillUpIntermediaryBuffer() == -1 )
            {
                break;
            }
        }
        return read == 0 && dst.limit() > 0 ? -1 : read;
    }

    private int readAsMuchAsPossibleFromIntermediaryBuffer( ByteBuffer dst )
    {
        int howMuchToRead = Math.min( dst.remaining(), remainingInIntermediaryBuffer() );
        dst.put( intermediaryBuffer, intermediaryBufferPosition, howMuchToRead );
        intermediaryBufferPosition += howMuchToRead;
        return howMuchToRead;
    }
    
    private int remainingInIntermediaryBuffer()
    {
        return intermediaryBufferSize-intermediaryBufferPosition;
    }

    private int fillUpIntermediaryBuffer() throws IOException
    {
        int result = source.read( ByteBuffer.wrap( intermediaryBuffer ) );
        monitor.bytesRead( result );
        intermediaryBufferPosition = 0;
        intermediaryBufferSize = result == -1 ? 0 : result;
        return result;
    }

    @Override
    public long position() throws IOException
    {
        return source.position() - intermediaryBufferSize + intermediaryBufferPosition;
    }

    @Override
    public BufferedFileChannel position( long newPosition ) throws IOException
    {
        long bufferEndPosition = source.position();
        long bufferStartPosition = bufferEndPosition - intermediaryBufferSize;
        if ( newPosition >= bufferStartPosition && newPosition <= bufferEndPosition )
        {
            // Only an optimization
            long diff = newPosition-position();
            intermediaryBufferPosition += diff;
        }
        else
        {
            source.position( newPosition );
            fillUpIntermediaryBuffer();
        }
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return source.size();
    }

    @Override
    public BufferedFileChannel truncate( long size ) throws IOException
    {
        source.truncate( size );
        return this;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        source.force( metaData );
    }
    
    public StoreChannel getSource()
    {
        return source;
    }

    @Override
    public void close() throws IOException
    {
        source.close();
    }
}
