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
package org.neo4j.io.fs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;

public class StoreFileChannel implements StoreChannel
{
    private final FileChannel channel;

    public StoreFileChannel( FileChannel channel )
    {
        this.channel = channel;
    }

    public StoreFileChannel( StoreFileChannel channel )
    {
        this.channel = channel.channel;
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return channel.write( srcs );
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        return channel.write( src, position );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return channel.write( srcs, offset, length );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        long filePosition = position;
        long expectedEndPosition = filePosition + src.limit() - src.position();
        int bytesWritten;
        while((filePosition += (bytesWritten = write( src, filePosition ))) < expectedEndPosition)
        {
            if( bytesWritten < 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        long bytesToWrite = src.limit() - src.position();
        int bytesWritten;
        while((bytesToWrite -= (bytesWritten = write( src ))) > 0)
        {
            if( bytesWritten < 0 )
            {
                throw new IOException( "Unable to write to disk, reported bytes written was " + bytesWritten );
            }
        }
    }

    @Override
    public StoreFileChannel truncate( long size ) throws IOException
    {
        channel.truncate( size );
        return this;
    }

    @Override
    public StoreFileChannel position( long newPosition ) throws IOException
    {
        channel.position( newPosition );
        return this;
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return channel.read( dst, position );
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        channel.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return channel.read( dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return channel.read( dsts, offset, length );
    }

    @Override
    public long position() throws IOException
    {
        return channel.position();
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return channel.tryLock();
    }

    @Override
    public boolean isOpen()
    {
        return channel.isOpen();
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return channel.read( dsts );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return channel.write( src );
    }

    @Override
    public void close() throws IOException
    {
        channel.close();
    }

    @Override
    public long size() throws IOException
    {
        return channel.size();
    }

    @Override
    public void flush() throws IOException
    {
        force( false );
    }

    static FileChannel unwrap( StoreChannel channel )
    {
        StoreFileChannel sfc = (StoreFileChannel) channel;
        return sfc.channel;
    }
}
