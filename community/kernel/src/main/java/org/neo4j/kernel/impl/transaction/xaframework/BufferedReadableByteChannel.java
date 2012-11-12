/**
 * Copyright (c) 2002-2012 "Neo Technology,"
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
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Combines an already open FileChannel and a ByteBuffer as a FileChannel. The
 * idea is that position reads past the end of the FileChannel will be satisfied
 * by the contents of the Buffer, having the Buffer in essence as an extension
 * of the channel. The position() of a new BufferedReadableByteChannel is the
 * position() of the provided FileChannel. The provided ByteBuffer is positioned
 * at 0. All reads after the position of the provided FileChannel at the moment
 * of creation are satisfied from the provided buffer - irrespective of any
 * changes in the size of the underlying file.
 *
 * This class is meant as a fix for the inability of Windows to memory map,
 * forcing reads of newly created files to be visible only through force()
 * operations. Therefore, the only supported operations are read(ByteBuffer),
 * read(ByteBuffer, long), position(), position(long) and size() - the rest
 * throw UnsupportedOperationException.
 *
 */
class BufferedReadableByteChannel extends FileChannel
{
    private final FileChannel fileChannel;
    private ByteBuffer byteBuffer;
    // The position after which reads are from the buffer and not the channel
    private final long bufferStartPosition;
    // The current position
    private long position;

    BufferedReadableByteChannel( FileChannel fileChannel, ByteBuffer buffer )
                                                                             throws IOException
    {
        this.fileChannel = fileChannel;
        bufferStartPosition = fileChannel.size();
        position = fileChannel.position();
        byteBuffer = buffer;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        if ( position == size() )
        {
            return -1;
        }
        int result = 0;
        if ( position < bufferStartPosition )
        {
            result += fileChannel.read( dst );
        }
        while ( dst.hasRemaining() && byteBuffer.hasRemaining() )
        {
            dst.put( byteBuffer.get() );
            result++;
        }
        position += result;
        return result;
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException
    {
        return position;
    }

    @Override
    public FileChannel position( long newPosition ) throws IOException
    {
        if ( newPosition < bufferStartPosition )
        {
            byteBuffer.position( 0 );
            fileChannel.position( newPosition );
        }
        else
        {
            fileChannel.position( fileChannel.size() );
            byteBuffer.position( (int) ( newPosition - bufferStartPosition ) );
        }
        position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return fileChannel.size() + byteBuffer.limit();
    }

    @Override
    public FileChannel truncate( long size ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferTo( long position, long count,
            WritableByteChannel target ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom( ReadableByteChannel src, long position, long count )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        long startingPosition = this.position;
        position( position );
        int result = read( dst );
        position( startingPosition );
        return result;
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map( MapMode mode, long position, long size )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock( long position, long size, boolean shared )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock( long position, long size, boolean shared )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() throws IOException
    {
        fileChannel.close();
    }
}
