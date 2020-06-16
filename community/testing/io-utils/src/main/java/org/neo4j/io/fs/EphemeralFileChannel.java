/*
 * Copyright (c) 2002-2020 "Neo4j,"
 * Neo4j Sweden AB [http://neo4j.com]
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
import java.nio.MappedByteBuffer;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.FileChannel;
import java.nio.channels.OverlappingFileLockException;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

import org.neo4j.io.ByteUnit;
import org.neo4j.io.memory.ByteBuffers;

import static java.lang.Math.min;
import static org.neo4j.memory.EmptyMemoryTracker.INSTANCE;

class EphemeralFileChannel extends FileChannel implements EphemeralPositionable
{
    final EphemeralFileStillOpenException openedAt;
    private final EphemeralFileData data;
    private long position;

    EphemeralFileChannel( EphemeralFileData data, EphemeralFileStillOpenException opened )
    {
        this.data = data;
        this.openedAt = opened;
        data.open( this );
    }

    @Override
    public String toString()
    {
        return getClass().getSimpleName() + "[" + openedAt.getFilename() + "]";
    }

    private void checkIfClosedOrInterrupted() throws IOException
    {
        if ( !isOpen() )
        {
            throw new ClosedChannelException();
        }
        if ( Thread.currentThread().isInterrupted() )
        {
            close();
            throw new ClosedByInterruptException();
        }
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        checkIfClosedOrInterrupted();
        return data.read( this, dst );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        checkIfClosedOrInterrupted();
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        checkIfClosedOrInterrupted();
        return data.write( this, src );
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        checkIfClosedOrInterrupted();
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException
    {
        checkIfClosedOrInterrupted();
        return position;
    }

    @Override
    public FileChannel position( long newPosition ) throws IOException
    {
        checkIfClosedOrInterrupted();
        this.position = newPosition;
        return this;
    }

    @Override
    public long size() throws IOException
    {
        checkIfClosedOrInterrupted();
        return data.size();
    }

    @Override
    public FileChannel truncate( long size ) throws IOException
    {
        checkIfClosedOrInterrupted();
        data.truncate( size );
        return this;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        checkIfClosedOrInterrupted();
        // Otherwise no forcing of an in-memory file
        data.force();
    }

    @Override
    public long transferTo( long position, long count, WritableByteChannel target )
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom( ReadableByteChannel src, long position, long count ) throws IOException
    {
        checkIfClosedOrInterrupted();
        long previousPos = position();
        position( position );
        try
        {
            long transferred = 0;
            ByteBuffer intermediary = ByteBuffers.allocate( 8, ByteUnit.MebiByte, INSTANCE );
            while ( transferred < count )
            {
                intermediary.clear();
                intermediary.limit( (int) min( intermediary.capacity(), count - transferred ) );
                int read = src.read( intermediary );
                if ( read == -1 )
                {
                    break;
                }
                transferred += read;
                intermediary.flip();
            }
            return transferred;
        }
        finally
        {
            position( previousPos );
        }
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        checkIfClosedOrInterrupted();
        return data.read( new EphemeralLocalPosition( position ), dst );
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        checkIfClosedOrInterrupted();
        return data.write( new EphemeralLocalPosition( position ), src );
    }

    @Override
    public MappedByteBuffer map( MapMode mode, long position, long size ) throws IOException
    {
        checkIfClosedOrInterrupted();
        throw new IOException( "Not supported" );
    }

    @Override
    public java.nio.channels.FileLock lock( long position, long size, boolean shared ) throws IOException
    {
        checkIfClosedOrInterrupted();
        if ( data.takeLock() )
        {
            return new EphemeralFileLock( this, data );
        }
        return null;
    }

    @Override
    public java.nio.channels.FileLock tryLock( long position, long size, boolean shared )
    {
        if ( data.takeLock() )
        {
            return new EphemeralFileLock( this, data );
        }
        throw new OverlappingFileLockException();
    }

    @Override
    protected void implCloseChannel()
    {
        data.close( this );
    }

    @Override
    public long pos()
    {
        return position;
    }

    @Override
    public void pos( long position )
    {
        this.position = position;
    }
}
