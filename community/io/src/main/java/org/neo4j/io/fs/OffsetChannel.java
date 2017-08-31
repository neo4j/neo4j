/*
 * Copyright (c) 2002-2017 "Neo Technology,"
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
import java.nio.channels.FileLock;

/**
 * A wrapper for a channel with an offset. Users must make
 * sure that the channel is within bounds or exceptions
 * will occur.
 */
public class OffsetChannel implements StoreChannel
{
    private final StoreChannel delegate;
    private final long offset;

    public OffsetChannel( StoreChannel delegate, long offset )
    {
        this.delegate = delegate;
        this.offset = offset;
    }

    private long offset( long position )
    {
        if ( position < 0 )
        {
            throw new IllegalArgumentException( "Position must be >= 0." );
        }
        return Math.addExact( position, offset );
    }

    @Override
    public FileLock tryLock() throws IOException
    {
        return delegate.tryLock();
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        return delegate.write( src, offset( position ) );
    }

    @Override
    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        delegate.writeAll( src, offset( position ) );
    }

    @Override
    public void writeAll( ByteBuffer src ) throws IOException
    {
        delegate.writeAll( src );
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return delegate.read( dst, offset( position ) );
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        delegate.force( metaData );
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        return delegate.read( dst );
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        return delegate.write( src );
    }

    @Override
    public long position() throws IOException
    {
        return delegate.position() - offset;
    }

    @Override
    public StoreChannel position( long newPosition ) throws IOException
    {
        return delegate.position( offset( newPosition ) );
    }

    @Override
    public long size() throws IOException
    {
        return delegate.size() - offset;
    }

    @Override
    public StoreChannel truncate( long size ) throws IOException
    {
        return delegate.truncate( offset( size ) );
    }

    @Override
    public void flush() throws IOException
    {
        delegate.flush();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return delegate.write( srcs, offset, length );
    }

    @Override
    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return delegate.write( srcs );
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return delegate.read( dsts, offset, length );
    }

    @Override
    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return delegate.read( dsts );
    }

    @Override
    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    @Override
    public void close() throws IOException
    {
        delegate.close();
    }
}
