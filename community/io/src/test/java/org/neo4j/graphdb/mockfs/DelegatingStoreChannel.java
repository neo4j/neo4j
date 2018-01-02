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
package org.neo4j.graphdb.mockfs;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileLock;

import org.neo4j.io.fs.StoreChannel;

public class DelegatingStoreChannel implements StoreChannel
{
    public final StoreChannel delegate;

    public DelegatingStoreChannel( StoreChannel delegate )
    {
        this.delegate = delegate;
    }

    public FileLock tryLock() throws IOException
    {
        return delegate.tryLock();
    }

    public int write( ByteBuffer src, long position ) throws IOException
    {
        return delegate.write( src, position );
    }

    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        return delegate.write( srcs, offset, length );
    }

    public void close() throws IOException
    {
        delegate.close();
    }

    public void writeAll( ByteBuffer src, long position ) throws IOException
    {
        delegate.writeAll( src, position );
    }

    public StoreChannel truncate( long size ) throws IOException
    {
        delegate.truncate( size );
        return this;
    }

    public void writeAll( ByteBuffer src ) throws IOException
    {
        delegate.writeAll( src );
    }

    public int write( ByteBuffer src ) throws IOException
    {
        return delegate.write( src );
    }

    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        return delegate.read( dsts, offset, length );
    }

    public long write( ByteBuffer[] srcs ) throws IOException
    {
        return delegate.write( srcs );
    }

    public boolean isOpen()
    {
        return delegate.isOpen();
    }

    public int read( ByteBuffer dst ) throws IOException
    {
        return delegate.read( dst );
    }

    public void force( boolean metaData ) throws IOException
    {
        delegate.force( metaData );
    }

    public long read( ByteBuffer[] dsts ) throws IOException
    {
        return delegate.read( dsts );
    }

    public int read( ByteBuffer dst, long position ) throws IOException
    {
        return delegate.read( dst, position );
    }

    public long position() throws IOException
    {
        return delegate.position();
    }

    public long size() throws IOException
    {
        return delegate.size();
    }

    public StoreChannel position( long newPosition ) throws IOException
    {
        delegate.position( newPosition );
        return this;
    }

    @Override
    public void flush() throws IOException
    {
        delegate.flush();
    }
}
